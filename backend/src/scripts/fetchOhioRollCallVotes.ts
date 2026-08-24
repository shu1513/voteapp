import { mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import {
  upsertLegislativeVoteSource,
  type LegislativeVoteUpsertOutcome,
} from "../pipeline/rollcall/legislativeVoteStore.js";
import type { LegislativeVoteChamber } from "../pipeline/rollcall/legislativeVotes.js";
import {
  classifyOhioVoteAction,
  ohioActionChamber,
  ohioActionHasVotes,
  ohioActionMemberVotes,
  ohioActionSha256,
  ohioActionVoteDate,
  ohioActionsUrl,
  ohioDisplayUrl,
  ohioEvidenceFileName,
  ohioKeptFloorDayCollisions,
  ohioLegislationListUrl,
  ohioMeasureId,
  ohioRollNumber,
  parseOhioBillNumber,
  OHIO_JURISDICTION,
  OHIO_KEPT_MEASURE_TYPES,
  type OhioAction,
  type OhioQuestionClass,
  type OhioVoteEvidence,
} from "../pipeline/rollcall/ohioRollCall.js";
import { readPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";

// Ohio fetcher for the roll-call import's state pilot
// (docs/plans/roll-call-vote-import.md §5 phase 3): pulls each bill's
// actions feed from the official Ohio LIS API, keeps every vote-bearing
// floor action as evidence, classifies it, and stores it on
// legislative_votes as a pending review-queue row. Committee votes
// (crpt_*/refer_* codes) are rejected before the queue — counted in the
// report, never stored. Read-only on the web, no candidate writes, no AI.
//
//   npm run rollcall:oh:fetch -- --ga 136 --bills hb96,sb1
//   npm run rollcall:oh:fetch -- --ga 136 --all-kept --evidence-dir evidence/rollcall/ohio-136/survey
//   npm run rollcall:oh:fetch -- --ga 136 --bills hb96 --dry-run

export const OHIO_ROLLCALL_FETCH_IMPORTER_VERSION = "rollcall-oh-fetch-v1";
const DEFAULT_DELAY_MS = 300;
// 991 hb + 467 sb + 9 hjr + 10 sjr in GA 136 when probed; the cap only
// stops a runaway list from expanding unnoticed.
const MAX_BILLS_PER_RUN = 4000;

export type OhioRollCallFetchReportRow = {
  bill: string;
  chamber: LegislativeVoteChamber | null;
  roll: number | null;
  actionCode: string | null;
  action: string | null;
  outcome: LegislativeVoteUpsertOutcome | "dry_run" | "committee" | "fetch_error" | "parse_error" | "collision";
  evidenceFile: string | null;
  voteDate: string | null;
  measureId: string | null;
  result: string | null;
  yeas: number | null;
  nays: number | null;
  isFloorVote: boolean | null;
  questionClass: OhioQuestionClass | null;
  classificationReason: string | null;
  judgmentCleared: boolean;
  error: string | null;
};

const FAILURE_OUTCOMES: ReadonlySet<OhioRollCallFetchReportRow["outcome"]> = new Set([
  "fetch_error",
  "parse_error",
  "collision",
]);

export function parseBillList(raw: string): string[] {
  const bills: string[] = [];
  const seen = new Set<string>();
  for (const part of raw.split(",")) {
    const token = part.trim().toLowerCase();
    if (token.length === 0) {
      continue;
    }
    if (!parseOhioBillNumber(token)) {
      throw new Error(`--bills entry is not an Ohio bill number: ${token}`);
    }
    if (!seen.has(token)) {
      seen.add(token);
      bills.push(token);
    }
  }
  if (bills.length === 0) {
    throw new Error("--bills names no bills");
  }
  return bills;
}

function readValueFlag(argv: readonly string[], flagName: string): string | null {
  const index = argv.indexOf(flagName);
  if (index >= 0) {
    const value = argv[index + 1];
    if (value === undefined || value.startsWith("--")) {
      throw new Error(`${flagName} requires a value`);
    }
    return value;
  }
  const inline = argv.find((token) => token.startsWith(`${flagName}=`));
  return inline ? inline.slice(flagName.length + 1) : null;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolveSleep) => setTimeout(resolveSleep, ms));
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

// Same abort window the federal fetcher uses: one stalled request must not
// hang a 1,477-bill --all-kept run.
const FETCH_TIMEOUT_MS = 30_000;

async function fetchOhioJson(url: string): Promise<unknown> {
  const response = await fetch(url, {
    headers: { accept: "application/json" },
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
  });
  if (!response.ok) {
    throw new Error(`${url} answered ${response.status}`);
  }
  return (await response.json()) as unknown;
}

async function fetchOhioJsonWithRetry(url: string, delayMs: number): Promise<unknown> {
  try {
    return await fetchOhioJson(url);
  } catch (error) {
    console.error(`retrying ${url}: ${errorMessage(error)}`);
    await sleep(delayMs);
    return fetchOhioJson(url);
  }
}

/** Every hb/sb/hjr/sjr bill number in the General Assembly's legislation list. */
export function keptBillNumbers(legislationList: unknown): string[] {
  if (typeof legislationList !== "object" || legislationList === null) {
    throw new Error("legislation list is not an object or array");
  }
  const bills: string[] = [];
  const seen = new Set<string>();
  for (const row of Object.values(legislationList)) {
    const number = (row as { number?: unknown })?.number;
    if (typeof number !== "string") {
      throw new Error(`legislation list row has no number: ${JSON.stringify(row).slice(0, 120)}`);
    }
    const measure = parseOhioBillNumber(number);
    if (!measure || !(OHIO_KEPT_MEASURE_TYPES as readonly string[]).includes(measure.type)) {
      continue;
    }
    const bill = number.trim().toLowerCase();
    if (!seen.has(bill)) {
      seen.add(bill);
      bills.push(bill);
    }
  }
  return bills;
}

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  assertKnownCliFlags("rollcall:oh:fetch", argv, [
    { name: "--ga", value: "both" },
    { name: "--bills", value: "both" },
    { name: "--all-kept", value: "none" },
    { name: "--delay-ms", value: "both" },
    { name: "--evidence-dir", value: "both" },
    { name: "--dry-run", value: "none" },
  ]);

  const generalAssembly = readPositiveIntegerFlag(argv, "--ga", 0);
  if (generalAssembly === 0) {
    throw new Error("--ga is required (e.g. --ga 136)");
  }
  const billsFlag = readValueFlag(argv, "--bills");
  const allKept = argv.includes("--all-kept");
  if ((billsFlag === null) === !allKept) {
    throw new Error("pass exactly one of --bills or --all-kept");
  }
  const delayMs = readPositiveIntegerFlag(argv, "--delay-ms", DEFAULT_DELAY_MS);
  const dryRun = argv.includes("--dry-run");

  const startedAt = new Date();
  const runId = `rollcall-oh-fetch-${generalAssembly}-${startedAt
    .toISOString()
    .replace(/[-:]/g, "")
    .replace(/\.\d{3}Z$/, "Z")}`;
  const evidenceDir = resolve(readValueFlag(argv, "--evidence-dir") ?? resolve(process.cwd(), "evidence", "rollcall", runId));
  mkdirSync(evidenceDir, { recursive: true });

  loadProjectEnv();
  let pool: Pool | null = null;
  if (!dryRun) {
    const databaseUrl = process.env.DATABASE_URL?.trim();
    if (!databaseUrl) {
      throw new Error("DATABASE_URL is required to store roll calls (or pass --dry-run)");
    }
    pool = new Pool({ connectionString: databaseUrl });
  }

  const bills = billsFlag !== null ? parseBillList(billsFlag) : keptBillNumbers(await fetchOhioJsonWithRetry(ohioLegislationListUrl(generalAssembly), delayMs));
  if (bills.length > MAX_BILLS_PER_RUN) {
    throw new Error(`${bills.length} bills is more than ${MAX_BILLS_PER_RUN}; split the run`);
  }

  const session = String(generalAssembly);
  const rows: OhioRollCallFetchReportRow[] = [];
  let billsFetched = 0;
  let billsWithoutVotes = 0;
  // Ohio has no roll numbers; the surrogate is occurred-epoch-seconds, so a
  // second bill landing on an already-stored (chamber, roll) key would
  // silently overwrite a different vote. Guard within the run and against
  // the stored measure before every upsert.
  const rollOwner = new Map<string, string>();
  try {
    for (const [index, bill] of bills.entries()) {
      if (index > 0) {
        await sleep(delayMs);
      }
      const machineUrl = ohioActionsUrl(generalAssembly, bill);
      let actions: OhioAction[];
      try {
        const feed = await fetchOhioJsonWithRetry(machineUrl, delayMs);
        if (typeof feed !== "object" || feed === null) {
          throw new Error("actions feed is not an object or array");
        }
        actions = Object.values(feed) as OhioAction[];
        billsFetched += 1;
      } catch (error) {
        rows.push({
          bill,
          chamber: null,
          roll: null,
          actionCode: null,
          action: null,
          outcome: "fetch_error",
          evidenceFile: null,
          voteDate: null,
          measureId: null,
          result: null,
          yeas: null,
          nays: null,
          isFloorVote: null,
          questionClass: null,
          classificationReason: null,
          judgmentCleared: false,
          error: errorMessage(error),
        });
        continue;
      }

      const measure = parseOhioBillNumber(bill)!;
      const measureId = ohioMeasureId(measure);
      const voteActions = actions.filter((action) => ohioActionHasVotes(action));
      if (voteActions.length === 0) {
        billsWithoutVotes += 1;
        continue;
      }
      // One kept floor vote per (chamber, day) per bill: the per-bill
      // source URL cannot name a single vote, so the dedupe scan leans on
      // this invariant (rollCallRecordUrls.ts). Preflighted over the whole
      // bill BEFORE anything is stored, so a colliding pair rejects BOTH
      // votes — rejecting only the second would leave the first, equally
      // indistinguishable, in the queue.
      const collidingDays = ohioKeptFloorDayCollisions(voteActions, measure);

      for (const action of voteActions) {
        const actionCode = typeof action.action_code === "string" ? action.action_code : "";
        const actionText = typeof action.action === "string" ? action.action : null;
        const row: OhioRollCallFetchReportRow = {
          bill,
          chamber: null,
          roll: null,
          actionCode: actionCode || null,
          action: actionText,
          outcome: "fetch_error",
          evidenceFile: null,
          voteDate: null,
          measureId,
          result: typeof action.result === "string" ? action.result : null,
          yeas: null,
          nays: null,
          isFloorVote: null,
          questionClass: null,
          classificationReason: null,
          judgmentCleared: false,
          error: null,
        };
        rows.push(row);
        try {
          const classification = classifyOhioVoteAction({ actionCode, measure });
          row.isFloorVote = classification.isFloorVote;
          row.questionClass = classification.questionClass;
          row.classificationReason = classification.reason;
          if (classification.reason.startsWith("committee:")) {
            row.outcome = "committee";
            continue;
          }
          const chamber = ohioActionChamber(action);
          const voteDate = ohioActionVoteDate(action);
          const roll = ohioRollNumber(action);
          const { yeas, nays } = ohioActionMemberVotes(action);
          row.chamber = chamber;
          row.voteDate = voteDate;
          row.roll = roll;
          row.yeas = yeas.length;
          row.nays = nays.length;
          if (typeof action.result !== "string" || action.result.trim().length === 0) {
            throw new Error("vote action has no result");
          }

          if (classification.isFloorVote === true && collidingDays.has(`${chamber}:${voteDate}`)) {
            row.outcome = "collision";
            row.error = `${bill} has two kept floor votes in the ${chamber} on ${voteDate}; the per-bill source URL cannot tell them apart`;
            continue;
          }
          const rollKey = `${chamber}:${roll}`;
          const owner = rollOwner.get(rollKey);
          if (owner !== undefined && owner !== bill) {
            row.outcome = "collision";
            row.error = `roll ${roll} (${chamber}) already belongs to ${owner} in this run`;
            continue;
          }
          rollOwner.set(rollKey, bill);
          if (pool) {
            const stored = await pool.query<{ measure_id: string | null }>(
              `SELECT measure_id FROM legislative_votes
                WHERE jurisdiction = $1 AND chamber = $2 AND session = $3 AND roll_number = $4`,
              [OHIO_JURISDICTION, chamber, session, roll]
            );
            if (stored.rows[0] !== undefined && stored.rows[0].measure_id !== measureId) {
              row.outcome = "collision";
              row.error = `roll ${roll} (${chamber}) is stored as ${stored.rows[0].measure_id ?? "no measure"}, not ${measureId}`;
              continue;
            }
          }

          const sourceSha256 = ohioActionSha256(action);
          const evidenceFile = ohioEvidenceFileName(chamber, generalAssembly, roll);
          const evidence: OhioVoteEvidence = {
            jurisdiction: OHIO_JURISDICTION,
            generalAssembly,
            chamber,
            rollNumber: roll,
            bill,
            measureId,
            machineUrl,
            fetchedAt: new Date().toISOString(),
            action,
          };
          writeFileSync(resolve(evidenceDir, evidenceFile), `${JSON.stringify(evidence, null, 2)}\n`);
          row.evidenceFile = evidenceFile;

          if (!pool) {
            row.outcome = "dry_run";
            continue;
          }
          const result = await upsertLegislativeVoteSource(pool, {
            jurisdiction: OHIO_JURISDICTION,
            chamber,
            session,
            rollNumber: roll,
            voteDate,
            measureId,
            exactQuestion: actionText ?? actionCode,
            isFloorVote: classification.isFloorVote,
            result: action.result.trim(),
            yeas: yeas.length,
            nays: nays.length,
            displayUrl: ohioDisplayUrl(generalAssembly, bill),
            machineUrl,
            billUrl: ohioDisplayUrl(generalAssembly, bill),
            sourceSha256,
            fetchedAt: new Date(),
            importerVersion: OHIO_ROLLCALL_FETCH_IMPORTER_VERSION,
          });
          row.outcome = result.outcome;
          row.judgmentCleared = result.judgmentCleared;
        } catch (error) {
          row.outcome = "parse_error";
          row.error = errorMessage(error);
        }
      }
    }
  } finally {
    await pool?.end();
  }

  const counts: Record<string, number> = {};
  for (const row of rows) {
    counts[row.outcome] = (counts[row.outcome] ?? 0) + 1;
  }
  const floorVotes = rows.filter((row) => row.isFloorVote === true).length;
  const unknownActions = rows.filter((row) => row.classificationReason?.startsWith("unknown_action:")).length;
  const report = {
    runId,
    importerVersion: OHIO_ROLLCALL_FETCH_IMPORTER_VERSION,
    jurisdiction: OHIO_JURISDICTION,
    generalAssembly,
    dryRun,
    startedAt: startedAt.toISOString(),
    finishedAt: new Date().toISOString(),
    evidenceDir,
    requestedBills: bills.length,
    billsFetched,
    billsWithoutVotes,
    floorVotes,
    unknownActions,
    counts,
    rows,
  };
  writeFileSync(resolve(evidenceDir, `${runId}-report.json`), `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify({ ...report, rows: undefined }, null, 2));
  if (rows.some((row) => FAILURE_OUTCOMES.has(row.outcome))) {
    process.exitCode = 1;
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("rollcall:oh:fetch failed:", errorMessage(error));
    process.exitCode = 1;
  });
}
