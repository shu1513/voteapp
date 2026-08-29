import { createHash } from "node:crypto";
import { mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import {
  upsertLegislativeVoteSource,
  type LegislativeVoteUpsertOutcome,
} from "../pipeline/rollcall/legislativeVoteStore.js";
import type { LegislativeVoteChamber } from "../pipeline/rollcall/legislativeVotes.js";
import {
  classifyLegiscanDatasetFile,
  classifyLegiscanRollCall,
  isLegiscanCommitteeChamberRollCall,
  legiscanEvidenceFileName,
  legiscanRollCallPageUrl,
  legiscanRollCallSha256,
  parseLegiscanBill,
  parseLegiscanRollCall,
  type LegiscanBillSummary,
  type LegiscanRollCall,
  type LegiscanVoteEvidence,
} from "../pipeline/rollcall/legiscanRollCall.js";
import { getLegiscanStateConfig } from "../pipeline/rollcall/legiscanStateConfigs.js";
import type { LegiscanQuestionClass } from "../pipeline/rollcall/legiscanStateConfigs.js";
import { rollCallUrlKey } from "../pipeline/rollcall/rollCallRecordUrls.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";

// LegiScan fetcher for the roll-call import's phase-4 state rollout
// (docs/plans/roll-call-vote-import.md §5 phase 4): reads an EXTRACTED
// LegiScan session dataset directory (the operator downloads the weekly
// bulk ZIP — legiscan.com/datasets, or getDatasetRaw with a key — and
// unzips it; no live-API code here), classifies every roll call of every
// kept-type bill with the state's configured desc vocabulary, keeps each
// storable vote as evidence, and stores it on legislative_votes as a
// pending review-queue row. Committee-sized votes and votes on excluded
// instrument types are rejected before the queue — counted in the report,
// never stored. Local files + DB only, no network, no AI.
//
// A state without a config entry starts with a SURVEY, which needs no
// config and no DB: it writes the state's desc histogram so a human can
// read the vocabulary before encoding it (legiscanStateConfigs.ts).
//
//   npm run rollcall:legiscan:fetch -- --state TX --dataset-dir scratch/legiscan/TX --survey
//   npm run rollcall:legiscan:fetch -- --state TX --dataset-dir scratch/legiscan/TX \
//     --evidence-dir evidence/rollcall/legiscan-tx-2172/survey
//   npm run rollcall:legiscan:fetch -- --state TX --dataset-dir scratch/legiscan/TX --bills hb1,sb2 --dry-run

export const LEGISCAN_ROLLCALL_FETCH_IMPORTER_VERSION = "rollcall-legiscan-fetch-v1";
// A whole-session dataset holds a few thousand roll calls; the cap only
// stops a mis-pointed directory (a multi-state dump) from running away.
const MAX_VOTES_PER_RUN = 40_000;

export type LegiscanRollCallFetchReportRow = {
  bill: string;
  chamber: LegislativeVoteChamber | null;
  roll: number | null;
  desc: string | null;
  outcome: LegislativeVoteUpsertOutcome | "dry_run" | "parse_error" | "collision";
  evidenceFile: string | null;
  voteDate: string | null;
  measureId: string | null;
  result: string | null;
  yeas: number | null;
  nays: number | null;
  isFloorVote: boolean | null;
  questionClass: LegiscanQuestionClass | null;
  classificationReason: string | null;
  judgmentCleared: boolean;
  error: string | null;
};

const FAILURE_OUTCOMES: ReadonlySet<LegiscanRollCallFetchReportRow["outcome"]> = new Set(["parse_error", "collision"]);

// One dataset directory, read once. Elements stay verbatim; every
// derivation happens (and is re-checked) where it is used.
export type LegiscanDataset = {
  billsById: Map<number, LegiscanBillSummary>;
  votes: { file: string; rollCall: Record<string, unknown> }[];
  people: Record<string, unknown>[];
  // Files that failed JSON.parse or bill parsing; surfaced, run fails.
  fileErrors: { file: string; error: string }[];
};

/**
 * Walks the extracted dataset directory and routes every JSON file by its
 * envelope key (bill / roll_call / person) — never by directory layout, so
 * an archive reorganization cannot misroute a file. Non-payload JSONs are
 * ignored.
 */
export function readLegiscanDataset(datasetDir: string): LegiscanDataset {
  const dataset: LegiscanDataset = { billsById: new Map(), votes: [], people: [], fileErrors: [] };
  const entries = readdirSync(datasetDir, { recursive: true }) as string[];
  for (const entry of entries.filter((file) => file.endsWith(".json")).sort()) {
    let payload;
    try {
      payload = classifyLegiscanDatasetFile(JSON.parse(readFileSync(join(datasetDir, entry), "utf8")) as unknown);
    } catch (error) {
      dataset.fileErrors.push({ file: entry, error: errorMessage(error) });
      continue;
    }
    if (payload.kind === "bill") {
      try {
        const bill = parseLegiscanBill(payload.bill);
        if (dataset.billsById.has(bill.billId)) {
          throw new Error(`bill_id ${bill.billId} appears in more than one file`);
        }
        dataset.billsById.set(bill.billId, bill);
      } catch (error) {
        dataset.fileErrors.push({ file: entry, error: errorMessage(error) });
      }
    } else if (payload.kind === "vote") {
      dataset.votes.push({ file: entry, rollCall: payload.rollCall });
    } else if (payload.kind === "person") {
      dataset.people.push(payload.person);
    }
  }
  return dataset;
}

// The survey: the state's desc vocabulary as measured, one row per
// (chamber, normalized desc), with the tally range that separates floor
// from committee at a glance. This is what a human reads before writing
// the state's legiscanStateConfigs.ts entry.
export type LegiscanSurveyRow = {
  chamber: string;
  desc: string;
  count: number;
  // How many of `count` carry NO member positions (a summary-only tally,
  // e.g. a Texas Senate non-record vote). A kept-looking desc that is
  // mostly summary-only tells the config author what the state actually
  // records — the fetcher can only import the recorded remainder.
  withoutMemberList: number;
  minTotal: number;
  maxTotal: number;
  billTypes: string[];
  sampleBills: string[];
};

export function surveyLegiscanDataset(dataset: LegiscanDataset): {
  rows: LegiscanSurveyRow[];
  billTypeCounts: Record<string, number>;
  sessionIds: Record<string, number>;
  committeeChamberVotes: number;
  voteParseErrors: { file: string; error: string }[];
} {
  const rows = new Map<string, LegiscanSurveyRow & { billTypeSet: Set<string> }>();
  const billTypeCounts: Record<string, number> = {};
  const sessionIds: Record<string, number> = {};
  let committeeChamberVotes = 0;
  const voteParseErrors: { file: string; error: string }[] = [];
  for (const bill of dataset.billsById.values()) {
    billTypeCounts[bill.billType] = (billTypeCounts[bill.billType] ?? 0) + 1;
    sessionIds[String(bill.sessionId)] = (sessionIds[String(bill.sessionId)] ?? 0) + 1;
  }
  for (const vote of dataset.votes) {
    // A joint/committee body's tally (Connecticut prints `J`): counted so
    // the survey shows how much of the feed is committee work, but kept out
    // of the desc histogram, which exists to name the FLOOR vocabulary.
    if (isLegiscanCommitteeChamberRollCall(vote.rollCall)) {
      committeeChamberVotes += 1;
      continue;
    }
    let rollCall;
    try {
      rollCall = parseLegiscanRollCall(vote.rollCall);
    } catch (error) {
      voteParseErrors.push({ file: vote.file, error: errorMessage(error) });
      continue;
    }
    const bill = dataset.billsById.get(rollCall.billId);
    const key = `${rollCall.chamber}:${rollCall.desc.toLowerCase().replace(/\s+/g, " ").trim()}`;
    const row = rows.get(key) ?? {
      chamber: rollCall.chamber,
      desc: rollCall.desc,
      count: 0,
      withoutMemberList: 0,
      minTotal: rollCall.total,
      maxTotal: rollCall.total,
      billTypes: [],
      billTypeSet: new Set<string>(),
      sampleBills: [],
    };
    row.count += 1;
    if (rollCall.votes.length === 0) {
      row.withoutMemberList += 1;
    }
    row.minTotal = Math.min(row.minTotal, rollCall.total);
    row.maxTotal = Math.max(row.maxTotal, rollCall.total);
    if (bill) {
      row.billTypeSet.add(bill.billType);
      if (row.sampleBills.length < 3 && !row.sampleBills.includes(bill.billNumber)) {
        row.sampleBills.push(bill.billNumber);
      }
    }
    rows.set(key, row);
  }
  return {
    rows: [...rows.values()]
      .map(({ billTypeSet, ...row }) => ({ ...row, billTypes: [...billTypeSet].sort() }))
      .sort((a, b) => b.count - a.count || a.chamber.localeCompare(b.chamber) || a.desc.localeCompare(b.desc)),
    billTypeCounts,
    sessionIds,
    committeeChamberVotes,
    voteParseErrors,
  };
}

/** `--bills hb1,SB0544` → the measure_id spellings to keep (`HB 1`, `SB 544`). */
export function parseLegiscanBillList(raw: string): Set<string> {
  const measures = new Set<string>();
  for (const part of raw.split(",")) {
    const token = part.trim();
    if (token.length === 0) {
      continue;
    }
    const match = /^([A-Za-z]+)\s*0*(\d+)$/.exec(token);
    if (!match) {
      throw new Error(`--bills entry is not a bill number: ${token}`);
    }
    measures.add(`${match[1]!.toUpperCase()} ${match[2]}`);
  }
  if (measures.size === 0) {
    throw new Error("--bills names no bills");
  }
  return measures;
}

/**
 * What makes two roll calls the same legislative action. LegiScan's senate
 * feeds can issue several roll_call_ids for one action — TX 89R re-issued
 * 640 of 6,824 stored rolls, all Senate, with every field except
 * roll_call_id byte-identical (evidence/rollcall/legiscan-tx-2160/
 * CODE-FINDINGS.md §1). The fan-out dedupes by `ls:<roll_call_id>`, so
 * each extra id would become a near-identical record on the same member;
 * the fetch collapses them on this key instead.
 */
export function legiscanRollCallIdentityKey(rollCall: LegiscanRollCall): string {
  const memberListSha1 = createHash("sha1")
    .update(
      [...rollCall.votes]
        .sort((a, b) => a.peopleId - b.peopleId)
        .map((vote) => `${vote.peopleId}:${vote.voteId}`)
        .join(",")
    )
    .digest("hex");
  return JSON.stringify([
    rollCall.chamber,
    rollCall.billId,
    rollCall.date,
    rollCall.desc,
    rollCall.yea,
    rollCall.nay,
    rollCall.nv,
    rollCall.absent,
    rollCall.passed,
    memberListSha1,
  ]);
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

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  assertKnownCliFlags("rollcall:legiscan:fetch", argv, [
    { name: "--state", value: "both" },
    { name: "--dataset-dir", value: "both" },
    { name: "--survey", value: "none" },
    { name: "--bills", value: "both" },
    { name: "--evidence-dir", value: "both" },
    { name: "--dry-run", value: "none" },
  ]);

  const stateRaw = readValueFlag(argv, "--state");
  if (stateRaw === null) {
    throw new Error("--state is required (e.g. --state TX)");
  }
  const state = stateRaw.trim().toUpperCase();
  const datasetDirRaw = readValueFlag(argv, "--dataset-dir");
  if (datasetDirRaw === null) {
    throw new Error("--dataset-dir is required (the extracted LegiScan session dataset)");
  }
  const datasetDir = resolve(datasetDirRaw);
  const survey = argv.includes("--survey");
  const billsFlag = readValueFlag(argv, "--bills");
  const dryRun = argv.includes("--dry-run");
  const billFilter = billsFlag === null ? null : parseLegiscanBillList(billsFlag);

  const startedAt = new Date();
  const runId = `rollcall-legiscan-fetch-${state.toLowerCase()}-${startedAt
    .toISOString()
    .replace(/[-:]/g, "")
    .replace(/\.\d{3}Z$/, "Z")}`;
  const evidenceDir = resolve(readValueFlag(argv, "--evidence-dir") ?? resolve(process.cwd(), "evidence", "rollcall", runId));
  mkdirSync(evidenceDir, { recursive: true });

  const dataset = readLegiscanDataset(datasetDir);

  if (survey) {
    const surveyed = surveyLegiscanDataset(dataset);
    const report = {
      runId,
      importerVersion: LEGISCAN_ROLLCALL_FETCH_IMPORTER_VERSION,
      survey: true,
      state,
      startedAt: startedAt.toISOString(),
      finishedAt: new Date().toISOString(),
      datasetDir,
      bills: dataset.billsById.size,
      votes: dataset.votes.length,
      people: dataset.people.length,
      fileErrors: dataset.fileErrors,
      ...surveyed,
    };
    writeFileSync(resolve(evidenceDir, `${runId}-survey.json`), `${JSON.stringify(report, null, 2)}\n`);
    console.log(JSON.stringify({ ...report, rows: report.rows.slice(0, 40) }, null, 2));
    if (dataset.fileErrors.length > 0 || surveyed.voteParseErrors.length > 0) {
      process.exitCode = 1;
    }
    return;
  }

  const config = getLegiscanStateConfig(state);
  const session = String(config.sessionId);

  // The dataset must be exactly the configured state and session — a
  // mis-pointed directory must stop the run before anything is stored.
  for (const bill of dataset.billsById.values()) {
    if (bill.state !== config.jurisdiction) {
      throw new Error(`${datasetDir} holds ${bill.state} bill ${bill.billNumber}; the run is --state ${state}`);
    }
    if (bill.sessionId !== config.sessionId) {
      throw new Error(
        `${datasetDir} holds session ${bill.sessionId} bill ${bill.billNumber}; the config pins session ${config.sessionId}`
      );
    }
  }
  if (dataset.votes.length > MAX_VOTES_PER_RUN) {
    throw new Error(`${dataset.votes.length} roll calls is more than ${MAX_VOTES_PER_RUN}; is --dataset-dir one session?`);
  }

  loadProjectEnv();
  let pool: Pool | null = null;
  if (!dryRun) {
    const databaseUrl = process.env.DATABASE_URL?.trim();
    if (!databaseUrl) {
      throw new Error("DATABASE_URL is required to store roll calls (or pass --dry-run)");
    }
    pool = new Pool({ connectionString: databaseUrl });
  }

  const rows: LegiscanRollCallFetchReportRow[] = [];
  let excludedMeasureVotes = 0;
  let committeeVotes = 0;
  let unrecordedVotes = 0;
  let duplicateVotes = 0;
  let billFilterMisses = 0;
  const rollOwner = new Map<number, string>();
  const keeperRollByIdentity = new Map<string, number>();
  try {
    // Deterministic order: by roll_call_id, which LegiScan issues in time
    // order. Parse errors surface as rows; unparseable ids sort first.
    const votes = dataset.votes
      .map((vote) => ({ ...vote, sortKey: typeof vote.rollCall.roll_call_id === "number" ? vote.rollCall.roll_call_id : 0 }))
      .sort((a, b) => a.sortKey - b.sortKey);
    for (const vote of votes) {
      // Committee bodies that are not a chamber (Connecticut's joint
      // standing committees, chamber `J`) are rejected before the queue —
      // the same disposition an unknown-desc committee-sized tally gets,
      // decided on the chamber code because such a roll has no chamber to
      // size against.
      if (isLegiscanCommitteeChamberRollCall(vote.rollCall)) {
        committeeVotes += 1;
        continue;
      }
      const row: LegiscanRollCallFetchReportRow = {
        bill: "",
        chamber: null,
        roll: null,
        desc: null,
        outcome: "parse_error",
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
        error: null,
      };
      try {
        const rollCall = parseLegiscanRollCall(vote.rollCall);
        row.chamber = rollCall.chamber;
        row.roll = rollCall.rollCallId;
        row.desc = rollCall.desc;
        row.voteDate = rollCall.date;
        row.yeas = rollCall.yea;
        row.nays = rollCall.nay;
        row.result = rollCall.passed ? "Passed" : "Failed";
        const bill = dataset.billsById.get(rollCall.billId);
        if (!bill) {
          throw new Error(`roll call ${rollCall.rollCallId} names bill_id ${rollCall.billId}, which the dataset does not hold`);
        }
        row.bill = bill.billNumber;
        row.measureId = bill.measureId;
        if (billFilter !== null && !billFilter.has(bill.measureId)) {
          billFilterMisses += 1;
          continue;
        }
        // Summary tallies with no member positions (a Texas Senate
        // non-record vote): nothing to fan out, so never stored — counted,
        // with the survey's per-desc withoutMemberList column as the place
        // to see which questions a state publishes this way.
        if (rollCall.votes.length === 0) {
          unrecordedVotes += 1;
          continue;
        }

        const classification = classifyLegiscanRollCall({
          desc: rollCall.desc,
          total: rollCall.total,
          chamber: rollCall.chamber,
          billType: bill.billType,
          config,
        });
        row.isFloorVote = classification.isFloorVote;
        row.questionClass = classification.questionClass;
        row.classificationReason = classification.reason;
        // Rejected before the queue, counted, never stored — the survey
        // report is where their vocabulary is inspected.
        if (classification.reason.startsWith("excluded_measure:")) {
          excludedMeasureVotes += 1;
          continue;
        }
        if (classification.reason.startsWith("committee_tally:")) {
          committeeVotes += 1;
          continue;
        }
        // The same roll_call_id in two dataset files is a malformed
        // dataset, never a LegiScan re-issue (re-issues carry NEW ids) —
        // so the collision must surface before the identity collapse
        // below could swallow an identical-content repeat.
        const owner = rollOwner.get(rollCall.rollCallId);
        if (owner !== undefined) {
          row.outcome = "collision";
          row.error = `roll_call_id ${rollCall.rollCallId} already belongs to ${owner} in this dataset`;
          rows.push(row);
          continue;
        }
        rollOwner.set(rollCall.rollCallId, bill.billNumber);

        // A re-issued id for an action already kept this run: votes are
        // processed in ascending roll_call_id order, so the kept id is the
        // lowest of its group — the rest are counted, never stored.
        const identity = legiscanRollCallIdentityKey(rollCall);
        if (keeperRollByIdentity.has(identity)) {
          duplicateVotes += 1;
          continue;
        }
        keeperRollByIdentity.set(identity, rollCall.rollCallId);
        rows.push(row);

        // The record's source_url: the bill feed's own per-roll page URL,
        // falling back to the documented page shape. Either way it must
        // name exactly this roll call, or the dedupe scan could not.
        const machineUrl =
          bill.voteUrlsByRollCallId.get(rollCall.rollCallId) ??
          legiscanRollCallPageUrl(config.jurisdiction, bill.billNumber, rollCall.rollCallId);
        if (rollCallUrlKey(machineUrl)?.key !== `ls:${rollCall.rollCallId}`) {
          throw new Error(`vote url ${machineUrl} does not name roll call ${rollCall.rollCallId}`);
        }

        if (pool) {
          const stored = await pool.query<{ measure_id: string | null }>(
            `SELECT measure_id FROM legislative_votes
              WHERE jurisdiction = $1 AND chamber = $2 AND session = $3 AND roll_number = $4`,
            [config.jurisdiction, rollCall.chamber, session, rollCall.rollCallId]
          );
          if (stored.rows[0] !== undefined && stored.rows[0].measure_id !== bill.measureId) {
            row.outcome = "collision";
            row.error = `roll ${rollCall.rollCallId} is stored as ${stored.rows[0].measure_id ?? "no measure"}, not ${bill.measureId}`;
            continue;
          }
        }

        const sourceSha256 = legiscanRollCallSha256(vote.rollCall);
        const evidenceFile = legiscanEvidenceFileName(config.jurisdiction, rollCall.chamber, config.sessionId, rollCall.rollCallId);
        const evidence: LegiscanVoteEvidence = {
          jurisdiction: config.jurisdiction,
          sessionId: config.sessionId,
          chamber: rollCall.chamber,
          rollNumber: rollCall.rollCallId,
          bill: bill.billNumber,
          measureId: bill.measureId,
          machineUrl,
          fetchedAt: new Date().toISOString(),
          rollCall: vote.rollCall,
        };
        writeFileSync(resolve(evidenceDir, evidenceFile), `${JSON.stringify(evidence, null, 2)}\n`);
        row.evidenceFile = evidenceFile;

        if (!pool) {
          row.outcome = "dry_run";
          continue;
        }
        const result = await upsertLegislativeVoteSource(pool, {
          jurisdiction: config.jurisdiction,
          chamber: rollCall.chamber,
          session,
          rollNumber: rollCall.rollCallId,
          voteDate: rollCall.date,
          measureId: bill.measureId,
          exactQuestion: rollCall.desc,
          isFloorVote: classification.isFloorVote,
          result: rollCall.passed ? "Passed" : "Failed",
          yeas: rollCall.yea,
          nays: rollCall.nay,
          displayUrl: machineUrl,
          machineUrl,
          billUrl: bill.stateLink ?? bill.legiscanUrl,
          sourceSha256,
          fetchedAt: new Date(),
          importerVersion: LEGISCAN_ROLLCALL_FETCH_IMPORTER_VERSION,
        });
        row.outcome = result.outcome;
        row.judgmentCleared = result.judgmentCleared;
      } catch (error) {
        row.outcome = "parse_error";
        row.error = errorMessage(error);
        if (!rows.includes(row)) {
          rows.push(row);
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
  const surfaced = rows.filter((row) => row.isFloorVote === null && row.outcome !== "parse_error").length;
  const report = {
    runId,
    importerVersion: LEGISCAN_ROLLCALL_FETCH_IMPORTER_VERSION,
    jurisdiction: config.jurisdiction,
    sessionId: config.sessionId,
    dryRun,
    startedAt: startedAt.toISOString(),
    finishedAt: new Date().toISOString(),
    datasetDir,
    evidenceDir,
    bills: dataset.billsById.size,
    datasetVotes: dataset.votes.length,
    billFilter: billFilter === null ? null : [...billFilter].sort(),
    billFilterMisses,
    excludedMeasureVotes,
    committeeVotes,
    unrecordedVotes,
    duplicateVotes,
    floorVotes,
    surfaced,
    fileErrors: dataset.fileErrors,
    counts,
    rows,
  };
  writeFileSync(resolve(evidenceDir, `${runId}-report.json`), `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify({ ...report, rows: undefined }, null, 2));
  if (rows.some((row) => FAILURE_OUTCOMES.has(row.outcome)) || dataset.fileErrors.length > 0) {
    process.exitCode = 1;
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("rollcall:legiscan:fetch failed:", errorMessage(error));
    process.exitCode = 1;
  });
}
