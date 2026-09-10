import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import {
  classifyHawaiiAction,
  hawaiiEvidenceFileName,
  hawaiiExpectedSeats,
  hawaiiHistorySha256,
  hawaiiKeptFloorDayCollisions,
  hawaiiMeasureUrl,
  hawaiiRollNumber,
  hawaiiSittingMembers,
  parseHawaiiFloorVoteText,
  parseHawaiiHistoryRow,
  parseHawaiiSeatFile,
  readHawaiiDataset,
  reconstructHawaiiMembers,
  HAWAII_JURISDICTION,
  type HawaiiHistoryRow,
  type HawaiiQuestionClass,
  type HawaiiSeatFile,
  type HawaiiVoteEvidence,
} from "../pipeline/rollcall/hawaiiRollCall.js";
import { parseLegiscanPerson, type LegiscanPerson } from "../pipeline/rollcall/legiscanMemberResolver.js";
import { formatLegiscanMeasureId } from "../pipeline/rollcall/legiscanRollCall.js";
import {
  upsertLegislativeVoteSource,
  type LegislativeVoteUpsertOutcome,
} from "../pipeline/rollcall/legislativeVoteStore.js";
import type { LegislativeVoteChamber } from "../pipeline/rollcall/legislativeVotes.js";
import { reportPath } from "../pipeline/rollcall/rollCallReportPaths.js";
import { readPositiveIntegerFlag } from "../utils/cliFlags.js";
import { assertKnownCliFlags } from "./manualCliFlags.js";

// Hawaii fetcher (the second dedicated state pipeline after Ohio): reads an
// extracted LegiScan dataset, finds every floor passage line in each bill's
// history, reconstructs the member lists from the named dissenters (see
// hawaiiRollCall.ts for why Hawaii needs this), writes one evidence file
// per vote, and stores it on legislative_votes as a pending review-queue
// row. Lines whose names or counts cannot be reconciled are reported as
// parse errors and never stored. Offline, no candidate writes, no AI.
//
//   npm run rollcall:hi:fetch -- --session 2175 --dataset-dir ~/legiscan-data/hi-2175/HI/2025-2025_Regular_Session \
//     --evidence-dir ~/legiscan-data/hi-2175-evidence
//   npm run rollcall:hi:fetch -- --session 2245 --dataset-dir <dir> --seat-file evidence/rollcall/hawaii-2245/seats.json --dry-run

export const HAWAII_ROLLCALL_FETCH_IMPORTER_VERSION = "rollcall-hi-fetch-v1";

export type HawaiiRollCallFetchReportRow = {
  bill: string;
  billStatus: number | null;
  chamber: LegislativeVoteChamber | null;
  roll: number | null;
  historyIndex: number;
  action: string;
  outcome: LegislativeVoteUpsertOutcome | "dry_run" | "excluded_measure" | "prior_session" | "parse_error" | "collision";
  evidenceFile: string | null;
  voteDate: string | null;
  measureId: string | null;
  reading: HawaiiQuestionClass | null;
  draft: string | null;
  yeas: number | null;
  nays: number | null;
  excused: number | null;
  classificationReason: string | null;
  judgmentCleared: boolean;
  error: string | null;
};

const FAILURE_OUTCOMES: ReadonlySet<HawaiiRollCallFetchReportRow["outcome"]> = new Set(["parse_error", "collision"]);

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

/** `--bills hb957,SB0897` → the measure_id spellings to keep (`HB 957`, `SB 897`). */
export function parseHawaiiBillList(raw: string): Set<string> {
  const measures = new Set<string>();
  for (const part of raw.split(",")) {
    const token = part.trim();
    if (token.length === 0) {
      continue;
    }
    if (!/^[A-Za-z]+\s*0*\d+$/.test(token)) {
      throw new Error(`--bills entry is not a bill number: ${token}`);
    }
    measures.add(formatLegiscanMeasureId(token));
  }
  if (measures.size === 0) {
    throw new Error("--bills names no bills");
  }
  return measures;
}

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  assertKnownCliFlags("rollcall:hi:fetch", argv, [
    { name: "--session", value: "both" },
    { name: "--dataset-dir", value: "both" },
    { name: "--bills", value: "both" },
    { name: "--seat-file", value: "both" },
    { name: "--evidence-dir", value: "both" },
    { name: "--dry-run", value: "none" },
  ]);
  const sessionId = readPositiveIntegerFlag(argv, "--session", 0);
  if (sessionId === 0) {
    throw new Error("--session is required (the LegiScan session_id, e.g. --session 2175)");
  }
  const datasetDirRaw = readValueFlag(argv, "--dataset-dir");
  if (datasetDirRaw === null) {
    throw new Error("--dataset-dir is required (the extracted LegiScan session dataset)");
  }
  const datasetDir = resolve(datasetDirRaw);
  const billsFlag = readValueFlag(argv, "--bills");
  const billFilter = billsFlag === null ? null : parseHawaiiBillList(billsFlag);
  const seatFileRaw = readValueFlag(argv, "--seat-file");
  const dryRun = argv.includes("--dry-run");

  const startedAt = new Date();
  const runId = `rollcall-hi-fetch-${sessionId}-${startedAt
    .toISOString()
    .replace(/[-:]/g, "")
    .replace(/\.\d{3}Z$/, "Z")}`;
  const evidenceDir = resolve(readValueFlag(argv, "--evidence-dir") ?? resolve(process.cwd(), "evidence", "rollcall", runId));
  mkdirSync(evidenceDir, { recursive: true });

  const dataset = readHawaiiDataset(datasetDir);
  if (dataset.fileErrors.length > 0) {
    throw new Error(
      `${datasetDir} has unreadable files: ${dataset.fileErrors.map((entry) => `${entry.file} (${entry.error})`).join("; ")}`
    );
  }
  for (const { summary } of dataset.billsById.values()) {
    if (summary.state !== HAWAII_JURISDICTION || summary.sessionId !== sessionId) {
      throw new Error(
        `${datasetDir} holds ${summary.state} session ${summary.sessionId} (bill ${summary.billNumber}); the run is --session ${sessionId}`
      );
    }
  }
  const people: LegiscanPerson[] = [];
  for (const raw of dataset.people) {
    const person = parseLegiscanPerson(raw);
    if (person !== null) {
      people.push(person);
    }
  }
  if (people.length === 0) {
    throw new Error(`${datasetDir} holds no people`);
  }
  let seats: HawaiiSeatFile | null = null;
  if (seatFileRaw !== null) {
    seats = parseHawaiiSeatFile(JSON.parse(readFileSync(resolve(seatFileRaw), "utf8")) as unknown, sessionId);
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

  const session = String(sessionId);
  const rows: HawaiiRollCallFetchReportRow[] = [];
  let billsScanned = 0;
  let billsWithFloorVotes = 0;
  let billFilterMisses = 0;
  const rollOwner = new Map<string, string>();
  try {
    const bills = [...dataset.billsById.values()].sort((a, b) => a.summary.billId - b.summary.billId);
    for (const { summary, status, yearStart, history } of bills) {
      const sessionStart = `${yearStart}-01-01`;
      if (billFilter !== null && !billFilter.has(summary.measureId)) {
        billFilterMisses += 1;
        continue;
      }
      billsScanned += 1;
      const machineUrl = hawaiiMeasureUrl(summary);

      // First pass: which history rows are floor passage lines at all.
      const floorRows: { row: HawaiiHistoryRow; raw: unknown; reason: string; kept: boolean; questionClass: HawaiiQuestionClass | null }[] = [];
      for (const [index, raw] of history.entries()) {
        let row: HawaiiHistoryRow;
        try {
          row = parseHawaiiHistoryRow(raw, index);
        } catch (error) {
          // A history row that cannot even be read is only a problem when
          // it is a passage line; report it as such, skip the rest.
          const action = typeof (raw as { action?: unknown })?.action === "string" ? ((raw as { action: string }).action as string) : "";
          if (classifyHawaiiAction(action, summary.billType).questionClass !== null) {
            rows.push({
              bill: summary.billNumber,
              billStatus: status,
              chamber: null,
              roll: null,
              historyIndex: index,
              action: action.slice(0, 200),
              outcome: "parse_error",
              evidenceFile: null,
              voteDate: null,
              measureId: summary.measureId,
              reading: null,
              draft: null,
              yeas: null,
              nays: null,
              excused: null,
              classificationReason: null,
              judgmentCleared: false,
              error: errorMessage(error),
            });
          }
          continue;
        }
        const classification = classifyHawaiiAction(row.action, summary.billType);
        if (classification.questionClass === null) {
          continue;
        }
        floorRows.push({ row, raw, reason: classification.reason, kept: classification.isFloorVote, questionClass: classification.questionClass });
      }
      if (floorRows.length === 0) {
        continue;
      }
      billsWithFloorVotes += 1;
      const collidingDays = hawaiiKeptFloorDayCollisions(
        floorRows.filter((entry) => entry.kept && entry.row.date >= sessionStart).map((entry) => entry.row)
      );

      for (const entry of floorRows) {
        const { row } = entry;
        const reportRow: HawaiiRollCallFetchReportRow = {
          bill: summary.billNumber,
          billStatus: status,
          chamber: row.chamber,
          roll: null,
          historyIndex: row.index,
          action: row.action.slice(0, 200),
          outcome: "parse_error",
          evidenceFile: null,
          voteDate: row.date,
          measureId: summary.measureId,
          reading: entry.questionClass,
          draft: null,
          yeas: null,
          nays: null,
          excused: null,
          classificationReason: entry.reason,
          judgmentCleared: false,
          error: null,
        };
        rows.push(reportRow);
        try {
          if (!entry.kept) {
            reportRow.outcome = "excluded_measure";
            continue;
          }
          if (row.date < sessionStart) {
            // A carried-over bill's earlier-session vote; the earlier
            // session's dataset holds it under its own session id.
            reportRow.outcome = "prior_session";
            continue;
          }
          const text = parseHawaiiFloorVoteText(row.action, row.chamber);
          reportRow.draft = text.draft;
          const sitting = hawaiiSittingMembers(people, row.chamber, row.date, seats);
          const members = reconstructHawaiiMembers(text, row.chamber, sitting, hawaiiExpectedSeats(row.chamber, row.date, seats));
          reportRow.yeas = members.yeas.length;
          reportRow.nays = members.nays.length;
          reportRow.excused = members.excused.length;

          if (collidingDays.has(`${row.chamber}:${row.date}`)) {
            reportRow.outcome = "collision";
            reportRow.error = `${summary.billNumber} has two floor passage votes in the ${row.chamber} on ${row.date}; the per-bill source URL cannot tell them apart`;
            continue;
          }
          const roll = hawaiiRollNumber(summary.billId, row.index);
          reportRow.roll = roll;
          const rollKey = `${row.chamber}:${roll}`;
          const owner = rollOwner.get(rollKey);
          if (owner !== undefined) {
            reportRow.outcome = "collision";
            reportRow.error = `roll ${roll} (${row.chamber}) already belongs to ${owner} in this run`;
            continue;
          }
          rollOwner.set(rollKey, summary.billNumber);
          if (pool) {
            const stored = await pool.query<{ measure_id: string | null }>(
              `SELECT measure_id FROM legislative_votes
                WHERE jurisdiction = $1 AND chamber = $2 AND session = $3 AND roll_number = $4`,
              [HAWAII_JURISDICTION, row.chamber, session, roll]
            );
            if (stored.rows[0] !== undefined && stored.rows[0].measure_id !== summary.measureId) {
              reportRow.outcome = "collision";
              reportRow.error = `roll ${roll} (${row.chamber}) is stored as ${stored.rows[0].measure_id ?? "no measure"}, not ${summary.measureId}`;
              continue;
            }
          }

          const evidenceFile = hawaiiEvidenceFileName(row.chamber, sessionId, roll);
          const evidence: HawaiiVoteEvidence = {
            jurisdiction: HAWAII_JURISDICTION,
            sessionId,
            chamber: row.chamber,
            rollNumber: roll,
            billId: summary.billId,
            bill: summary.billNumber,
            measureId: summary.measureId,
            historyIndex: row.index,
            machineUrl,
            fetchedAt: new Date().toISOString(),
            reading: text.reading,
            draft: text.draft,
            history: entry.raw,
            named: { reservations: text.reservations, noes: text.noes, excused: text.excused },
            members,
          };
          writeFileSync(resolve(evidenceDir, evidenceFile), `${JSON.stringify(evidence, null, 2)}\n`);
          reportRow.evidenceFile = evidenceFile;

          if (!pool) {
            reportRow.outcome = "dry_run";
            continue;
          }
          const result = await upsertLegislativeVoteSource(pool, {
            jurisdiction: HAWAII_JURISDICTION,
            chamber: row.chamber,
            session,
            rollNumber: roll,
            voteDate: row.date,
            measureId: summary.measureId,
            exactQuestion: row.action,
            isFloorVote: true,
            result: "Passed",
            yeas: members.yeas.length,
            nays: members.nays.length,
            displayUrl: machineUrl,
            machineUrl,
            billUrl: machineUrl,
            sourceSha256: hawaiiHistorySha256(entry.raw),
            fetchedAt: new Date(),
            importerVersion: HAWAII_ROLLCALL_FETCH_IMPORTER_VERSION,
          });
          reportRow.outcome = result.outcome;
          reportRow.judgmentCleared = result.judgmentCleared;
        } catch (error) {
          reportRow.outcome = "parse_error";
          reportRow.error = errorMessage(error);
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
  const report = {
    runId,
    importerVersion: HAWAII_ROLLCALL_FETCH_IMPORTER_VERSION,
    jurisdiction: HAWAII_JURISDICTION,
    sessionId,
    dryRun,
    startedAt: startedAt.toISOString(),
    finishedAt: new Date().toISOString(),
    datasetDir: reportPath(datasetDir),
    evidenceDir: reportPath(evidenceDir),
    seatFile: seatFileRaw === null ? null : reportPath(seatFileRaw),
    bills: dataset.billsById.size,
    people: people.length,
    billsScanned,
    billsWithFloorVotes,
    billFilterMisses,
    floorVotes: rows.filter((row) => row.classificationReason?.startsWith("kept:")).length,
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
    console.error("rollcall:hi:fetch failed:", errorMessage(error));
    process.exitCode = 1;
  });
}
