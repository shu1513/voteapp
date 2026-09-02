// Read-only: build the period ledger for every active Kansas finance link
// from the live SOS CFR viewer and print it. Writes nothing — the Phase 4
// sync will consume the same ledgers.

import { pathToFileURL } from "node:url";

import { Pool } from "pg";

import { loadProjectEnv } from "../config/env.js";
import { isKansasCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import { listDueKansasCandidateFinanceSyncRows } from "../pipeline/kansasFinance/kansasCandidateFinanceDueList.js";
import {
  buildKansasCandidateLedger,
  type KansasCandidateLedgerResult,
} from "../pipeline/kansasFinance/kansasCandidateLedger.js";
import { kansasCfrOfficeForRace } from "../pipeline/kansasFinance/kansasFinanceEligibleOffices.js";
import { createKansasFilingPoolLoader } from "../pipeline/kansasFinance/kansasFilingSearch.js";
import { assertKnownCliFlags } from "./financeCliFlagGuard.js";

export type ReportKansasCandidateFinanceLedgersScriptOptions = {
  force: boolean;
  maxCandidates: number;
  electionLookbackDays: number;
  electionLookaheadDays: number;
};

const BOOLEAN_FLAGS = new Set(["--force"]);
const VALUE_FLAGS = new Set(["--max-candidates", "--lookback-days", "--lookahead-days"]);

function parsePositiveInteger(args: readonly string[], name: string, fallback: number): number {
  const values: string[] = [];
  const prefix = `${name}=`;
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]!;
    if (arg.startsWith(prefix)) {
      values.push(arg.slice(prefix.length).trim());
    } else if (arg === name) {
      values.push(args[index + 1]!.trim());
      index += 1;
    }
  }
  if (values.length > 1) throw new Error(`Provide ${name} at most once`);
  const value = values[0];
  if (value === undefined) return fallback;
  if (!/^[1-9]\d*$/.test(value) || !Number.isSafeInteger(Number(value))) {
    throw new Error(`Invalid ${name} value: ${value}`);
  }
  return Number(value);
}

export function parseReportKansasCandidateFinanceLedgersScriptArgs(
  args: readonly string[]
): ReportKansasCandidateFinanceLedgersScriptOptions {
  assertKnownCliFlags(args, "Kansas candidate finance ledger report", BOOLEAN_FLAGS, VALUE_FLAGS);
  return {
    force: args.includes("--force"),
    maxCandidates: parsePositiveInteger(args, "--max-candidates", 25),
    electionLookbackDays: parsePositiveInteger(args, "--lookback-days", 98),
    electionLookaheadDays: parsePositiveInteger(args, "--lookahead-days", 730),
  };
}

type LedgerReportRow = {
  candidateId: string;
  electionId: string;
  candidateName: string;
  committeeId: string;
  status: KansasCandidateLedgerResult["status"] | "error";
  reason?: string;
  filedNames?: string[];
  complete?: boolean;
  periods?: Record<string, string>;
  reports?: number;
  paperReports?: number;
  appointments?: number;
  affidavits?: number;
  lastMinuteFilings?: number;
  outOfCycleFilings?: number;
  unexpectedFilings?: number;
  error?: string;
};

function getDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for the Kansas candidate finance ledger report");
  }
  return databaseUrl;
}

async function main(): Promise<void> {
  const startedAt = new Date();
  loadProjectEnv();
  const options = parseReportKansasCandidateFinanceLedgersScriptArgs(process.argv.slice(2));

  if (!isKansasCampaignFinanceSyncEnabled(options.force)) {
    console.log("Kansas campaign finance sync disabled; no ledgers built");
    return;
  }

  const pool = new Pool({ connectionString: getDatabaseUrl() });
  try {
    // staleAfterDays 0: every active link, synced or not — this is a report, not a sync.
    const due = await listDueKansasCandidateFinanceSyncRows(pool, {
      now: startedAt,
      staleAfterDays: 0,
      maxCandidates: options.maxCandidates,
      electionLookbackDays: options.electionLookbackDays,
      electionLookaheadDays: options.electionLookaheadDays,
    });
    const loadFilingPool = createKansasFilingPoolLoader({
      now: startedAt,
      onSkippedRows: (office, skipped) =>
        console.warn(`Kansas ledger report: ${skipped} ${office.label} rows carried another office and were skipped`),
    });
    const results: LedgerReportRow[] = [];
    for (const row of due.rows) {
      const base = {
        candidateId: row.candidateId,
        electionId: row.electionId,
        candidateName: row.candidateName,
        committeeId: row.committeeId,
      };
      const office = kansasCfrOfficeForRace({ officeScope: row.officeScope, officeCanonicalName: row.officeName });
      if (office === null) {
        results.push({ ...base, status: "unresolved", reason: "office_unmapped" });
        continue;
      }
      try {
        const result = await buildKansasCandidateLedger({
          target: { committeeId: row.committeeId, office, electionYear: row.electionYear },
          now: startedAt,
          loadFilingPool,
        });
        if (result.status === "resolved") {
          results.push({
            ...base,
            status: "resolved",
            complete: result.complete,
            periods: Object.fromEntries(result.ledger.entries.map((entry) => [entry.period.key, entry.status])),
            reports: result.reports.length,
            paperReports: result.paperReports.length,
            appointments: result.appointments.length,
            affidavits: result.affidavitDates.length,
            lastMinuteFilings: result.ledger.lastMinuteFilings.length,
            outOfCycleFilings: result.ledger.outOfCycleFilings.length,
            unexpectedFilings: result.ledger.unexpectedFilings.length,
          });
        } else {
          results.push({ ...base, status: "unresolved", reason: result.reason, filedNames: result.filedNames });
        }
      } catch (error) {
        results.push({ ...base, status: "error", error: error instanceof Error ? error.message : String(error) });
      }
    }
    const count = (predicate: (row: LedgerReportRow) => boolean) => results.filter(predicate).length;
    console.log(
      JSON.stringify(
        {
          type: "kansas_candidate_finance_ledger_report",
          ts: new Date().toISOString(),
          started_at: startedAt.toISOString(),
          total_active_links: due.totalDueRows,
          attempted: results.length,
          complete: count((row) => row.status === "resolved" && row.complete === true),
          incomplete: count((row) => row.status === "resolved" && row.complete === false),
          unresolved: count((row) => row.status === "unresolved"),
          errors: count((row) => row.status === "error"),
          results,
        },
        null,
        2
      )
    );
  } finally {
    await pool.end();
  }
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    console.error("Kansas candidate finance ledger report failed:", error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
