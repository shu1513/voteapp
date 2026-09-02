// Montana IE sweep quarantine report (docs/plans/montana-finance.md, Phase
// 2b): classifies the cached yearly sweep and prints where the dollars sit —
// resolved by stance, and quarantined by committee and reason. This is the
// working input for Phase 3 quarantine triage and the attachment-recovery
// campaign (Conservatives4MT first). No database access.
//
//   npm run montana-candidates:finance:outside-report -- --year 2026
//   npm run montana-candidates:finance:outside-report -- --year 2026 --refresh
//
// --refresh re-harvests the sweep first (an explicit manual action; the
// scheduled batch refreshes the sweep itself when the raw-refresh flag is
// on).

import { pathToFileURL } from "node:url";

import { loadProjectEnv } from "../config/env.js";
import { readMontanaCersOutsideSpendingArtifacts } from "../pipeline/montanaFinance/montanaCersArtifactCache.js";
import { acquireMontanaCersOutsideSpendingArtifacts } from "../pipeline/montanaFinance/montanaOutsideSpendingAcquisition.js";
import {
  classifyMontanaOutsideSpendingRows,
  summarizeMontanaOutsideSpendingByCommittee,
  type MontanaIeClassifiedRow,
} from "../pipeline/montanaFinance/montanaOutsideSpendingAggregator.js";
import {
  assertNoUnknownMontanaFinanceFlags,
  parseMontanaFinancePositiveIntegerFlag,
} from "./montanaCandidateFinanceCliArgs.js";

export type ReportMontanaOutsideSpendingScriptOptions = {
  year: number;
  refresh: boolean;
};

export function parseReportMontanaOutsideSpendingScriptArgs(
  args: readonly string[]
): ReportMontanaOutsideSpendingScriptOptions {
  assertNoUnknownMontanaFinanceFlags(args, { booleanFlags: ["--refresh"], valueFlags: ["--year"] });
  const year = parseMontanaFinancePositiveIntegerFlag(args, "--year");
  if (year === undefined) {
    throw new Error("--year is required (e.g. --year 2026)");
  }
  return { year, refresh: args.includes("--refresh") };
}

function dollars(cents: number): number {
  return cents / 100;
}

export function toMontanaOutsideSpendingReport(input: {
  year: number;
  classifiedRows: readonly MontanaIeClassifiedRow[];
}) {
  const totals = new Map<string, { rowCount: number; cents: number }>();
  const resolvedTargets = new Map<
    number,
    { cersCandidateName: string; rowCount: number; supportCents: number; opposeCents: number }
  >();
  for (const entry of input.classifiedRows) {
    const key =
      entry.outcome.kind === "resolved"
        ? `resolved_${entry.outcome.stance}`
        : `${entry.outcome.kind}_${entry.outcome.reason}`;
    const total = totals.get(key) ?? { rowCount: 0, cents: 0 };
    total.rowCount += 1;
    total.cents += entry.row.totalAmtCents;
    totals.set(key, total);
    if (entry.outcome.kind === "resolved") {
      const target = resolvedTargets.get(entry.outcome.cersCandidateId) ?? {
        cersCandidateName: entry.outcome.cersCandidateName,
        rowCount: 0,
        supportCents: 0,
        opposeCents: 0,
      };
      target.rowCount += 1;
      if (entry.outcome.stance === "support") {
        target.supportCents += entry.row.totalAmtCents;
      } else {
        target.opposeCents += entry.row.totalAmtCents;
      }
      resolvedTargets.set(entry.outcome.cersCandidateId, target);
    }
  }
  return {
    type: "montana_outside_spending_report",
    ts: new Date().toISOString(),
    year: input.year,
    totals_by_outcome: Object.fromEntries(
      [...totals.entries()]
        .sort((left, right) => right[1].cents - left[1].cents)
        .map(([key, { rowCount, cents }]) => [key, { row_count: rowCount, amount: dollars(cents) }])
    ),
    resolved_targets: [...resolvedTargets.entries()]
      .sort(
        (left, right) =>
          right[1].supportCents + right[1].opposeCents - (left[1].supportCents + left[1].opposeCents)
      )
      .map(([cersCandidateId, target]) => ({
        cers_candidate_id: cersCandidateId,
        cers_candidate_name: target.cersCandidateName,
        row_count: target.rowCount,
        support_amount: target.supportCents > 0 ? dollars(target.supportCents) : null,
        oppose_amount: target.opposeCents > 0 ? dollars(target.opposeCents) : null,
      })),
    committees: summarizeMontanaOutsideSpendingByCommittee(input.classifiedRows),
  };
}

async function main(): Promise<void> {
  loadProjectEnv();
  const options = parseReportMontanaOutsideSpendingScriptArgs(process.argv.slice(2));
  if (options.refresh) {
    const acquisition = await acquireMontanaCersOutsideSpendingArtifacts({ year: options.year });
    console.error(
      `Montana IE sweep refreshed: ${acquisition.committeeCount} committees, ${acquisition.transactionRowCount} rows`
    );
  }
  const bundle = await readMontanaCersOutsideSpendingArtifacts({ year: options.year });
  const classifiedRows = classifyMontanaOutsideSpendingRows({
    sweep: bundle.sweep,
    registrationRows: bundle.registrationRows,
    electionYear: options.year,
  });
  console.log(JSON.stringify(toMontanaOutsideSpendingReport({ year: options.year, classifiedRows }), null, 2));
}

const entrypoint = process.argv[1] ? pathToFileURL(process.argv[1]).href : null;
if (entrypoint === import.meta.url) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Montana outside-spending report failed:", message);
    process.exitCode = 1;
  });
}
