// Direct-finance aggregation for South Carolina (plan-south-carolina-finance.md
// Phase 3). Totals come from the authoritative report-detail summaries of each
// accepted run's final report; breakdowns come from Contribution/Search rows.
// All arithmetic in integer cents; dollars only at the output boundary (the
// shared writer convention).
//
// Verified source facts this module rests on (backend/docs/
// south-carolina-campaign-finance.md):
// - Report-index rows are submitted reports only, cumulative PER ELECTION RUN,
//   and the index `contributions`/`expenses`/`balance` equal the detail's
//   income Total / expenditure Total / Campaign Funds ending balance
//   cent-exact — kept here as an integrity check.
// - The income Total INCLUDES loans, so the direct total must be built from
//   the detail's typed lines, never from the index row.
// - Search rows are cash + in-kind (Wilson and Evette identities, cent-exact);
//   the candidate's own "Personal Contributions" line may also surface as rows,
//   so reconciliation accepts cash+in-kind and cash+in-kind+personal.
// - isPrimary/isGeneral flags are unreliable on older runs (2008 primary-date
//   reports flagged general); runs are keyed by campaignId, never by flags.

import type {
  SouthCarolinaCandidateReportRow,
  SouthCarolinaContributionSearchRow,
  SouthCarolinaReportDetails,
} from "./southCarolinaEthicsClient.js";

export const SOUTH_CAROLINA_DIRECT_COVERAGE_NOTE =
  "Occupation and contribution-size breakdowns use itemized contributions; filings may aggregate contributions of $100 or less, and a candidate's personal contributions may not be itemized.";

export const SOUTH_CAROLINA_UNKNOWN_OCCUPATION_LABEL = "Unknown";

// Income line types on public report details (probed live 2026-08-27, Evette
// report 430061 carries the full set). Anything else with a nonzero cycle
// total fails closed rather than silently moving the direct total.
const DIRECT_INCOME_TYPES = new Set(["CASH CONTRIBUTIONS", "IN KIND CONTRIBUTIONS", "PERSONAL CONTRIBUTIONS"]);
const EXCLUDED_INCOME_TYPES = new Set(["LOANS", "DEBT SETOFF FUNDS", "ACCOUNT CREDITS"]);
const TOTAL_LINE_TYPE = "TOTAL";

const PLACEHOLDER_OCCUPATION_KEYS = new Set([
  "",
  "N A",
  "NA",
  "NONE",
  "NOT APPLICABLE",
  "NULL",
  "UNKNOWN",
]);

export type SouthCarolinaAcceptedRun = {
  campaignId: number;
  electionDate: string;
  // The run's newest filing period (the index already points at the newest
  // version of each period). Its balance is the run's cash on hand.
  finalReport: SouthCarolinaCandidateReportRow;
  // One final report per election date the run spans, oldest first. When a
  // run crosses the primary/general boundary the source keeps one campaignId
  // but the cycle totals either RESET or CONTINUE there, varying by filer —
  // the aggregator classifies each boundary from the reported balances, so it
  // needs every phase's final, not just the newest. Single-phase runs hold
  // exactly [finalReport].
  phaseFinals: readonly SouthCarolinaCandidateReportRow[];
};

// Newest filing period wins; equal periods resolve to the higher report id
// (the source's newest version of that period).
function laterReport(
  left: SouthCarolinaCandidateReportRow,
  right: SouthCarolinaCandidateReportRow
): SouthCarolinaCandidateReportRow {
  if (right.filingEndDate > left.filingEndDate) return right;
  if (right.filingEndDate < left.filingEndDate) return left;
  return right.reportId > left.reportId ? right : left;
}

export type SouthCarolinaFinanceDirectBreakdown = {
  categoryType: "occupation" | "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type SouthCarolinaDirectFinanceAggregation =
  | { status: "no_filed_reports" }
  | { status: "failed"; diagnostics: string[] }
  | {
      status: "aggregated";
      totalReceipts: number;
      directContributionTotal: number;
      totalDisbursements: number;
      cashOnHand: number;
      directBreakdowns: SouthCarolinaFinanceDirectBreakdown[];
      directCoverageNote: string | null;
      runCount: number;
      includedContributionRowCount: number;
      otherRunContributionRowCount: number;
    };

function normalizeText(value: string | null | undefined): string {
  return (value ?? "").trim().replace(/\s+/g, " ");
}

function keyText(value: string | null | undefined): string {
  return normalizeText(value)
    .toLocaleUpperCase("en-US")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function toCents(amount: number, label: string): number {
  const cents = Math.round(amount * 100);
  if (!Number.isFinite(amount) || Math.abs(amount * 100 - cents) > 1e-6 || !Number.isSafeInteger(cents)) {
    throw new AggregationFailure(`South Carolina finance ${label} is not cent-exact: ${amount}`);
  }
  return cents;
}

class AggregationFailure extends Error {}

// Reports in the linked election's year, grouped into runs by campaignId
// (= officeRunId; one run per election event). Ordering inside a run and the
// final-report choice use the filing-period end — never submission or
// amendment timestamps (the Evette Jul-10/Jul-14 trap).
// acceptedElectionDates (M/D/YYYY, as served) narrows runs to the linked
// office's cycle events — primary/runoff/general dates the sync derives
// upstream. Without it the filter is the election year alone, which can admit
// an unrelated same-year run (a special election for another office); WITH it,
// omitting the primary date silently drops the primary run's money — the sync
// must pass the full statutory trio or nothing.
export function selectSouthCarolinaAcceptedRuns(
  reports: readonly SouthCarolinaCandidateReportRow[],
  electionYear: number,
  acceptedElectionDates?: readonly string[]
): SouthCarolinaAcceptedRun[] {
  if (!Number.isInteger(electionYear) || electionYear < 2000 || electionYear > 2100) {
    throw new Error(`Invalid South Carolina election year: ${electionYear}`);
  }
  const acceptedDates = acceptedElectionDates ? new Set(acceptedElectionDates) : null;
  if (acceptedDates) {
    for (const date of acceptedDates) {
      if (!/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(date) || Number.parseInt(date.slice(-4), 10) !== electionYear) {
        throw new Error(`Invalid South Carolina accepted election date: ${date}`);
      }
    }
  }
  const byRun = new Map<number, SouthCarolinaCandidateReportRow[]>();
  for (const report of reports) {
    if (Number.parseInt(report.electionDate.slice(-4), 10) !== electionYear) {
      continue;
    }
    if (acceptedDates && !acceptedDates.has(report.electionDate)) {
      continue;
    }
    const rows = byRun.get(report.campaignId) ?? [];
    rows.push(report);
    byRun.set(report.campaignId, rows);
  }
  const runs: SouthCarolinaAcceptedRun[] = [];
  for (const [campaignId, rows] of byRun) {
    // Split the run into election-date phases before picking finals: a run
    // that crosses the primary/general boundary may restart its cumulative
    // totals there (filer-dependent), so the aggregator needs each phase's
    // own final to classify and combine.
    const byPhase = new Map<string, SouthCarolinaCandidateReportRow[]>();
    for (const row of rows) {
      const phaseRows = byPhase.get(row.electionDate) ?? [];
      phaseRows.push(row);
      byPhase.set(row.electionDate, phaseRows);
    }
    const phaseFinals = [...byPhase.values()]
      .map((phaseRows) => phaseRows.reduce(laterReport))
      .sort(
        (left, right) =>
          left.filingEndDate.localeCompare(right.filingEndDate) || left.reportId - right.reportId
      );
    const finalReport = phaseFinals[phaseFinals.length - 1]!;
    runs.push({ campaignId, electionDate: finalReport.electionDate, finalReport, phaseFinals });
  }
  return runs.sort(
    (left, right) =>
      left.finalReport.filingEndDate.localeCompare(right.finalReport.filingEndDate) ||
      left.campaignId - right.campaignId
  );
}

// Calendar years the accepted runs' filing periods touch — the
// Contribution/Search year filters a sync must request.
export function southCarolinaContributionYearsForRuns(
  reports: readonly SouthCarolinaCandidateReportRow[],
  electionYear: number,
  acceptedElectionDates?: readonly string[]
): number[] {
  const years = new Set<number>();
  const accepted = new Set(
    selectSouthCarolinaAcceptedRuns(reports, electionYear, acceptedElectionDates).map((run) => run.campaignId)
  );
  for (const report of reports) {
    if (!accepted.has(report.campaignId)) {
      continue;
    }
    const start = Number.parseInt(report.filingStartDate.slice(0, 4), 10);
    const end = Number.parseInt(report.filingEndDate.slice(0, 4), 10);
    for (let year = start; year <= end; year += 1) {
      years.add(year);
    }
  }
  return [...years].sort((a, b) => a - b);
}

type RunCycleTotals = {
  totalReceiptsCents: number;
  directCents: number;
  cashInKindCents: number;
  personalCents: number;
  disbursementsCents: number;
};

// Totals for ONE election-date phase of a run, read off that phase's final
// report. A run's money is the sum of its phases (see phaseFinals).
function cycleTotalsFromDetails(
  campaignId: number,
  phaseFinal: SouthCarolinaCandidateReportRow,
  details: SouthCarolinaReportDetails
): RunCycleTotals {
  let directCents = 0;
  let cashInKindCents = 0;
  let personalCents = 0;
  let totalReceiptsCents: number | null = null;
  for (const line of details.income) {
    const type = keyText(line.type);
    const cents = toCents(line.electionCycleTotal, `run ${campaignId} income "${line.type}"`);
    if (type === TOTAL_LINE_TYPE) {
      totalReceiptsCents = cents;
    } else if (DIRECT_INCOME_TYPES.has(type)) {
      directCents += cents;
      if (type === "PERSONAL CONTRIBUTIONS") {
        personalCents += cents;
      } else {
        cashInKindCents += cents;
      }
    } else if (!EXCLUDED_INCOME_TYPES.has(type)) {
      if (cents !== 0) {
        throw new AggregationFailure(
          `South Carolina finance run ${campaignId} has unrecognized income type "${line.type}" with nonzero cycle total`
        );
      }
    }
  }
  if (totalReceiptsCents === null) {
    throw new AggregationFailure(`South Carolina finance run ${campaignId} detail has no income Total line`);
  }

  let disbursementsCents: number | null = null;
  for (const line of details.expenditures) {
    if (keyText(line.type) === TOTAL_LINE_TYPE) {
      disbursementsCents = toCents(line.electionCycleTotal, `run ${campaignId} expenditure Total`);
    }
  }
  if (disbursementsCents === null) {
    throw new AggregationFailure(`South Carolina finance run ${campaignId} detail has no expenditure Total line`);
  }

  // The report-index row and the detail describe the same report; their run
  // cumulatives are cent-identical in the source (verified live). A mismatch
  // means the wrong detail was fetched or the source contract moved.
  const indexContributionsCents = toCents(phaseFinal.contributions, `run ${campaignId} index contributions`);
  const indexExpensesCents = toCents(phaseFinal.expenses, `run ${campaignId} index expenses`);
  if (indexContributionsCents !== totalReceiptsCents || indexExpensesCents !== disbursementsCents) {
    throw new AggregationFailure(
      `South Carolina finance run ${campaignId} detail totals disagree with report ${phaseFinal.reportId} index row`
    );
  }
  const campaignFundsLines = details.totals.filter((line) => keyText(line.totalType) === "CAMPAIGN FUNDS");
  if (campaignFundsLines.length !== 1) {
    throw new AggregationFailure(
      `South Carolina finance run ${campaignId} detail has ${campaignFundsLines.length} Campaign Funds balance lines`
    );
  }
  const endingBalanceCents = toCents(
    campaignFundsLines[0]!.endingBalance,
    `run ${campaignId} Campaign Funds ending balance`
  );
  if (endingBalanceCents !== toCents(phaseFinal.balance, `run ${campaignId} index balance`)) {
    throw new AggregationFailure(
      `South Carolina finance run ${campaignId} Campaign Funds ending balance disagrees with report ${phaseFinal.reportId} index row`
    );
  }

  return { totalReceiptsCents, directCents, cashInKindCents, personalCents, disbursementsCents };
}

type Aggregate = {
  categoryType: SouthCarolinaFinanceDirectBreakdown["categoryType"];
  categoryName: string;
  amountCents: number;
  contributors: Set<string>;
};

function sizeBucket(amountCents: number): string {
  if (amountCents < 10_000) return "$1-$99";
  if (amountCents < 25_000) return "$100-$249";
  if (amountCents < 50_000) return "$250-$499";
  if (amountCents < 100_000) return "$500-$999";
  if (amountCents < 500_000) return "$1,000-$4,999";
  return "$5,000+";
}

function occupationName(value: string | null): string {
  const filed = normalizeText(value);
  return PLACEHOLDER_OCCUPATION_KEYS.has(keyText(filed)) ? SOUTH_CAROLINA_UNKNOWN_OCCUPATION_LABEL : filed;
}

function addAggregate(
  aggregates: Map<string, Aggregate>,
  categoryType: Aggregate["categoryType"],
  categoryName: string,
  amountCents: number,
  contributor: string
): void {
  const key = `${categoryType}\u0000${keyText(categoryName)}`;
  const aggregate = aggregates.get(key);
  if (aggregate) {
    aggregate.amountCents += amountCents;
    aggregate.contributors.add(contributor);
  } else {
    aggregates.set(key, { categoryType, categoryName, amountCents, contributors: new Set([contributor]) });
  }
}

export function aggregateSouthCarolinaDirectFinance(input: {
  candidateFilerId: number;
  electionYear: number;
  reports: readonly SouthCarolinaCandidateReportRow[];
  // One entry per accepted run's finalReport.reportId (selectSouthCarolinaAcceptedRuns).
  detailsByReportId: ReadonlyMap<number, SouthCarolinaReportDetails>;
  contributionRows: readonly SouthCarolinaContributionSearchRow[];
  acceptedElectionDates?: readonly string[];
  sourceUrl?: string | null;
  maxOccupationBreakdowns?: number;
}): SouthCarolinaDirectFinanceAggregation {
  const maxOccupationBreakdowns = input.maxOccupationBreakdowns ?? 50;
  if (!Number.isSafeInteger(maxOccupationBreakdowns) || maxOccupationBreakdowns <= 0) {
    throw new Error(`Invalid South Carolina occupation breakdown limit: ${input.maxOccupationBreakdowns}`);
  }
  const runs = selectSouthCarolinaAcceptedRuns(input.reports, input.electionYear, input.acceptedElectionDates);
  if (runs.length === 0) {
    return { status: "no_filed_reports" };
  }

  try {
    let totalReceiptsCents = 0;
    let directCents = 0;
    let disbursementsCents = 0;
    const runTargets = new Map<number, { cashInKindCents: number; personalCents: number }>();
    for (const run of runs) {
      // A run spanning more than one election date behaves TWO ways in the
      // source, varying by filer (both live-proven 2026-08-29):
      // - reset: the later phase's cycle totals restart at the boundary
      //   (Mitchell 57316: general detail period == cycle == $14,450), so the
      //   run's money is the SUM of its phases;
      // - continuation: the later phase's cycle totals still include the
      //   earlier phase (Forrest 11869: general detail period $19,993.10,
      //   cycle $39,019.12 = primary $19,026.02 + period), so the later
      //   phase SUBSUMES the earlier one and summing double-counts.
      // Balances carry across the boundary in both models, so the index rows
      // decide which one holds: exactly one model reproduces the later
      // phase's balance from the earlier one. Ambiguous or unexplained
      // boundaries fail closed.
      const phases = run.phaseFinals.map((phaseFinal) => {
        const details = input.detailsByReportId.get(phaseFinal.reportId);
        if (!details) {
          throw new Error(`Missing South Carolina report details for report ${phaseFinal.reportId}`);
        }
        return {
          balanceCents: toCents(phaseFinal.balance, `run ${run.campaignId} report ${phaseFinal.reportId} balance`),
          totals: cycleTotalsFromDetails(run.campaignId, phaseFinal, details),
        };
      });
      // Closed segments (ended by a reset) accumulate; `current` is the phase
      // whose cycle totals cover everything since the last reset.
      let closed: RunCycleTotals = {
        totalReceiptsCents: 0,
        directCents: 0,
        cashInKindCents: 0,
        personalCents: 0,
        disbursementsCents: 0,
      };
      let current = phases[0]!;
      for (const next of phases.slice(1)) {
        const resetHolds =
          current.balanceCents + next.totals.totalReceiptsCents - next.totals.disbursementsCents ===
          next.balanceCents;
        const continuationHolds =
          current.balanceCents +
            (next.totals.totalReceiptsCents - current.totals.totalReceiptsCents) -
            (next.totals.disbursementsCents - current.totals.disbursementsCents) ===
          next.balanceCents;
        if (resetHolds && continuationHolds) {
          // Both hold exactly when the earlier phase's receipts equal its
          // spending. All-zero is degenerate — the two models give identical
          // totals — so treat it as continuation; nonzero is truly ambiguous.
          if (current.totals.totalReceiptsCents !== 0 || current.totals.disbursementsCents !== 0) {
            throw new AggregationFailure(
              `South Carolina finance run ${run.campaignId} phase boundary is ambiguous: earlier-phase receipts equal spending, so balances cannot distinguish cumulative reset from continuation`
            );
          }
          current = next;
          continue;
        }
        if (resetHolds) {
          closed = {
            totalReceiptsCents: closed.totalReceiptsCents + current.totals.totalReceiptsCents,
            directCents: closed.directCents + current.totals.directCents,
            cashInKindCents: closed.cashInKindCents + current.totals.cashInKindCents,
            personalCents: closed.personalCents + current.totals.personalCents,
            disbursementsCents: closed.disbursementsCents + current.totals.disbursementsCents,
          };
          current = next;
        } else if (continuationHolds) {
          current = next;
        } else {
          throw new AggregationFailure(
            `South Carolina finance run ${run.campaignId} phase boundary matches neither cumulative reset nor continuation against the reported balances`
          );
        }
      }
      totalReceiptsCents += closed.totalReceiptsCents + current.totals.totalReceiptsCents;
      directCents += closed.directCents + current.totals.directCents;
      disbursementsCents += closed.disbursementsCents + current.totals.disbursementsCents;
      runTargets.set(run.campaignId, {
        cashInKindCents: closed.cashInKindCents + current.totals.cashInKindCents,
        personalCents: closed.personalCents + current.totals.personalCents,
      });
    }

    // Cash on hand: balance of the chronologically latest report across runs
    // (balances carry over run boundaries).
    const latestRun = runs[runs.length - 1]!;
    const cashOnHandCents = toCents(latestRun.finalReport.balance, "latest report balance");

    // Itemized rows: scope to the linked filer and the accepted runs; rows for
    // other runs (older cycles in the requested calendar years) are expected.
    const acceptedRunIds = new Set(runs.map((run) => run.campaignId));
    const seenContributionIds = new Set<number>();
    const itemizedCentsByRun = new Map<number, number>();
    const aggregates = new Map<string, Aggregate>();
    let includedContributionRowCount = 0;
    let otherRunContributionRowCount = 0;
    for (const row of input.contributionRows) {
      if (row.candidateId !== input.candidateFilerId || !acceptedRunIds.has(row.officeRunId)) {
        otherRunContributionRowCount += 1;
        continue;
      }
      if (seenContributionIds.has(row.contributionId)) {
        throw new AggregationFailure(
          `South Carolina finance duplicate contributionId ${row.contributionId} in run ${row.officeRunId}`
        );
      }
      seenContributionIds.add(row.contributionId);
      const amountCents = toCents(row.amount, `contribution ${row.contributionId} amount`);
      if (amountCents < 0) {
        throw new AggregationFailure(
          `South Carolina finance negative contribution ${row.contributionId}: ${row.amount}`
        );
      }
      itemizedCentsByRun.set(row.officeRunId, (itemizedCentsByRun.get(row.officeRunId) ?? 0) + amountCents);
      includedContributionRowCount += 1;
      if (amountCents === 0) {
        continue;
      }
      const contributor = keyText(row.contributorName);
      addAggregate(aggregates, "contribution_size", sizeBucket(amountCents), amountCents, contributor);
      if (row.group === "No") {
        addAggregate(aggregates, "occupation", occupationName(row.contributorOccupation), amountCents, contributor);
      }
    }

    // Per-run reconciliation against the authoritative summary. Search rows
    // are cash + in-kind, and the candidate's own personal contributions may
    // appear as rows too — fully, partially, or not at all. Coverage is full
    // only when the itemized sum reaches the whole direct total; anything
    // short of that leaves money out of the breakdowns and gets the note.
    // A sum above the direct total is unexplained and fails closed.
    let partialCoverage = false;
    for (const run of runs) {
      const target = runTargets.get(run.campaignId)!;
      const fullDirectCents = target.cashInKindCents + target.personalCents;
      const itemized = itemizedCentsByRun.get(run.campaignId) ?? 0;
      if (itemized > fullDirectCents) {
        throw new AggregationFailure(
          `South Carolina finance run ${run.campaignId} itemized sum ${itemized} exceeds summary contributions ${fullDirectCents}`
        );
      }
      if (itemized < fullDirectCents) {
        partialCoverage = true;
      }
    }

    const byType = new Map<Aggregate["categoryType"], Aggregate[]>();
    for (const aggregate of aggregates.values()) {
      const rows = byType.get(aggregate.categoryType) ?? [];
      rows.push(aggregate);
      byType.set(aggregate.categoryType, rows);
    }
    const directBreakdowns: SouthCarolinaFinanceDirectBreakdown[] = [];
    for (const type of ["occupation", "contribution_size"] as const) {
      const limit = type === "occupation" ? maxOccupationBreakdowns : Number.POSITIVE_INFINITY;
      for (const aggregate of (byType.get(type) ?? [])
        .sort((a, b) => b.amountCents - a.amountCents || a.categoryName.localeCompare(b.categoryName))
        .slice(0, limit)) {
        directBreakdowns.push({
          categoryType: type,
          categoryName: aggregate.categoryName,
          amount: aggregate.amountCents / 100,
          contributorCount: aggregate.contributors.size,
          sourceUrl: input.sourceUrl ?? null,
        });
      }
    }

    return {
      status: "aggregated",
      totalReceipts: totalReceiptsCents / 100,
      directContributionTotal: directCents / 100,
      totalDisbursements: disbursementsCents / 100,
      cashOnHand: cashOnHandCents / 100,
      directBreakdowns,
      directCoverageNote: partialCoverage ? SOUTH_CAROLINA_DIRECT_COVERAGE_NOTE : null,
      runCount: runs.length,
      includedContributionRowCount,
      otherRunContributionRowCount,
    };
  } catch (error) {
    if (error instanceof AggregationFailure) {
      return { status: "failed", diagnostics: [error.message] };
    }
    throw error;
  }
}
