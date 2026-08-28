import { classifyFinanceLabel } from "../finance/financeLabelClassifier.js";
import type { NevadaContributionCsvRow } from "./nevadaAuroraCsv.js";

// Aggregates one linked candidate's itemized AURORA contribution rows into
// contribution-size buckets and organization-donor industry breakdowns.
// Nevada collects no donor occupation or employer; the industry chart covers
// identifiable organization donors only (classifier labelType "donor" —
// person names classify to no industry and stay in totals and size buckets).

export type NevadaDirectContributionAggregationInput = {
  filerKey: string;
  /** ISO period bounds, inclusive (the cycle window's report periods). */
  periodStart: string;
  periodEnd: string;
  /** Statewide rows; the aggregator filters to the filer and window itself. */
  contributionRows: readonly NevadaContributionCsvRow[];
  /**
   * Report years accepted for in-window rows. The filer display name is the
   * only CSV join key (AURORA has no stable public filer ID), so an in-window
   * row whose report name cites only years outside this set is hard evidence
   * of a same-name collision with a different filer's cycle; callers should
   * treat a nonzero foreignReportYearRowCount as fatal. Rows whose report
   * names carry no year (filer-typed free text) give no signal and pass.
   */
  allowedReportYears?: readonly number[];
  sourceUrl?: string | null;
};

export type NevadaFinanceDirectBreakdown = {
  categoryType: "industry" | "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: null;
  sourceUrl: string | null;
};

export type NevadaDirectContributionAggregationResult = {
  directContributionTotalCents: number;
  directBreakdowns: NevadaFinanceDirectBreakdown[];
  directContributionRowCount: number;
  legalDefenseFundRowCount: number;
  outOfWindowRowCount: number;
  /** In-window rows whose report names cite only years outside allowedReportYears. */
  foreignReportYearRowCount: number;
  /** In-window Written Commitment rows (excluded from totals and breakdowns). */
  writtenCommitmentRowCount: number;
};

type Aggregate = {
  categoryType: NevadaFinanceDirectBreakdown["categoryType"];
  categoryName: string;
  amountCents: number;
};

function contributionSizeBucket(amountCents: number): string {
  if (amountCents < 100) return "$0.01-$0.99";
  if (amountCents < 100 * 100) return "$1-$99";
  if (amountCents < 250 * 100) return "$100-$249";
  if (amountCents < 500 * 100) return "$250-$499";
  if (amountCents < 1_000 * 100) return "$500-$999";
  if (amountCents < 5_000 * 100) return "$1,000-$4,999";
  return "$5,000+";
}

function addAggregate(
  aggregates: Map<string, Aggregate>,
  categoryType: Aggregate["categoryType"],
  categoryName: string,
  amountCents: number
): void {
  const key = `${categoryType}\u0000${categoryName}`;
  const existing = aggregates.get(key);
  if (existing) {
    existing.amountCents += amountCents;
    return;
  }
  aggregates.set(key, { categoryType, categoryName, amountCents });
}

function toDirectBreakdowns(
  aggregates: Iterable<Aggregate>,
  sourceUrl: string | null
): NevadaFinanceDirectBreakdown[] {
  const categoryOrder: Aggregate["categoryType"][] = ["industry", "contribution_size"];
  const values = [...aggregates];
  const result: NevadaFinanceDirectBreakdown[] = [];
  for (const categoryType of categoryOrder) {
    for (const aggregate of values
      .filter((value) => value.categoryType === categoryType && value.amountCents > 0)
      .sort(
        (left, right) =>
          right.amountCents - left.amountCents || left.categoryName.localeCompare(right.categoryName)
      )) {
      result.push({
        categoryType,
        categoryName: aggregate.categoryName,
        amount: aggregate.amountCents / 100,
        contributorCount: null,
        sourceUrl,
      });
    }
  }
  return result;
}

export function aggregateNevadaDirectContributions(
  input: NevadaDirectContributionAggregationInput
): NevadaDirectContributionAggregationResult {
  if (!input.filerKey || input.filerKey !== input.filerKey.trim()) {
    throw new Error(`Invalid Nevada filer key ${JSON.stringify(input.filerKey)}`);
  }
  if (input.periodStart > input.periodEnd) {
    throw new Error(`Invalid Nevada aggregation window ${input.periodStart}..${input.periodEnd}`);
  }
  const sourceUrl = input.sourceUrl ?? null;
  const allowedReportYears =
    input.allowedReportYears === undefined ? null : new Set(input.allowedReportYears);
  const aggregates = new Map<string, Aggregate>();
  let directContributionTotalCents = 0;
  let directContributionRowCount = 0;
  let legalDefenseFundRowCount = 0;
  let outOfWindowRowCount = 0;
  let foreignReportYearRowCount = 0;
  let writtenCommitmentRowCount = 0;

  for (const row of input.contributionRows) {
    if (row.filerKey !== input.filerKey) continue;
    if (row.isLegalDefenseFund) {
      legalDefenseFundRowCount += 1;
      continue;
    }
    if (row.date < input.periodStart || row.date > input.periodEnd) {
      outOfWindowRowCount += 1;
      continue;
    }
    if (row.transactionType.includes("Written Commitment")) {
      // Promised-but-unreceived money: reported on its own summary lines, so
      // it sits outside the lines 1+5(+7) reconciliation window and is not a
      // received gift for the breakdowns.
      writtenCommitmentRowCount += 1;
      continue;
    }
    if (allowedReportYears) {
      const yearTokens = [...row.reportName.matchAll(/\b(19|20)\d{2}\b/g)].map((m) => Number(m[0]));
      if (yearTokens.length > 0 && !yearTokens.some((y) => allowedReportYears.has(y))) {
        // Still aggregated: callers treat any foreign row as fatal, so partial
        // exclusion would only hide the collision from the reconciliation gate.
        foreignReportYearRowCount += 1;
      }
    }
    directContributionRowCount += 1;
    directContributionTotalCents += row.amountCents;
    // Reversal rows (parenthesized negatives) are not gifts, so they stay out
    // of the size buckets — but they DO net against the donor's industry:
    // a +$100k gift plus its -$100k rejection must not chart as $100k of
    // industry support. Aggregates that net to <= 0 are dropped on output.
    if (row.amountCents > 0) {
      addAggregate(aggregates, "contribution_size", contributionSizeBucket(row.amountCents), row.amountCents);
    }
    if (row.amountCents !== 0) {
      const industry = classifyFinanceLabel({
        rawLabel: row.contributorName,
        labelType: "donor",
      }).industrySlug;
      if (industry !== null) {
        addAggregate(aggregates, "industry", industry, row.amountCents);
      }
    }
  }

  return {
    directContributionTotalCents,
    directBreakdowns: toDirectBreakdowns(aggregates.values(), sourceUrl),
    directContributionRowCount,
    legalDefenseFundRowCount,
    outOfWindowRowCount,
    foreignReportYearRowCount,
    writtenCommitmentRowCount,
  };
}
