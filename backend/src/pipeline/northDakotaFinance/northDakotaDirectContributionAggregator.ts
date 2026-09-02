// North Dakota direct-finance aggregation (plan hard fact 1, Phase 2).
//
// Source: the daily Contributions bulk CSV — every current-version row the
// committee filed (gate 5: the file holds current versions only; the sync
// re-proves that against the API every run). Category and contributor-type
// vocabularies were pinned from the live 2025-2026 files (2026-09-01); a
// value outside them is counted and surfaced, never guessed — the sync
// fails closed on any.
//
// Money model:
// - total_receipts = every recognized contribution-file row in the window,
//   lump rows included. This is the figure the portal's own charts sum to.
// - direct_contribution_total (the card's "Raised") = Monetary + In-Kind
//   from donors — individuals, businesses, PACs, party committees — plus
//   the unitemized lump rows ("Total - $200 or less" / "Total - $100 or
//   less": aggregated small contributions the statute lets filers report
//   without itemizing). The candidate's own money (Candidate / Self rows)
//   and "Reimbursement of Expenditure" rows (money coming back from a
//   payee, not a donation) are excluded. Loans have no bulk file and never
//   enter; there are no Return rows in this generator's ND output.
// - Contribution-size buckets = positive itemized Monetary rows from
//   individuals. Lump rows carry no contributor type and mix every donor
//   class, so they cannot be bucketed and are disclosed in the note instead.
// - Spending and cash: not computed here. Candidate expenditures exist only
//   on the year-end statement (hard fact 2); totalDisbursements and
//   cashOnHand stay NULL in this phase.

import type { NorthDakotaContributionCsvRow } from "./northDakotaCfrsCsv.js";
import { isNorthDakotaDateInWindow, type NorthDakotaCycleWindow } from "./northDakotaReportingCycleWindows.js";

const CATEGORY_MONETARY = "Monetary";
const CATEGORY_IN_KIND = "In-Kind";
const CATEGORY_REIMBURSEMENT = "Reimbursement of Expenditure";
const CATEGORY_LUMP_200 = "Total - $200 or less";
const CATEGORY_LUMP_100 = "Total - $100 or less";

/** Category vocabulary of the contributions bulk file (verified live). */
export const NORTH_DAKOTA_CONTRIBUTION_FILE_CATEGORIES: ReadonlySet<string> = new Set([
  CATEGORY_MONETARY,
  CATEGORY_IN_KIND,
  CATEGORY_REIMBURSEMENT,
  CATEGORY_LUMP_200,
  CATEGORY_LUMP_100,
]);

const LUMP_CATEGORIES: ReadonlySet<string> = new Set([CATEGORY_LUMP_200, CATEGORY_LUMP_100]);

const CONTRIBUTOR_TYPE_INDIVIDUAL = "Individual";
/** Donor classes on Monetary / In-Kind rows (verified live: candidate committees 2025-2026). */
const DONOR_CONTRIBUTOR_TYPES: ReadonlySet<string> = new Set([
  CONTRIBUTOR_TYPE_INDIVIDUAL,
  "Business or Organization",
  "Committee/PAC",
  "Party Committee",
]);
/** The candidate's own money, under either label the portal uses. */
const SELF_CONTRIBUTOR_TYPES: ReadonlySet<string> = new Set(["Candidate", "Self"]);

export const NORTH_DAKOTA_DIRECT_COVERAGE_NOTE =
  "Raised counts contributions from individuals, businesses, PACs, and party committees, including small contributions of $200 or less that North Dakota reports as lump sums; the candidate's own money and reimbursements are not included, and the size chart covers itemized individual contributions only. North Dakota candidates report spending only on the year-end statement, so spending is unavailable until then rather than zero.";

export type NorthDakotaFinanceDirectBreakdown = {
  categoryType: "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
};

export type NorthDakotaDirectFinanceAggregationResult = {
  /** Sum of every recognized contribution-file row in the window. */
  totalReceiptsCents: number;
  /** Donor money only (see the money model above). */
  directContributionCents: number;
  /** Portion of directContributionCents that came in as unitemized lump rows. */
  unitemizedCents: number;
  selfFundingCents: number;
  reimbursementCents: number;
  contributionRowCount: number;
  lumpRowCount: number;
  unrecognizedContributionCategories: string[];
  unrecognizedContributorTypes: string[];
  breakdowns: NorthDakotaFinanceDirectBreakdown[];
};

/** Delaware bucket edges — the fleet's contribution-size vocabulary. */
function sizeBucket(amountCents: number): string {
  if (amountCents < 10_000) return "$1-$99";
  if (amountCents < 25_000) return "$100-$249";
  if (amountCents < 50_000) return "$250-$499";
  if (amountCents < 100_000) return "$500-$999";
  if (amountCents < 500_000) return "$1,000-$4,999";
  return "$5,000+";
}

type Totals = Map<string, { cents: number; contributors: Set<string> }>;

function add(totals: Totals, name: string, cents: number, contributorKey: string): void {
  const entry = totals.get(name) ?? { cents: 0, contributors: new Set<string>() };
  entry.cents += cents;
  entry.contributors.add(contributorKey);
  totals.set(name, entry);
}

export function aggregateNorthDakotaDirectFinance(input: {
  entityId: string;
  window: Pick<NorthDakotaCycleWindow, "windowStart" | "windowEnd">;
  contributionRows: readonly NorthDakotaContributionCsvRow[];
}): NorthDakotaDirectFinanceAggregationResult {
  let totalReceiptsCents = 0;
  let directContributionCents = 0;
  let unitemizedCents = 0;
  let selfFundingCents = 0;
  let reimbursementCents = 0;
  let contributionRowCount = 0;
  let lumpRowCount = 0;
  const unrecognizedContributionCategories = new Set<string>();
  const unrecognizedContributorTypes = new Set<string>();
  const bucketTotals: Totals = new Map();

  for (const row of input.contributionRows) {
    if (row.registrantId !== input.entityId || !isNorthDakotaDateInWindow(row.transactionDate, input.window)) {
      continue;
    }
    contributionRowCount += 1;
    const cents = row.amountCents;
    const category = row.transactionCategory;
    const contributorType = row.contributorType;

    if (LUMP_CATEGORIES.has(category)) {
      lumpRowCount += 1;
      totalReceiptsCents += cents;
      directContributionCents += cents;
      unitemizedCents += cents;
      continue;
    }
    if (category === CATEGORY_REIMBURSEMENT) {
      totalReceiptsCents += cents;
      reimbursementCents += cents;
      continue;
    }
    if (category !== CATEGORY_MONETARY && category !== CATEGORY_IN_KIND) {
      unrecognizedContributionCategories.add(category);
      continue;
    }
    const isDonor = DONOR_CONTRIBUTOR_TYPES.has(contributorType);
    const isSelf = SELF_CONTRIBUTOR_TYPES.has(contributorType);
    if (!isDonor && !isSelf) {
      unrecognizedContributorTypes.add(contributorType);
      continue;
    }
    totalReceiptsCents += cents;
    if (isSelf) {
      selfFundingCents += cents;
      continue;
    }
    directContributionCents += cents;
    if (category === CATEGORY_MONETARY && contributorType === CONTRIBUTOR_TYPE_INDIVIDUAL && cents > 0) {
      add(bucketTotals, sizeBucket(cents), cents, row.contributorName.toUpperCase());
    }
  }

  const breakdowns: NorthDakotaFinanceDirectBreakdown[] = [...bucketTotals.entries()]
    .sort((left, right) => right[1].cents - left[1].cents || left[0].localeCompare(right[0]))
    .map(([name, entry]) => ({
      categoryType: "contribution_size" as const,
      categoryName: name,
      amount: entry.cents / 100,
      contributorCount: entry.contributors.size,
    }));

  return {
    totalReceiptsCents,
    directContributionCents,
    unitemizedCents,
    selfFundingCents,
    reimbursementCents,
    contributionRowCount,
    lumpRowCount,
    unrecognizedContributionCategories: [...unrecognizedContributionCategories].sort(),
    unrecognizedContributorTypes: [...unrecognizedContributorTypes].sort(),
    breakdowns,
  };
}
