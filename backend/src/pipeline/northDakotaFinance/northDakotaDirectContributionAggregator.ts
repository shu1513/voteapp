// North Dakota direct-finance aggregation (plan hard facts 1 and 3;
// Phases 2 and 3).
//
// Sources: the daily Contributions bulk CSV is the money authority — every
// current-version row the committee filed (gate 5: the file holds current
// versions only; the sync re-proves that against the API every run). The
// API rows are the attribute authority for occupation, the one field the
// CSV lacks (the sync proves they are the same rows as the file before
// anything here runs). Category and contributor-type vocabularies were
// pinned from the live 2025-2026 files (2026-09-01); a value outside them
// is counted and surfaced, never guessed — the sync fails closed on any.
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
// - Occupation breakdown = the filed `employerOccupation` on positive
//   Monetary / In-Kind API rows from individuals, the state's label
//   published verbatim after whitespace normalization; blank and "Unknown"
//   excluded, nothing inferred. NDCC 16.1-08.1-02.3 requires occupation
//   only once an individual's period aggregate reaches $5,000 (and never
//   from judicial candidates), so the rows publish only behind the plan's
//   display gate (hard fact 3): occupation-bearing individual dollars
//   >= 20% of positive itemized individual dollars AND >= 3
//   occupation-bearing donors. Below the gate the totals still publish and
//   the occupation rows are simply absent (component isolation).
// - Spending and cash: not computed here. Candidate expenditures exist only
//   on the year-end statement (hard fact 2); totalDisbursements and
//   cashOnHand stay NULL in this phase.

import type { NorthDakotaTransactionRow } from "./northDakotaCfrsClient.js";
import type { NorthDakotaContributionCsvRow } from "./northDakotaCfrsCsv.js";
import { apiAmountToCents } from "./northDakotaPhaseZero.js";
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
/** Donor-money categories, as the API spells them (identical to the CSV). */
const DONATION_CATEGORIES: ReadonlySet<string> = new Set([CATEGORY_MONETARY, CATEGORY_IN_KIND]);

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

/** A filed placeholder, not an occupation (3 rows live 2026-09-01). */
const EXCLUDED_OCCUPATION_LABELS: ReadonlySet<string> = new Set(["unknown"]);
/** Plan hard fact 3 display gate: >= 1/5 of individual dollars AND >= 3 donors carry an occupation. */
export const NORTH_DAKOTA_OCCUPATION_DISPLAY_MIN_DONORS = 3;
const OCCUPATION_DISPLAY_SHARE_DIVISOR = 5;
const MAX_OCCUPATION_BREAKDOWNS = 50;

export const NORTH_DAKOTA_DIRECT_COVERAGE_NOTE =
  "Raised counts contributions from individuals, businesses, PACs, and party committees, including small contributions of $200 or less that North Dakota reports as lump sums; the candidate's own money and reimbursements are not included, and the size chart covers itemized individual contributions only. North Dakota requires a donor's occupation only when an individual gives $5,000 or more in a reporting period, and never for judicial candidates, so the occupation chart shows filed occupations only and appears only when they cover at least 20% of itemized individual money. North Dakota candidates report spending only on the year-end statement, so spending is unavailable until then rather than zero.";

export type NorthDakotaFinanceDirectBreakdown = {
  categoryType: "occupation" | "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
};

/** Occupation coverage behind the display gate (hard fact 3); diagnostics, not money. */
export type NorthDakotaOccupationCoverage = {
  /** Positive itemized individual dollars in the window (API Monetary / In-Kind rows). */
  individualCents: number;
  /** Portion of individualCents whose rows carry a filed occupation. */
  occupationCents: number;
  donorCount: number;
  occupationDonorCount: number;
  displayGatePassed: boolean;
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
  occupation: NorthDakotaOccupationCoverage;
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

function sorted(totals: Totals): Array<[string, { cents: number; contributors: Set<string> }]> {
  return [...totals.entries()].sort((left, right) => right[1].cents - left[1].cents || left[0].localeCompare(right[0]));
}

/** Verbatim state label, whitespace-normalized; null for blank / "Unknown". */
export function normalizeNorthDakotaOccupationLabel(value: string | null): string | null {
  const label = (value ?? "").replace(/\s+/g, " ").trim();
  if (label === "" || EXCLUDED_OCCUPATION_LABELS.has(label.toLowerCase())) {
    return null;
  }
  return label;
}

/** Donor identity on API rows: the portal's contributor id, else the name (Phase 0A's counterparty key). */
function apiDonorKey(row: NorthDakotaTransactionRow): string {
  if (row.contributorPayeeID !== null) return `id:${row.contributorPayeeID}`;
  const name = (row.contributorPayeeName ?? "").replace(/\s+/g, " ").trim().toUpperCase();
  return name === "" ? `#${row.transactionID}` : `name:${name}`;
}

export function aggregateNorthDakotaDirectFinance(input: {
  entityId: string;
  window: Pick<NorthDakotaCycleWindow, "windowStart" | "windowEnd">;
  contributionRows: readonly NorthDakotaContributionCsvRow[];
  /** The same window years' API harvest (reconciled row-for-row against the CSV by the sync). */
  apiRows: readonly NorthDakotaTransactionRow[];
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

  // Occupation (hard fact 3): API rows only, individuals only, filed
  // labels only. Per-donor identity keeps the gate's donor count honest
  // when one person gives several times.
  const occupationTotals: Totals = new Map();
  const donors = new Set<string>();
  const occupationDonors = new Set<string>();
  let individualCents = 0;
  let occupationCents = 0;
  for (const row of input.apiRows) {
    if (
      row.entityID !== input.entityId ||
      row.entityTypeDesc !== CONTRIBUTOR_TYPE_INDIVIDUAL ||
      row.transactionCategoryDesc === null ||
      !DONATION_CATEGORIES.has(row.transactionCategoryDesc) ||
      !isNorthDakotaDateInWindow(row.transactionDate, input.window)
    ) {
      continue;
    }
    const cents = apiAmountToCents(row.transactionAmount);
    if (cents <= 0) {
      continue;
    }
    const donorKey = apiDonorKey(row);
    individualCents += cents;
    donors.add(donorKey);
    const occupation = normalizeNorthDakotaOccupationLabel(row.employerOccupation);
    if (occupation === null) {
      continue;
    }
    occupationCents += cents;
    occupationDonors.add(donorKey);
    add(occupationTotals, occupation, cents, donorKey);
  }
  const displayGatePassed =
    individualCents > 0 &&
    occupationCents * OCCUPATION_DISPLAY_SHARE_DIVISOR >= individualCents &&
    occupationDonors.size >= NORTH_DAKOTA_OCCUPATION_DISPLAY_MIN_DONORS;

  const breakdowns: NorthDakotaFinanceDirectBreakdown[] = [];
  const push = (categoryType: NorthDakotaFinanceDirectBreakdown["categoryType"], totals: Totals, limit?: number) => {
    for (const [name, entry] of sorted(totals).slice(0, limit)) {
      breakdowns.push({ categoryType, categoryName: name, amount: entry.cents / 100, contributorCount: entry.contributors.size });
    }
  };
  if (displayGatePassed) {
    push("occupation", occupationTotals, MAX_OCCUPATION_BREAKDOWNS);
  }
  push("contribution_size", bucketTotals);

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
    occupation: {
      individualCents,
      occupationCents,
      donorCount: donors.size,
      occupationDonorCount: occupationDonors.size,
      displayGatePassed,
    },
    breakdowns,
  };
}
