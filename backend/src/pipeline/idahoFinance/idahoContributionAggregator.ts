// Idaho direct money (docs/plans/idaho-finance.md, Phase 2a). Pure: takes one
// grid registration plus the contribution-search rows for that filer name and
// returns the summary and direct breakdowns the writer stores.
//
// Headline totals are the grid's official per-registration figures
// (totalRaised / totalSpent / balanceOfFunds) — never recomputed from rows,
// because returned contributions are subtracted by the state and are not
// served by the search. Rows feed only the breakdowns:
// - contribution_size: itemized rows (ITMY, INKIND) in the shared size
//   buckets; unitemized rows (NITMY, ANYMS) as one lump;
// - contributor_source_type: every direct row by the filer-declared source
//   code (Idaho collects no occupation or employer, so there is no
//   occupation or industry chart).
// ITR (interest) counts toward the grid total but is not a contribution.
//
// Row coverage: the row sum is compared with the grid total and reported,
// never enforced here. Live 2026-09-02: 8 of 9 probed registrations
// reconcile to the cent; one (Stegner) is missing an entire filed monthly
// report from the search, so breakdowns can be incomplete while the grid
// total is right. The sync decides what to write from `rowCoverage`.

import {
  idahoRegistrationProfileUrl,
  normalizeIdahoRegistrationGuid,
  type IdahoCandidateRegistrationRow,
  type IdahoContributionRow,
} from "./idahoCfsClient.js";
import type { IdahoFinanceDirectCategoryType } from "./idahoFinanceWriter.js";
import { selectIdahoRegistrationContributions } from "./idahoPhaseZero.js";

// Official Sunshine subtype codes seen live (findings doc). Anything else
// fails closed so a new code is noticed instead of silently miscounted.
export const IDAHO_ITEMIZED_CONTRIBUTION_SUBTYPE_CODES: ReadonlySet<string> = new Set(["ITMY", "INKIND"]);
export const IDAHO_UNITEMIZED_CONTRIBUTION_SUBTYPE_CODES: ReadonlySet<string> = new Set(["NITMY", "ANYMS"]);
const IDAHO_NON_DIRECT_RECEIPT_SUBTYPE_CODES: ReadonlySet<string> = new Set(["ITR"]);

// Label without digits so the shared size-bucket sort keeps it last.
export const IDAHO_UNITEMIZED_SIZE_BUCKET = "Unitemized small contributions";

export type IdahoContributorSourceType =
  | "individuals"
  | "business_nonprofit_entities"
  | "pac_independent"
  | "party_committee"
  | "candidate_self"
  | "other";

// transactionSourceTypeCode -> the category names New Mexico, Nebraska, and
// Vermont already store, so one UI label set covers every state.
const IDAHO_CONTRIBUTOR_SOURCE_TYPES: Readonly<Record<string, IdahoContributorSourceType>> = {
  TIND: "individuals",
  TBSN: "business_nonprofit_entities",
  TPAC: "pac_independent",
  // Party central committees ("Boise County Republican Central Committee").
  TCENC: "party_committee",
  TCAN: "candidate_self",
  TSELF: "candidate_self",
};

export function mapIdahoContributorSourceType(code: string | null): IdahoContributorSourceType {
  if (code === null) return "other";
  return IDAHO_CONTRIBUTOR_SOURCE_TYPES[code.trim().toUpperCase()] ?? "other";
}

export type IdahoContributionAggregationInput = {
  registration: IdahoCandidateRegistrationRow;
  /** Contribution-search rows for the filer name; other registrations' rows are ignored. */
  contributionRows: readonly IdahoContributionRow[];
  /** Defaults to the registration's public profile page. */
  sourceUrl?: string | null;
};

export type IdahoDirectFinanceSummary = {
  totalReceipts: number;
  directContributionTotal: number;
  totalDisbursements: number;
  /** Signed; the grid reports negative balances for indebted campaigns. */
  cashOnHand: number;
  sourceUrl: string;
};

export type IdahoFinanceDirectBreakdown = {
  categoryType: IdahoFinanceDirectCategoryType;
  categoryName: string;
  amount: number;
  contributorCount: null;
  sourceUrl: string;
};

/**
 * exact: row sum == grid total. rows_exceed_grid: the state subtracted
 * returned contributions the search still lists. rows_below_grid: the search
 * is missing rows, so breakdowns cover only part of the grid total.
 */
export type IdahoRowCoverage = "exact" | "rows_exceed_grid" | "rows_below_grid";

export type IdahoContributionAggregationResult = {
  summary: IdahoDirectFinanceSummary;
  directBreakdowns: IdahoFinanceDirectBreakdown[];
  sourceRowCount: number;
  registrationRowCount: number;
  directContributionRowCount: number;
  itemizedRowCount: number;
  unitemizedRowCount: number;
  nonDirectReceiptRowCount: number;
  nonPositiveRowCount: number;
  /** Dollars. Sum of every registration row (signed, interest included) — the grid's own basis. */
  rowTotal: number;
  gridTotalRaised: number;
  rowCoverage: IdahoRowCoverage;
};

type Aggregate = {
  categoryType: IdahoFinanceDirectCategoryType;
  categoryName: string;
  amountCents: number;
};

function amountToCents(amount: number, label: string): number {
  const cents = Math.round(amount * 100);
  if (!Number.isFinite(amount) || !Number.isSafeInteger(cents)) {
    throw new Error(`Invalid Idaho amount for ${label}: ${amount}`);
  }
  return cents;
}

function gridAmountCents(amount: number, label: string, allowNegative: boolean): number {
  const cents = amountToCents(amount, label);
  if (!allowNegative && cents < 0) {
    throw new Error(`Invalid Idaho grid ${label}: ${amount}`);
  }
  return cents;
}

// Same buckets as New Hampshire (same vendor build).
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
  categoryType: IdahoFinanceDirectCategoryType,
  categoryName: string,
  amountCents: number
): void {
  const key = `${categoryType} ${categoryName}`;
  const existing = aggregates.get(key);
  if (existing) {
    existing.amountCents += amountCents;
    return;
  }
  aggregates.set(key, { categoryType, categoryName, amountCents });
}

function isDirectContribution(row: IdahoContributionRow): boolean {
  const code = row.transactionSubTypeCode;
  if (IDAHO_ITEMIZED_CONTRIBUTION_SUBTYPE_CODES.has(code) || IDAHO_UNITEMIZED_CONTRIBUTION_SUBTYPE_CODES.has(code)) {
    return true;
  }
  if (IDAHO_NON_DIRECT_RECEIPT_SUBTYPE_CODES.has(code)) {
    return false;
  }
  throw new Error(`Unknown Idaho contribution subtype ${JSON.stringify(code)} for transaction ${row.transactionId}`);
}

// Every selected row must belong to this registration's entity and cycle,
// and appear once: the search serves one current version per transaction,
// and a repeated row guid means pagination duplicated it.
function validateRows(rows: readonly IdahoContributionRow[], registration: IdahoCandidateRegistrationRow): void {
  const transactionIds = new Set<number>();
  const guids = new Set<string>();
  for (const row of rows) {
    if (row.filerEntityId !== registration.filerEntityId) {
      throw new Error(
        `Idaho registration ${registration.registrationGuid} received a row for entity ${row.filerEntityId} ` +
          `(expected ${registration.filerEntityId})`
      );
    }
    if (row.electionYear !== registration.electionYear) {
      throw new Error(
        `Idaho registration ${registration.registrationGuid} received a ${row.electionYear} row ` +
          `(expected ${registration.electionYear})`
      );
    }
    if (transactionIds.has(row.transactionId)) {
      throw new Error(`Idaho contribution search repeated transaction ${row.transactionId}`);
    }
    if (guids.has(row.guid)) {
      throw new Error(`Idaho contribution search repeated row ${row.guid}`);
    }
    transactionIds.add(row.transactionId);
    guids.add(row.guid);
    isDirectContribution(row);
  }
}

function toDirectBreakdowns(aggregates: Iterable<Aggregate>, sourceUrl: string): IdahoFinanceDirectBreakdown[] {
  const categoryOrder: IdahoFinanceDirectCategoryType[] = ["contribution_size", "contributor_source_type"];
  const values = [...aggregates];
  const result: IdahoFinanceDirectBreakdown[] = [];
  for (const categoryType of categoryOrder) {
    for (const aggregate of values
      .filter((value) => value.categoryType === categoryType)
      .sort((left, right) => right.amountCents - left.amountCents || left.categoryName.localeCompare(right.categoryName))) {
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

export function aggregateIdahoContributions(input: IdahoContributionAggregationInput): IdahoContributionAggregationResult {
  const { registration } = input;
  const registrationGuid = normalizeIdahoRegistrationGuid(registration.registrationGuid);
  const gridTotalRaisedCents = gridAmountCents(registration.totalRaised, "totalRaised", false);
  const totalDisbursementsCents = gridAmountCents(registration.totalSpent, "totalSpent", false);
  const cashOnHandCents = gridAmountCents(registration.balanceOfFunds, "balanceOfFunds", true);
  const sourceUrl = input.sourceUrl?.trim() || idahoRegistrationProfileUrl(registrationGuid);

  const rows = selectIdahoRegistrationContributions(input.contributionRows, registrationGuid);
  validateRows(rows, registration);

  const aggregates = new Map<string, Aggregate>();
  let rowTotalCents = 0;
  let directContributionTotalCents = 0;
  let directContributionRowCount = 0;
  let itemizedRowCount = 0;
  let unitemizedRowCount = 0;
  let nonDirectReceiptRowCount = 0;
  let nonPositiveRowCount = 0;

  for (const row of rows) {
    const amountCents = amountToCents(row.transactionAmount, `transaction ${row.transactionId}`);
    rowTotalCents += amountCents;
    if (amountCents <= 0) {
      nonPositiveRowCount += 1;
      continue;
    }
    if (!isDirectContribution(row)) {
      nonDirectReceiptRowCount += 1;
      continue;
    }

    directContributionRowCount += 1;
    directContributionTotalCents += amountCents;
    if (IDAHO_ITEMIZED_CONTRIBUTION_SUBTYPE_CODES.has(row.transactionSubTypeCode)) {
      itemizedRowCount += 1;
      addAggregate(aggregates, "contribution_size", contributionSizeBucket(amountCents), amountCents);
    } else {
      unitemizedRowCount += 1;
      addAggregate(aggregates, "contribution_size", IDAHO_UNITEMIZED_SIZE_BUCKET, amountCents);
    }
    addAggregate(aggregates, "contributor_source_type", mapIdahoContributorSourceType(row.sourceTypeCode), amountCents);
  }

  const rowCoverage: IdahoRowCoverage =
    rowTotalCents === gridTotalRaisedCents
      ? "exact"
      : rowTotalCents > gridTotalRaisedCents
        ? "rows_exceed_grid"
        : "rows_below_grid";

  return {
    summary: {
      totalReceipts: gridTotalRaisedCents / 100,
      directContributionTotal: directContributionTotalCents / 100,
      totalDisbursements: totalDisbursementsCents / 100,
      cashOnHand: cashOnHandCents / 100,
      sourceUrl,
    },
    directBreakdowns: toDirectBreakdowns(aggregates.values(), sourceUrl),
    sourceRowCount: input.contributionRows.length,
    registrationRowCount: rows.length,
    directContributionRowCount,
    itemizedRowCount,
    unitemizedRowCount,
    nonDirectReceiptRowCount,
    nonPositiveRowCount,
    rowTotal: rowTotalCents / 100,
    gridTotalRaised: gridTotalRaisedCents / 100,
    rowCoverage,
  };
}
