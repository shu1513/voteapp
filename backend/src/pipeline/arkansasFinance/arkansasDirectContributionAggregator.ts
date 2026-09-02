// Direct-finance aggregation for Arkansas CFIS (plan-arkansas-finance.md,
// Phase 3). Headline totals are the registration row's server-computed
// cycle figures; occupation and contribution-size breakdowns are computed
// from the registration-scoped receipt search. All arithmetic in integer
// cents; dollars only at the output boundary (the shared writer convention).
//
// Verified source facts this module rests on (Phase 0 gate 2 and a live
// gold-set profile on 2026-09-02, eight registrations):
// - The TCON transaction search returns receipts only: loan and
//   returned-contribution rows never appear (a filer with 19 CSV loan rows
//   reconciles to the cent without them), and every row is one of
//   "Itemized Monetary", "Non-Itemized Monetary", "Itemized Nonmoney",
//   "Interest". Anything else fails closed so new vocabulary surfaces.
// - registration.totalRaised = monetary rows + interest rows, cent-exact,
//   for filers whose search carries no superseded report versions (loans
//   and in-kind excluded; interest verified on a filer with 20 interest rows
//   that reconciles only with them counted). Some amended filers overshoot:
//   transactions an amendment removed survive in the search as their own
//   rows (per-row GetTransactionDetailsByGuid on 551 rows across three
//   amended filers found every transactionID exactly once, all at version
//   1 — leftovers, not duplicate versions, so a version dedupe cannot fix
//   them). Leftovers only ever add money, so breakdowns are published only
//   when the sum reconciles exactly; otherwise the totals still publish and
//   the breakdowns are quarantined with the dollar delta. A reconciled
//   amended filer (8313, ten amended reports) carries one current row per
//   transaction; the search has never been seen serving a stale row.
// - Non-itemized rows are per-report lumps (no source, no occupation) and
//   interest rows have no contributor: both count toward the reconciliation
//   and never toward a bucket. Nonmoney (in-kind) rows are outside
//   totalRaised entirely.
// - Amounts can carry sub-cent noise (1500.001 observed); they round to
//   cents, which is how the registration total reconciles.
// - transactionSource is "Individual", "Candidate", "Political Action
//   Committee", "Business/Organization/Unlisted PAC", or null (lumps). Only
//   Individual rows feed the occupation chart; every positive itemized
//   monetary row feeds the size buckets.
// - occupation is the dropdown category or "Other(<free text>)"; unwrap the
//   wrapper, treat placeholders as Unknown, keep the state's own vocabulary.

import type { ArkansasFilerRegistrationRow, ArkansasTransactionRow } from "./arkansasCfisClient.js";
import type { ArkansasFinanceDirectBreakdownInput } from "./arkansasFinanceWriter.js";

export const ARKANSAS_UNKNOWN_OCCUPATION_LABEL = "Unknown";

const DEFAULT_MAX_OCCUPATION_BREAKDOWNS = 25;

const PLACEHOLDER_OCCUPATION_KEYS = new Set(["", "N A", "NA", "NONE", "NOT APPLICABLE", "NULL", "UNKNOWN", "OTHER"]);

type ReceiptKind = "itemized_monetary" | "non_itemized_monetary" | "nonmoney" | "interest";

export type ArkansasDirectContributionAggregationInput = {
  registration: ArkansasFilerRegistrationRow;
  /** TCON rows for registration.registrationGuid (a complete, deduplicated pull). */
  receiptRows: readonly ArkansasTransactionRow[];
  sourceUrl: string | null;
  maxOccupationBreakdowns?: number;
};

export type ArkansasDirectContributionReconciliation = {
  status: "reconciled" | "unreconciled";
  registrationRaisedCents: number;
  receiptMonetaryCents: number;
  deltaCents: number;
};

export type ArkansasDirectContributionAggregationResult = {
  summary: {
    totalReceipts: number;
    directContributionTotal: number;
    totalDisbursements: number;
    cashOnHand: number;
  };
  reconciliation: ArkansasDirectContributionReconciliation;
  /** Empty when unreconciled: the totals still publish, the breakdowns do not. */
  directBreakdowns: ArkansasFinanceDirectBreakdownInput[];
  rowCounts: {
    total: number;
    itemizedMonetary: number;
    nonItemizedMonetary: number;
    nonmoney: number;
    interest: number;
    nonPositive: number;
    subCentAmount: number;
    hasChild: number;
  };
  diagnostics: string[];
};

function centsFromAmount(amount: number, label: string): { cents: number; subCent: boolean } {
  const cents = Math.round(amount * 100);
  if (!Number.isFinite(amount) || !Number.isSafeInteger(cents)) {
    throw new Error(`Arkansas finance ${label} is not a money amount: ${amount}`);
  }
  return { cents, subCent: Math.abs(amount * 100 - cents) > 1e-6 };
}

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

function receiptKind(row: ArkansasTransactionRow): ReceiptKind {
  const subtype = keyText(row.transactionSubTypeDescription);
  if (subtype === "ITEMIZED MONETARY") return "itemized_monetary";
  if (subtype === "NON ITEMIZED MONETARY") return "non_itemized_monetary";
  if (subtype === "ITEMIZED NONMONEY" || subtype === "NON ITEMIZED NONMONEY") return "nonmoney";
  if (subtype === "INTEREST") return "interest";
  throw new Error(
    `Arkansas finance receipt ${row.guid} has an unknown sub type: ${row.transactionSubTypeDescription ?? "<null>"}`
  );
}

/** Dropdown value or "Other(<free text>)"; placeholders collapse to Unknown. */
export function arkansasOccupationLabel(value: string | null): string {
  const trimmed = normalizeText(value);
  const wrapped = /^Other\s*\((.*)\)$/is.exec(trimmed);
  const inner = wrapped?.[1] === undefined ? trimmed : normalizeText(wrapped[1]);
  return PLACEHOLDER_OCCUPATION_KEYS.has(keyText(inner)) ? ARKANSAS_UNKNOWN_OCCUPATION_LABEL : inner;
}

function sizeBucket(amountCents: number): string {
  if (amountCents < 10_000) return "$1-$99";
  if (amountCents < 25_000) return "$100-$249";
  if (amountCents < 50_000) return "$250-$499";
  if (amountCents < 100_000) return "$500-$999";
  if (amountCents < 500_000) return "$1,000-$4,999";
  return "$5,000+";
}

type Aggregate = {
  categoryType: "occupation" | "contribution_size";
  categoryName: string;
  amountCents: number;
  contributors: Set<string>;
};

function addAggregate(
  aggregates: Map<string, Aggregate>,
  categoryType: Aggregate["categoryType"],
  categoryName: string,
  amountCents: number,
  contributor: string
): void {
  const key = `${categoryType} ${keyText(categoryName)}`;
  const aggregate = aggregates.get(key);
  if (aggregate) {
    aggregate.amountCents += amountCents;
    aggregate.contributors.add(contributor);
  } else {
    aggregates.set(key, { categoryType, categoryName, amountCents, contributors: new Set([contributor]) });
  }
}

export function aggregateArkansasDirectContributions(
  input: ArkansasDirectContributionAggregationInput
): ArkansasDirectContributionAggregationResult {
  const maxOccupationBreakdowns = input.maxOccupationBreakdowns ?? DEFAULT_MAX_OCCUPATION_BREAKDOWNS;
  if (!Number.isSafeInteger(maxOccupationBreakdowns) || maxOccupationBreakdowns <= 0) {
    throw new Error(`Invalid Arkansas finance maxOccupationBreakdowns: ${maxOccupationBreakdowns}`);
  }
  const registration = input.registration;
  const registrationRaisedCents = centsFromAmount(registration.totalRaised, "registration totalRaised").cents;
  const totalSpentCents = centsFromAmount(registration.totalSpent, "registration totalSpent").cents;
  const balanceCents = centsFromAmount(registration.balanceOfFunds, "registration balanceOfFunds").cents;

  const rowCounts = {
    total: 0,
    itemizedMonetary: 0,
    nonItemizedMonetary: 0,
    nonmoney: 0,
    interest: 0,
    nonPositive: 0,
    subCentAmount: 0,
    hasChild: 0,
  };
  const aggregates = new Map<string, Aggregate>();
  const seenGuids = new Set<string>();
  let receiptMonetaryCents = 0;

  for (const row of input.receiptRows) {
    if (row.filerRegistrationGuid !== registration.registrationGuid) {
      throw new Error(
        `Arkansas finance receipt ${row.guid} belongs to registration ${row.filerRegistrationGuid}, expected ${registration.registrationGuid}`
      );
    }
    if (seenGuids.has(row.guid)) {
      throw new Error(`Arkansas finance receipt ${row.guid} appears twice`);
    }
    seenGuids.add(row.guid);
    rowCounts.total += 1;
    if (row.hasChild) rowCounts.hasChild += 1;
    const kind = receiptKind(row);
    const amount = centsFromAmount(row.transactionAmount, `receipt ${row.guid} amount`);
    const amountCents = amount.cents;
    if (amount.subCent) rowCounts.subCentAmount += 1;
    if (kind === "nonmoney") {
      rowCounts.nonmoney += 1;
      continue;
    }
    receiptMonetaryCents += amountCents;
    if (kind === "interest") {
      rowCounts.interest += 1;
      continue;
    }
    if (kind === "non_itemized_monetary") {
      rowCounts.nonItemizedMonetary += 1;
      continue;
    }
    rowCounts.itemizedMonetary += 1;
    if (amountCents <= 0) {
      rowCounts.nonPositive += 1;
      continue;
    }
    const contributor = keyText(row.sourceName);
    addAggregate(aggregates, "contribution_size", sizeBucket(amountCents), amountCents, contributor);
    if (keyText(row.transactionSource) === "INDIVIDUAL") {
      addAggregate(aggregates, "occupation", arkansasOccupationLabel(row.occupation), amountCents, contributor);
    }
  }

  const deltaCents = receiptMonetaryCents - registrationRaisedCents;
  const reconciliation: ArkansasDirectContributionReconciliation = {
    status: deltaCents === 0 ? "reconciled" : "unreconciled",
    registrationRaisedCents,
    receiptMonetaryCents,
    deltaCents,
  };
  const diagnostics: string[] = [];
  if (reconciliation.status === "unreconciled") {
    diagnostics.push(
      `Arkansas finance receipts for registration ${registration.registrationGuid} sum to ${(receiptMonetaryCents / 100).toFixed(2)} ` +
        `against the registration total ${(registrationRaisedCents / 100).toFixed(2)} (delta ${(deltaCents / 100).toFixed(2)}); ` +
        "breakdowns withheld"
    );
  }

  const directBreakdowns: ArkansasFinanceDirectBreakdownInput[] = [];
  if (reconciliation.status === "reconciled") {
    const byType = new Map<Aggregate["categoryType"], Aggregate[]>();
    for (const aggregate of aggregates.values()) {
      const rows = byType.get(aggregate.categoryType) ?? [];
      rows.push(aggregate);
      byType.set(aggregate.categoryType, rows);
    }
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
          sourceUrl: input.sourceUrl,
        });
      }
    }
  }

  return {
    summary: {
      // CFIS "Total Raised" is monetary contributions plus interest earned
      // (loans and in-kind excluded), published verbatim as the state's own
      // figure for both receipt totals.
      totalReceipts: registrationRaisedCents / 100,
      directContributionTotal: registrationRaisedCents / 100,
      totalDisbursements: totalSpentCents / 100,
      cashOnHand: balanceCents / 100,
    },
    reconciliation,
    directBreakdowns,
    rowCounts,
    diagnostics,
  };
}
