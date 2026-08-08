import {
  GEORGIA_INDIVIDUAL_SOURCE_TYPE_CODE_BY_HOST,
  isGeorgiaRecognizedTransactionStatus,
  type GeorgiaEthicsHost,
  type GeorgiaTransactionRow,
} from "./georgiaEthicsClient.js";

// Direct-contribution aggregation for Georgia (georgia_plan.md D5, taxonomy
// A8). Rows arrive already scoped to the candidate's registration chain and
// already source-selected per report (D8) — this module never filters by
// transaction date (the store holds garbage dates on valid rows, A4) and
// never re-derives report membership.
//
// Bucket rules (D5):
// - occupation: monetary itemized rows from INDIVIDUALS only, keyed on the
//   per-host individual source code (TIND PeachFile, IND archive — A8);
//   blank/placeholder occupations land in an explicit "Unknown" bucket so
//   coverage is honest (D12); unitemized dollars never enter it.
// - contribution_size: every positive monetary itemized row, any source type
//   (maryland parity).
// - In-kind, unitemized, and anonymous rows stay out of both buckets but IN
//   the synced-row sum — the official index total includes them (D4).
// - Returns are their own always-negative rows with no structural link to
//   the original (A9): excluded from buckets, counted in a diagnostic, and
//   INCLUDED in the sum (the index nets returns itself).
// - Rows whose status is outside the host's pinned set are excluded from
//   everything and counted (D8 fail-closed); everything else — loans,
//   interest, unknown subtypes — stays in the sum and out of the buckets,
//   which is exactly what the index total needs (D4).

export type GeorgiaTaggedTransactionRow = {
  host: GeorgiaEthicsHost;
  row: GeorgiaTransactionRow;
};

// Monetary-itemized / unitemized / in-kind / anonymous subtype codes, pinned
// from spike bytes (A8; population-checked on 3,476 PeachFile + 1,119 archive
// rows). PER HOST — the code sets are disjoint. The PeachFile anonymous code
// never appeared in the probed store; an anonymous PeachFile row therefore
// lands in the unpinned-subtype diagnostic, which handles it identically
// (in the sum, out of the buckets).
export const GEORGIA_TRANSACTION_SUB_TYPE_CLASS_BY_HOST_CODE = {
  peachfile: {
    ITMY: "itemized",
    NITMY: "unitemized",
    INKIND: "in_kind",
  },
  efile_archive: {
    MOI: "itemized",
    NIM: "unitemized",
    IKD: "in_kind",
    ANO: "anonymous",
  },
} as const;

type GeorgiaSubTypeClass = "itemized" | "unitemized" | "in_kind" | "anonymous";

// Placeholder occupation values (D5): filed value only, but these filings
// carry no information and collapse into the Unknown bucket.
const PLACEHOLDER_OCCUPATION_KEYS = new Set([
  "",
  "N A",
  "NA",
  "NONE",
  "NOT APPLICABLE",
  "NULL",
  "UNKNOWN",
  "INFO REQUESTED",
  "INFORMATION REQUESTED",
]);

export const GEORGIA_UNKNOWN_OCCUPATION_LABEL = "Unknown";

export type GeorgiaFinanceDirectBreakdown = {
  categoryType: "occupation" | "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type GeorgiaDirectContributionAggregationInput = {
  rows: readonly GeorgiaTaggedTransactionRow[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
};

export type GeorgiaDirectContributionAggregationResult = {
  directBreakdowns: GeorgiaFinanceDirectBreakdown[];
  // Signed sum in dollars of every recognized-status row — the reconciliation
  // input compared against the official candidate-index total (D4).
  syncedRowSum: number;
  totalRowCount: number;
  // Rows that fed at least one bucket.
  bucketedRowCount: number;
  // Itemized individual dollars carrying a real occupation vs the Unknown
  // bucket — the D12 coverage inputs.
  occupationCoveredAmount: number;
  occupationUnknownAmount: number;
  unitemizedAmount: number;
  inKindAmount: number;
  anonymousAmount: number;
  returnedRowCount: number;
  returnedAmount: number;
  unpinnedSubTypeRowCount: number;
  unpinnedSubTypeAmount: number;
  unrecognizedStatusRowCount: number;
  unrecognizedStatusAmount: number;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;

type Aggregate = {
  categoryType: GeorgiaFinanceDirectBreakdown["categoryType"];
  categoryName: string;
  amountCents: number;
  contributorKeys: Set<string>;
};

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Georgia direct contribution aggregation ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeTextKey(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function amountToCents(amount: number): number | null {
  if (!Number.isFinite(amount)) {
    return null;
  }
  const cents = Math.round(amount * 100);
  return Number.isSafeInteger(cents) ? cents : null;
}

function centsToDollars(cents: number): number {
  return cents / 100;
}

function subTypeClass(host: GeorgiaEthicsHost, code: string | null): GeorgiaSubTypeClass | null {
  if (!code) {
    return null;
  }
  const classByCode: Record<string, GeorgiaSubTypeClass> = GEORGIA_TRANSACTION_SUB_TYPE_CLASS_BY_HOST_CODE[host];
  return classByCode[code.trim()] ?? null;
}

function isReturnRow(amountCents: number): boolean {
  // Returns are always-negative rows with no structural link to the original
  // (A9). The API's transactionTypeCode is the constant class code
  // (TCON/CON), not the per-row type name, so the sign is the discriminator —
  // and any other negative row (e.g. a loan payment) belongs out of the
  // buckets and in the sum for exactly the same reasons.
  return amountCents < 0;
}

function contributorIdentityKey(row: GeorgiaTransactionRow): string {
  const parts = [row.sourceName, row.payeeEmployer, row.payeeOccupation].map(normalizeTextKey).filter(Boolean);
  return parts.length > 0 ? parts.join("\u0000") : `unknown-${row.transactionId}`;
}

function occupationCategoryName(row: GeorgiaTransactionRow): string {
  const filed = (row.payeeOccupation ?? "").trim().replace(/\s+/g, " ");
  return PLACEHOLDER_OCCUPATION_KEYS.has(normalizeTextKey(filed)) ? GEORGIA_UNKNOWN_OCCUPATION_LABEL : filed;
}

function contributionSizeBucket(amount: number): string {
  if (amount < 100) {
    return "$1-$99";
  }
  if (amount < 250) {
    return "$100-$249";
  }
  if (amount < 500) {
    return "$250-$499";
  }
  if (amount < 1_000) {
    return "$500-$999";
  }
  if (amount < 5_000) {
    return "$1,000-$4,999";
  }
  return "$5,000+";
}

function addAggregate(
  aggregates: Map<string, Aggregate>,
  input: {
    categoryType: Aggregate["categoryType"];
    categoryName: string;
    amountCents: number;
    contributorKey: string;
  }
): void {
  const categoryName = input.categoryName.trim().replace(/\s+/g, " ");
  if (!categoryName) {
    return;
  }
  const key = `${input.categoryType}\u0000${categoryName.toUpperCase()}`;
  const existing = aggregates.get(key);
  if (!existing) {
    aggregates.set(key, {
      categoryType: input.categoryType,
      categoryName,
      amountCents: input.amountCents,
      contributorKeys: new Set([input.contributorKey]),
    });
    return;
  }
  existing.amountCents += input.amountCents;
  existing.contributorKeys.add(input.contributorKey);
}

function toDirectBreakdowns(input: {
  aggregates: Iterable<Aggregate>;
  sourceUrl: string | null;
  maxBreakdownsPerCategory: number;
}): GeorgiaFinanceDirectBreakdown[] {
  const byCategory = new Map<Aggregate["categoryType"], Aggregate[]>();
  for (const aggregate of input.aggregates) {
    const list = byCategory.get(aggregate.categoryType) ?? [];
    list.push(aggregate);
    byCategory.set(aggregate.categoryType, list);
  }
  const result: GeorgiaFinanceDirectBreakdown[] = [];
  const categoryOrder: Aggregate["categoryType"][] = ["occupation", "contribution_size"];
  for (const categoryType of categoryOrder) {
    const limit = categoryType === "contribution_size" ? Number.POSITIVE_INFINITY : input.maxBreakdownsPerCategory;
    for (const aggregate of (byCategory.get(categoryType) ?? [])
      .sort((left, right) => right.amountCents - left.amountCents || left.categoryName.localeCompare(right.categoryName))
      .slice(0, limit)) {
      result.push({
        categoryType: aggregate.categoryType,
        categoryName: aggregate.categoryName,
        amount: centsToDollars(aggregate.amountCents),
        contributorCount: aggregate.contributorKeys.size,
        sourceUrl: input.sourceUrl,
      });
    }
  }
  return result;
}

export function aggregateGeorgiaDirectContributions(
  input: GeorgiaDirectContributionAggregationInput
): GeorgiaDirectContributionAggregationResult {
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const sourceUrl = input.sourceUrl ?? null;
  const aggregates = new Map<string, Aggregate>();

  let syncedRowSumCents = 0;
  let bucketedRowCount = 0;
  let occupationCoveredCents = 0;
  let occupationUnknownCents = 0;
  let unitemizedCents = 0;
  let inKindCents = 0;
  let anonymousCents = 0;
  let returnedRowCount = 0;
  let returnedCents = 0;
  let unpinnedSubTypeRowCount = 0;
  let unpinnedSubTypeCents = 0;
  let unrecognizedStatusRowCount = 0;
  let unrecognizedStatusCents = 0;

  for (const { host, row } of input.rows) {
    const amountCents = amountToCents(row.transactionAmount);
    if (amountCents === null) {
      unrecognizedStatusRowCount += 1;
      continue;
    }
    if (!isGeorgiaRecognizedTransactionStatus(host, row.transactionStatusCode)) {
      unrecognizedStatusRowCount += 1;
      unrecognizedStatusCents += amountCents;
      continue;
    }

    syncedRowSumCents += amountCents;

    if (isReturnRow(amountCents)) {
      returnedRowCount += 1;
      returnedCents += amountCents;
      continue;
    }

    const subClass = subTypeClass(host, row.transactionSubTypeCode);
    if (subClass === null) {
      unpinnedSubTypeRowCount += 1;
      unpinnedSubTypeCents += amountCents;
      continue;
    }
    if (subClass === "unitemized") {
      unitemizedCents += amountCents;
      continue;
    }
    if (subClass === "in_kind") {
      inKindCents += amountCents;
      continue;
    }
    if (subClass === "anonymous") {
      anonymousCents += amountCents;
      continue;
    }
    if (amountCents <= 0) {
      // Zero-amount itemized rows carry no bucket information.
      continue;
    }

    bucketedRowCount += 1;
    const contributorKey = contributorIdentityKey(row);
    addAggregate(aggregates, {
      categoryType: "contribution_size",
      categoryName: contributionSizeBucket(centsToDollars(amountCents)),
      amountCents,
      contributorKey,
    });

    const isIndividual = row.transactionSourceTypeCode?.trim() === GEORGIA_INDIVIDUAL_SOURCE_TYPE_CODE_BY_HOST[host];
    if (isIndividual) {
      const categoryName = occupationCategoryName(row);
      if (categoryName === GEORGIA_UNKNOWN_OCCUPATION_LABEL) {
        occupationUnknownCents += amountCents;
      } else {
        occupationCoveredCents += amountCents;
      }
      addAggregate(aggregates, {
        categoryType: "occupation",
        categoryName,
        amountCents,
        contributorKey,
      });
    }
  }

  return {
    directBreakdowns: toDirectBreakdowns({ aggregates: aggregates.values(), sourceUrl, maxBreakdownsPerCategory }),
    syncedRowSum: centsToDollars(syncedRowSumCents),
    totalRowCount: input.rows.length,
    bucketedRowCount,
    occupationCoveredAmount: centsToDollars(occupationCoveredCents),
    occupationUnknownAmount: centsToDollars(occupationUnknownCents),
    unitemizedAmount: centsToDollars(unitemizedCents),
    inKindAmount: centsToDollars(inKindCents),
    anonymousAmount: centsToDollars(anonymousCents),
    returnedRowCount,
    returnedAmount: centsToDollars(returnedCents),
    unpinnedSubTypeRowCount,
    unpinnedSubTypeAmount: centsToDollars(unpinnedSubTypeCents),
    unrecognizedStatusRowCount,
    unrecognizedStatusAmount: centsToDollars(unrecognizedStatusCents),
  };
}
