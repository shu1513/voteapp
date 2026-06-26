import {
  parseMarylandCfsMoney,
  type MarylandCfsContributionRow,
} from "./marylandCfsArtifactReader.js";

export type MarylandDirectContributionAggregationInput = {
  committeeId: string;
  electionYear: number;
  contributionRows: readonly MarylandCfsContributionRow[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
};

export type MarylandDirectFinanceSummary = {
  totalReceipts: number;
  directContributionTotal: number;
  sourceUrl: string | null;
};

export type MarylandFinanceDirectBreakdown = {
  categoryType: "occupation" | "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type MarylandDirectContributionAggregationResult = {
  summary: MarylandDirectFinanceSummary;
  directBreakdowns: MarylandFinanceDirectBreakdown[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type Aggregate = {
  categoryType: MarylandFinanceDirectBreakdown["categoryType"];
  categoryName: string;
  amountCents: number;
  contributorKeys: Set<string>;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;

const DIRECT_DONOR_SUPPORT_TRANSACTION_TYPES = new Set([
  "CONTRIBUTION",
  "CONTRIBUTIONS",
  "MONETARY CONTRIBUTION",
  "MONETARY CONTRIBUTIONS",
  "IN KIND",
  "IN KIND CONTRIBUTION",
  "IN KIND CONTRIBUTIONS",
  "COORDINATED IN KIND",
  "COORDINATED IN KIND CONTRIBUTION",
]);

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Maryland direct contribution aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Maryland direct contribution aggregation ${fieldName}: ${normalized}`);
  }
  return normalized;
}

function normalizeId(value: string): string {
  return value.trim().replace(/\s+/g, " ").toUpperCase();
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

function parseAmountCents(raw: string): number | null {
  const amount = parseMarylandCfsMoney(raw);
  if (amount === null || !Number.isFinite(amount)) {
    return null;
  }
  const cents = Math.round(amount * 100);
  return Number.isSafeInteger(cents) ? cents : null;
}

function centsToDollars(cents: number): number {
  return cents / 100;
}

function parseMarylandCfsDateYear(raw: string): number | null {
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }
  const slashMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})\b/.exec(trimmed);
  if (slashMatch) {
    return Number(slashMatch[3]);
  }
  const isoMatch = /^(\d{4})-\d{2}-\d{2}/.exec(trimmed);
  if (isoMatch) {
    return Number(isoMatch[1]);
  }
  return null;
}

export function marylandElectionCycleStartYear(electionYear: number): number {
  return normalizeElectionYear(electionYear) - 1;
}

function isCycleYear(input: { rawDate: string; electionYear: number }): boolean {
  const year = parseMarylandCfsDateYear(input.rawDate);
  if (year === null) {
    return false;
  }
  return year >= marylandElectionCycleStartYear(input.electionYear) && year <= input.electionYear;
}

function isCandidateCommittee(row: MarylandCfsContributionRow): boolean {
  const committeeType = normalizeTextKey(row["Committee Type"]);
  return (
    committeeType === "CANDIDATE" ||
    committeeType === "CANDIDATE COMMITTEE" ||
    committeeType === "PUBLIC FINANCING" ||
    committeeType === "PUBLIC FINANCING COMMITTEE"
  );
}

export function isMarylandTotalReceipt(input: {
  row: MarylandCfsContributionRow;
  electionYear: number;
}): boolean {
  const amountCents = parseAmountCents(input.row["Transaction Amount"]);
  return (
    amountCents !== null &&
    amountCents > 0 &&
    isCandidateCommittee(input.row) &&
    isCycleYear({
      rawDate: input.row["Transaction Date"],
      electionYear: normalizeElectionYear(input.electionYear),
    })
  );
}

export function isMarylandDirectDonorSupportReceipt(input: {
  row: MarylandCfsContributionRow;
  electionYear: number;
}): boolean {
  if (!isMarylandTotalReceipt(input)) {
    return false;
  }
  return DIRECT_DONOR_SUPPORT_TRANSACTION_TYPES.has(normalizeTextKey(input.row["Transaction Type"]));
}

function contributorIdentityKey(row: MarylandCfsContributionRow, rowIndex: number): string {
  const parts = [
    row["Contributor Type"],
    row["Contributor Company Name"],
    row["Contributor Last Name"],
    row["Contributor First Name"],
    row["Contributor Middle Name"],
    row["Contributor Mailing Address1"],
    row["Contributor City"],
    row["Contributor State"],
    row["Contributor ZipCode"],
  ]
    .map(normalizeTextKey)
    .filter(Boolean);
  if (parts.length > 0) {
    return parts.join("\u0000");
  }
  const descriptionKey = normalizeTextKey(row.Description);
  return descriptionKey || `unknown-row-${rowIndex}`;
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

function aggregateKey(categoryType: Aggregate["categoryType"], categoryName: string): string {
  return `${categoryType}\u0000${categoryName.trim().toUpperCase()}`;
}

function addAggregate(
  aggregates: Map<string, Aggregate>,
  input: { categoryType: Aggregate["categoryType"]; categoryName: string; amountCents: number; contributorKey: string }
): void {
  const categoryName = input.categoryName.trim().replace(/\s+/g, " ");
  if (!categoryName) {
    return;
  }
  const key = aggregateKey(input.categoryType, categoryName);
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
}): MarylandFinanceDirectBreakdown[] {
  const byCategory = new Map<Aggregate["categoryType"], Aggregate[]>();
  for (const aggregate of input.aggregates) {
    const list = byCategory.get(aggregate.categoryType) ?? [];
    list.push(aggregate);
    byCategory.set(aggregate.categoryType, list);
  }

  const result: MarylandFinanceDirectBreakdown[] = [];
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

export function aggregateMarylandDirectContributions(
  input: MarylandDirectContributionAggregationInput
): MarylandDirectContributionAggregationResult {
  const committeeId = normalizeId(requireNonEmpty(input.committeeId, "Maryland committee id"));
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const sourceUrl = input.sourceUrl ?? null;
  const aggregates = new Map<string, Aggregate>();
  let matchedContributionRowCount = 0;
  let includedContributionRowCount = 0;
  let skippedContributionRowCount = 0;
  let totalReceiptsCents = 0;
  let directContributionTotalCents = 0;

  for (const [rowIndex, row] of input.contributionRows.entries()) {
    if (normalizeId(row["Filing Entity Id"]) !== committeeId) {
      continue;
    }
    matchedContributionRowCount += 1;

    const amountCents = parseAmountCents(row["Transaction Amount"]);
    if (
      amountCents === null ||
      amountCents <= 0 ||
      !isCandidateCommittee(row) ||
      !isCycleYear({ rawDate: row["Transaction Date"], electionYear })
    ) {
      skippedContributionRowCount += 1;
      continue;
    }

    totalReceiptsCents += amountCents;
    if (!isMarylandDirectDonorSupportReceipt({ row, electionYear })) {
      skippedContributionRowCount += 1;
      continue;
    }

    includedContributionRowCount += 1;
    directContributionTotalCents += amountCents;
    addAggregate(aggregates, {
      categoryType: "contribution_size",
      categoryName: contributionSizeBucket(centsToDollars(amountCents)),
      amountCents,
      contributorKey: contributorIdentityKey(row, rowIndex),
    });
  }

  return {
    summary: {
      totalReceipts: centsToDollars(totalReceiptsCents),
      directContributionTotal: centsToDollars(directContributionTotalCents),
      sourceUrl,
    },
    directBreakdowns: toDirectBreakdowns({
      aggregates: aggregates.values(),
      sourceUrl,
      maxBreakdownsPerCategory,
    }),
    matchedContributionRowCount,
    includedContributionRowCount,
    skippedContributionRowCount,
  };
}
