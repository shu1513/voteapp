import type { OklahomaGuardianContributionRow } from "./oklahomaGuardianContributionReader.js";

export type OklahomaDirectContributionAggregationInput = {
  committeeId: string;
  electionYear: number;
  contributionRows: readonly OklahomaGuardianContributionRow[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
};

export type OklahomaDirectFinanceSummary = {
  totalReceipts: number;
  directContributionTotal: number;
  sourceUrl: string | null;
};

export type OklahomaFinanceDirectBreakdown = {
  categoryType: "occupation" | "contribution_size";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type OklahomaDirectContributionAggregationResult = {
  summary: OklahomaDirectFinanceSummary;
  directBreakdowns: OklahomaFinanceDirectBreakdown[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type Aggregate = {
  categoryType: OklahomaFinanceDirectBreakdown["categoryType"];
  categoryName: string;
  amountCents: number;
  contributorKeys: Set<string>;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;

const DIRECT_DONOR_SUPPORT_RECEIPT_TYPES = new Set([
  "CONTRIBUTION",
  "IN KIND",
  "IN KIND CONTRIBUTION",
  "MONETARY",
  "MONETARY CONTRIBUTION",
]);

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2014 || value > 2100) {
    throw new Error(`Invalid Oklahoma direct contribution aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Oklahoma direct contribution aggregation ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeId(value: string): string {
  return value.trim().toUpperCase();
}

function normalizeTextKey(value: string): string {
  return value
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function parseAmountCents(raw: string): number | null {
  const normalized = raw.replace(/[$,]/g, "").trim();
  if (normalized.length === 0 || !/^-?\d+(?:\.\d{1,2})?$/.test(normalized)) {
    return null;
  }

  const sign = normalized.startsWith("-") ? -1 : 1;
  const unsigned = sign === -1 ? normalized.slice(1) : normalized;
  const [dollarsPart, centsPart = ""] = unsigned.split(".");
  const dollars = Number.parseInt(dollarsPart, 10);
  if (!Number.isSafeInteger(dollars)) {
    return null;
  }
  const cents = Number.parseInt(centsPart.padEnd(2, "0"), 10) || 0;
  const total = dollars * 100 + cents;
  return Number.isSafeInteger(total) ? sign * total : null;
}

function centsToDollars(cents: number): number {
  return cents / 100;
}

function parseOklahomaGuardianDateYear(raw: string): number | null {
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

export function oklahomaElectionCycleStartYear(electionYear: number): number {
  return normalizeElectionYear(electionYear) - 1;
}

function isCycleYear(input: { rawDate: string; electionYear: number }): boolean {
  const year = parseOklahomaGuardianDateYear(input.rawDate);
  if (year === null) {
    return false;
  }
  return year >= oklahomaElectionCycleStartYear(input.electionYear) && year <= input.electionYear;
}

function isCandidateCommittee(row: OklahomaGuardianContributionRow): boolean {
  return normalizeTextKey(row["Committee Type"]).includes("CANDIDATE");
}

export function isOklahomaTotalReceipt(input: {
  row: OklahomaGuardianContributionRow;
  electionYear: number;
}): boolean {
  const amountCents = parseAmountCents(input.row["Receipt Amount"]);
  return (
    amountCents !== null &&
    amountCents > 0 &&
    isCandidateCommittee(input.row) &&
    isCycleYear({
      rawDate: input.row["Receipt Date"],
      electionYear: normalizeElectionYear(input.electionYear),
    })
  );
}

export function isOklahomaDirectDonorSupportReceipt(input: {
  row: OklahomaGuardianContributionRow;
  electionYear: number;
}): boolean {
  if (!isOklahomaTotalReceipt(input)) {
    return false;
  }
  return DIRECT_DONOR_SUPPORT_RECEIPT_TYPES.has(normalizeTextKey(input.row["Receipt Type"]));
}

function contributorIdentityKey(row: OklahomaGuardianContributionRow): string {
  const parts = [
    row["Receipt Source Type"],
    row["Last Name"],
    row["First Name"],
    row["Middle Name"],
    row.Suffix,
    row.City,
    row.State,
    row.Zip,
  ]
    .map(normalizeTextKey)
    .filter(Boolean);
  if (parts.length > 0) {
    return parts.join("\u0000");
  }
  return normalizeTextKey(row["Receipt ID"]) || "unknown";
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
  if (categoryName.length === 0) {
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
}): OklahomaFinanceDirectBreakdown[] {
  const byCategory = new Map<Aggregate["categoryType"], Aggregate[]>();
  for (const aggregate of input.aggregates) {
    const list = byCategory.get(aggregate.categoryType) ?? [];
    list.push(aggregate);
    byCategory.set(aggregate.categoryType, list);
  }

  const result: OklahomaFinanceDirectBreakdown[] = [];
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

export function aggregateOklahomaDirectContributions(
  input: OklahomaDirectContributionAggregationInput
): OklahomaDirectContributionAggregationResult {
  const committeeId = normalizeId(requireNonEmpty(input.committeeId, "Oklahoma committee id"));
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const aggregates = new Map<string, Aggregate>();
  let matchedContributionRowCount = 0;
  let includedContributionRowCount = 0;
  let skippedContributionRowCount = 0;
  let totalReceiptsCents = 0;
  let directContributionTotalCents = 0;

  for (const row of input.contributionRows) {
    if (normalizeId(row["Org ID"]) !== committeeId) {
      continue;
    }
    matchedContributionRowCount += 1;

    const amountCents = parseAmountCents(row["Receipt Amount"]);
    if (
      amountCents === null ||
      amountCents <= 0 ||
      !isCandidateCommittee(row) ||
      !isCycleYear({ rawDate: row["Receipt Date"], electionYear })
    ) {
      skippedContributionRowCount += 1;
      continue;
    }

    totalReceiptsCents += amountCents;
    if (!isOklahomaDirectDonorSupportReceipt({ row, electionYear })) {
      skippedContributionRowCount += 1;
      continue;
    }

    includedContributionRowCount += 1;
    directContributionTotalCents += amountCents;
    const contributorKey = contributorIdentityKey(row);
    addAggregate(aggregates, {
      categoryType: "occupation",
      categoryName: row.Occupation,
      amountCents,
      contributorKey,
    });
    addAggregate(aggregates, {
      categoryType: "contribution_size",
      categoryName: contributionSizeBucket(centsToDollars(amountCents)),
      amountCents,
      contributorKey,
    });
  }

  return {
    summary: {
      totalReceipts: centsToDollars(totalReceiptsCents),
      directContributionTotal: centsToDollars(directContributionTotalCents),
      sourceUrl: input.sourceUrl ?? null,
    },
    directBreakdowns: toDirectBreakdowns({
      aggregates: aggregates.values(),
      sourceUrl: input.sourceUrl ?? null,
      maxBreakdownsPerCategory,
    }),
    matchedContributionRowCount,
    includedContributionRowCount,
    skippedContributionRowCount,
  };
}
