import type { NebraskaNadcContributionRow } from "./nebraskaNadcArtifactReader.js";
import { classifyFinanceLabel } from "../finance/financeLabelClassifier.js";

export type NebraskaDirectContributionAggregationInput = {
  committeeId: string;
  electionYear: number;
  contributionRows: readonly NebraskaNadcContributionRow[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
};

export type NebraskaDirectFinanceSummary = {
  totalReceipts: number;
  directContributionTotal: number;
  sourceUrl: string | null;
};

export type NebraskaContributorSourceType =
  | "individuals"
  | "business_nonprofit_entities"
  | "pac_independent"
  | "party_committee"
  | "candidate_self"
  | "other";

export type NebraskaFinanceDirectBreakdown = {
  categoryType: "occupation" | "industry" | "contribution_size" | "contributor_source_type";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type NebraskaDirectContributionAggregationResult = {
  summary: NebraskaDirectFinanceSummary;
  directBreakdowns: NebraskaFinanceDirectBreakdown[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type Aggregate = {
  categoryType: NebraskaFinanceDirectBreakdown["categoryType"];
  categoryName: string;
  amountCents: number;
  contributorCount: number;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;
const MIN_DIRECT_INDUSTRY_BREAKDOWN_AMOUNT_CENTS = 25_000 * 100;
const DIRECT_DONOR_SUPPORT_RECEIPT_TYPES = new Set([
  "CONTRIBUTION",
  "EARMARKED IN KIND",
  "EARMARKED MONETARY",
  "IN KIND CONTRIBUTION",
  "MONETARY",
  "MONETARY CONTRIBUTION",
  "PLEDGE PAYMENT RECEIVED",
]);

const CONTRIBUTOR_SOURCE_TYPE_BUCKETS = new Map<string, NebraskaContributorSourceType>([
  ["BUSINESS FOR PROFIT AND NON PROFIT ENTITIES", "business_nonprofit_entities"],
  ["INDIVIDUAL", "individuals"],
  ["PAC INDEPENDENT", "pac_independent"],
  ["POLITICAL PARTY COMMITTEE", "party_committee"],
  ["SELF CANDIDATE", "candidate_self"],
]);

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2021 || value > 2100) {
    throw new Error(`Invalid Nebraska direct contribution aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Nebraska direct contribution aggregation ${fieldName}: ${value}`);
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

function parseNebraskaNadcDateYear(raw: string): number | null {
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

export function nebraskaElectionCycleStartYear(electionYear: number): number {
  return normalizeElectionYear(electionYear) - 1;
}

function isCycleYear(input: { rawDate: string; electionYear: number }): boolean {
  const year = parseNebraskaNadcDateYear(input.rawDate);
  if (year === null) {
    return false;
  }
  return year >= nebraskaElectionCycleStartYear(input.electionYear) && year <= input.electionYear;
}

function isCandidateCommittee(row: NebraskaNadcContributionRow): boolean {
  return normalizeTextKey(row["Filer Type"]) === "CANDIDATE COMMITTEE";
}

export function isNebraskaTotalReceipt(input: {
  row: NebraskaNadcContributionRow;
  electionYear: number;
}): boolean {
  const amountCents = parseAmountCents(input.row["Receipt Amount"]);
  return (
    amountCents !== null &&
    amountCents > 0 &&
    isCandidateCommittee(input.row) &&
    isCycleYear({ rawDate: input.row["Receipt Date"], electionYear: normalizeElectionYear(input.electionYear) })
  );
}

export function isNebraskaDirectDonorSupportReceipt(input: {
  row: NebraskaNadcContributionRow;
  electionYear: number;
}): boolean {
  if (!isNebraskaTotalReceipt(input)) {
    return false;
  }
  return DIRECT_DONOR_SUPPORT_RECEIPT_TYPES.has(normalizeTextKey(input.row["Receipt Transaction/Contribution Type"]));
}

export function mapNebraskaContributorSourceType(value: string): NebraskaContributorSourceType {
  return CONTRIBUTOR_SOURCE_TYPE_BUCKETS.get(normalizeTextKey(value)) ?? "other";
}

function directBusinessDonorIndustry(row: NebraskaNadcContributionRow): string | null {
  if (mapNebraskaContributorSourceType(row["Contributor or Transaction Source Type"]) !== "business_nonprofit_entities") {
    return null;
  }
  const classification = classifyFinanceLabel({
    rawLabel: row["Contributor or Source Name (Individual Last Name)"],
    labelType: "donor",
  });
  return classification.industrySlug;
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
  input: { categoryType: Aggregate["categoryType"]; categoryName: string; amountCents: number }
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
      contributorCount: 1,
    });
    return;
  }

  existing.amountCents += input.amountCents;
  existing.contributorCount += 1;
}

function toDirectBreakdowns(input: {
  aggregates: Iterable<Aggregate>;
  sourceUrl: string | null;
  maxBreakdownsPerCategory: number;
}): NebraskaFinanceDirectBreakdown[] {
  const byCategory = new Map<Aggregate["categoryType"], Aggregate[]>();
  for (const aggregate of input.aggregates) {
    const list = byCategory.get(aggregate.categoryType) ?? [];
    list.push(aggregate);
    byCategory.set(aggregate.categoryType, list);
  }

  const result: NebraskaFinanceDirectBreakdown[] = [];
  const categoryOrder: Aggregate["categoryType"][] = [
    "occupation",
    "industry",
    "contributor_source_type",
    "contribution_size",
  ];
  for (const categoryType of categoryOrder) {
    const limit = categoryType === "contribution_size" ? Number.POSITIVE_INFINITY : input.maxBreakdownsPerCategory;
    for (const aggregate of (byCategory.get(categoryType) ?? [])
      .filter(
        (item) => categoryType !== "industry" || item.amountCents >= MIN_DIRECT_INDUSTRY_BREAKDOWN_AMOUNT_CENTS
      )
      .sort((left, right) => right.amountCents - left.amountCents || left.categoryName.localeCompare(right.categoryName))
      .slice(0, limit)) {
      result.push({
        categoryType: aggregate.categoryType,
        categoryName: aggregate.categoryName,
        amount: centsToDollars(aggregate.amountCents),
        contributorCount: aggregate.contributorCount,
        sourceUrl: input.sourceUrl,
      });
    }
  }

  return result;
}

export function aggregateNebraskaDirectContributions(
  input: NebraskaDirectContributionAggregationInput
): NebraskaDirectContributionAggregationResult {
  const committeeId = normalizeId(requireNonEmpty(input.committeeId, "Nebraska committee id"));
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
    if (!isNebraskaDirectDonorSupportReceipt({ row, electionYear })) {
      skippedContributionRowCount += 1;
      continue;
    }

    includedContributionRowCount += 1;
    directContributionTotalCents += amountCents;
    addAggregate(aggregates, {
      categoryType: "occupation",
      categoryName: row.Occupation,
      amountCents,
    });
    addAggregate(aggregates, {
      categoryType: "industry",
      categoryName: directBusinessDonorIndustry(row) ?? "",
      amountCents,
    });
    addAggregate(aggregates, {
      categoryType: "contributor_source_type",
      categoryName: mapNebraskaContributorSourceType(row["Contributor or Transaction Source Type"]),
      amountCents,
    });
    addAggregate(aggregates, {
      categoryType: "contribution_size",
      categoryName: contributionSizeBucket(centsToDollars(amountCents)),
      amountCents,
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
