import { classifyFinanceLabel } from "../finance/financeLabelClassifier.js";
import type { NewMexicoCfisContributionRow } from "./newMexicoCfisArtifactReader.js";

export type NewMexicoDirectContributionAggregationInput = {
  committeeId: string;
  electionYear: number;
  contributionRows: readonly NewMexicoCfisContributionRow[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
};

export type NewMexicoDirectFinanceSummary = {
  totalReceipts: number;
  directContributionTotal: number;
  sourceUrl: string | null;
};

export type NewMexicoContributorSourceType =
  | "individuals"
  | "business_nonprofit_entities"
  | "pac_independent"
  | "party_committee"
  | "candidate_self"
  | "other";

export type NewMexicoFinanceDirectBreakdown = {
  categoryType: "occupation" | "industry" | "contribution_size" | "contributor_source_type";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type NewMexicoDirectContributionAggregationResult = {
  summary: NewMexicoDirectFinanceSummary;
  directBreakdowns: NewMexicoFinanceDirectBreakdown[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type Aggregate = {
  categoryType: NewMexicoFinanceDirectBreakdown["categoryType"];
  categoryName: string;
  amountCents: number;
  contributorKeys: Set<string>;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;
const MIN_DIRECT_INDUSTRY_BREAKDOWN_AMOUNT_CENTS = 25_000 * 100;

const DIRECT_DONOR_SUPPORT_CONTRIBUTION_TYPES = new Set([
  "CONTRIBUTION",
  "CONTRIBUTIONS IN KIND",
  "CONTRIBUTIONS MONETARY",
  "IN KIND CONTRIBUTION",
  "IN KIND CONTRIBUTIONS",
  "MONETARY",
  "MONETARY CONTRIBUTION",
  "MONETARY CONTRIBUTIONS",
]);

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2020 || value > 2100) {
    throw new Error(`Invalid New Mexico direct contribution aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid New Mexico direct contribution aggregation ${fieldName}: ${value}`);
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
  if (normalized.length === 0 || !/^-?\d+(?:\.\d{1,4})?$/.test(normalized)) {
    return null;
  }

  const amount = Number(normalized);
  if (!Number.isFinite(amount)) {
    return null;
  }
  const cents = Math.round(amount * 100);
  return Number.isSafeInteger(cents) ? cents : null;
}

function centsToDollars(cents: number): number {
  return cents / 100;
}

function parseNewMexicoCfisDateYear(raw: string): number | null {
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

export function newMexicoElectionCycleStartYear(electionYear: number): number {
  return normalizeElectionYear(electionYear) - 1;
}

function isCycleYear(input: { rawDate: string; electionYear: number }): boolean {
  const year = parseNewMexicoCfisDateYear(input.rawDate);
  if (year === null) {
    return false;
  }
  return year >= newMexicoElectionCycleStartYear(input.electionYear) && year <= input.electionYear;
}

function isCandidateCommittee(row: NewMexicoCfisContributionRow): boolean {
  return normalizeTextKey(row["Report Entity Type"]) === "CANDIDATE";
}

export function isNewMexicoTotalReceipt(input: {
  row: NewMexicoCfisContributionRow;
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

export function isNewMexicoDirectDonorSupportReceipt(input: {
  row: NewMexicoCfisContributionRow;
  electionYear: number;
}): boolean {
  if (!isNewMexicoTotalReceipt(input)) {
    return false;
  }
  return DIRECT_DONOR_SUPPORT_CONTRIBUTION_TYPES.has(normalizeTextKey(input.row["Contribution Type"]));
}

export function mapNewMexicoContributorSourceType(value: string): NewMexicoContributorSourceType {
  const normalized = normalizeTextKey(value);
  if (/\b(INDIVIDUAL|PERSON)\b/.test(normalized)) {
    return "individuals";
  }
  if (/\b(BUSINESS|CORPORATION|CORPORATE|COMPANY|ORGANIZATION|ENTITY|NONPROFIT|NON PROFIT)\b/.test(normalized)) {
    return "business_nonprofit_entities";
  }
  if (/\b(PAC|POLITICAL COMMITTEE|POLITICAL ACTION COMMITTEE)\b/.test(normalized)) {
    return "pac_independent";
  }
  if (/\b(PARTY|POLITICAL PARTY)\b/.test(normalized)) {
    return "party_committee";
  }
  if (/\b(CANDIDATE|SELF)\b/.test(normalized)) {
    return "candidate_self";
  }
  return "other";
}

function contributorDisplayName(row: NewMexicoCfisContributionRow): string {
  const lastName = row["Last Name"].trim();
  const firstName = row["First Name"].trim();
  const middleName = row["Middle Name"].trim();
  const name = [firstName, middleName, lastName].filter(Boolean).join(" ");
  return name || lastName || firstName;
}

function directBusinessDonorIndustry(row: NewMexicoCfisContributionRow): string | null {
  if (mapNewMexicoContributorSourceType(row["Contributor Code"]) !== "business_nonprofit_entities") {
    return null;
  }
  const classification = classifyFinanceLabel({
    rawLabel: contributorDisplayName(row),
    labelType: "donor",
  });
  return classification.industrySlug;
}

function contributorIdentityKey(row: NewMexicoCfisContributionRow): string {
  const parts = [
    row["Contributor Code"],
    row["Last Name"],
    row["First Name"],
    row["Middle Name"],
    row.Suffix,
    row["Contributor City"],
    row["Contributor State"],
    row["Contributor Zip Code"],
  ]
    .map(normalizeTextKey)
    .filter(Boolean);
  if (parts.length > 0) {
    return parts.join("\u0000");
  }
  return normalizeTextKey(row["Transaction ID"]) || "unknown";
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
}): NewMexicoFinanceDirectBreakdown[] {
  const byCategory = new Map<Aggregate["categoryType"], Aggregate[]>();
  for (const aggregate of input.aggregates) {
    const list = byCategory.get(aggregate.categoryType) ?? [];
    list.push(aggregate);
    byCategory.set(aggregate.categoryType, list);
  }

  const result: NewMexicoFinanceDirectBreakdown[] = [];
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
        contributorCount: aggregate.contributorKeys.size,
        sourceUrl: input.sourceUrl,
      });
    }
  }

  return result;
}

export function aggregateNewMexicoDirectContributions(
  input: NewMexicoDirectContributionAggregationInput
): NewMexicoDirectContributionAggregationResult {
  const committeeId = normalizeId(requireNonEmpty(input.committeeId, "New Mexico committee id"));
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
    if (normalizeId(row.OrgID) !== committeeId) {
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
    if (!isNewMexicoDirectDonorSupportReceipt({ row, electionYear })) {
      skippedContributionRowCount += 1;
      continue;
    }

    includedContributionRowCount += 1;
    directContributionTotalCents += amountCents;
    const contributorKey = contributorIdentityKey(row);
    addAggregate(aggregates, {
      categoryType: "occupation",
      categoryName: row["Contributor Occupation"],
      amountCents,
      contributorKey,
    });
    addAggregate(aggregates, {
      categoryType: "industry",
      categoryName: directBusinessDonorIndustry(row) ?? "",
      amountCents,
      contributorKey,
    });
    addAggregate(aggregates, {
      categoryType: "contributor_source_type",
      categoryName: mapNewMexicoContributorSourceType(row["Contributor Code"]),
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
