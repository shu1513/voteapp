import type { IndianaCampaignFinanceContributionRow } from "./indianaCampaignFinanceReader.js";
import { classifyFinanceLabel } from "../finance/financeLabelClassifier.js";

export type IndianaDirectContributionAggregationInput = {
  committeeId: string;
  electionYear: number;
  contributionRows: readonly IndianaCampaignFinanceContributionRow[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
};

export type IndianaDirectFinanceSummary = {
  totalReceipts: number;
  directContributionTotal: number;
  sourceUrl: string | null;
};

export type IndianaFinanceDirectBreakdown = {
  categoryType: "occupation" | "contribution_size" | "pac_backed_industry";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type IndianaDirectContributionAggregationResult = {
  summary: IndianaDirectFinanceSummary;
  directBreakdowns: IndianaFinanceDirectBreakdown[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type Aggregate = {
  categoryType: IndianaFinanceDirectBreakdown["categoryType"];
  categoryName: string;
  amountCents: number;
  contributorKeys: Set<string>;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;
const DIRECT_DONOR_SUPPORT_TYPES = new Set(["DIRECT", "IN KIND", "IN KIND CONTRIBUTION", "UNITEMIZED"]);
const PAC_CONTRIBUTOR_TYPE_PATTERN = /\b(PAC|POLITICAL ACTION|POLITICAL COMMITTEE|COMMITTEE)\b/;
const NON_ORGANIZATION_DONOR_TYPE_PATTERN =
  /\b(INDIVIDUAL|PERSON|CANDIDATE|SELF|PAC|POLITICAL ACTION|POLITICAL COMMITTEE|COMMITTEE|PARTY)\b/;

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Indiana direct contribution aggregation election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Indiana direct contribution aggregation ${fieldName}: ${value}`);
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

function parseIndianaDateYear(raw: string): number | null {
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }
  const isoMatch = /^(\d{4})-\d{2}-\d{2}/.exec(trimmed);
  if (isoMatch) {
    return Number(isoMatch[1]);
  }
  const slashMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})\b/.exec(trimmed);
  if (slashMatch) {
    return Number(slashMatch[3]);
  }
  return null;
}

export function indianaElectionCycleStartYear(electionYear: number): number {
  return normalizeElectionYear(electionYear) - 1;
}

function isCycleYear(input: { rawDate: string; electionYear: number }): boolean {
  const year = parseIndianaDateYear(input.rawDate);
  return year !== null && year >= indianaElectionCycleStartYear(input.electionYear) && year <= input.electionYear;
}

function isCandidateCommittee(row: IndianaCampaignFinanceContributionRow): boolean {
  return normalizeTextKey(row.CommitteeType) === "CANDIDATE";
}

function isPacCommittee(row: IndianaCampaignFinanceContributionRow): boolean {
  const committeeType = normalizeTextKey(row.CommitteeType);
  return committeeType !== "CANDIDATE" && /\b(PAC|POLITICAL ACTION|POLITICAL COMMITTEE)\b/.test(committeeType);
}

function isPositiveCycleReceipt(input: {
  row: IndianaCampaignFinanceContributionRow;
  electionYear: number;
}): boolean {
  const amountCents = parseAmountCents(input.row.Amount);
  return (
    amountCents !== null &&
    amountCents > 0 &&
    isCycleYear({ rawDate: input.row.ContributionDate, electionYear: normalizeElectionYear(input.electionYear) })
  );
}

export function isIndianaTotalReceipt(input: {
  row: IndianaCampaignFinanceContributionRow;
  electionYear: number;
}): boolean {
  const amountCents = parseAmountCents(input.row.Amount);
  return (
    amountCents !== null &&
    amountCents > 0 &&
    isCandidateCommittee(input.row) &&
    isCycleYear({ rawDate: input.row.ContributionDate, electionYear: normalizeElectionYear(input.electionYear) })
  );
}

export function isIndianaDirectDonorSupportReceipt(input: {
  row: IndianaCampaignFinanceContributionRow;
  electionYear: number;
}): boolean {
  if (!isIndianaTotalReceipt(input)) {
    return false;
  }
  return DIRECT_DONOR_SUPPORT_TYPES.has(normalizeTextKey(input.row.Type));
}

function contributorIdentityKey(row: IndianaCampaignFinanceContributionRow): string {
  const parts = [
    row.ContributorType,
    row.Name,
    row.Address,
    row.City,
    row.State,
    row.Zip,
  ]
    .map(normalizeTextKey)
    .filter(Boolean);
  if (parts.length > 0) {
    return parts.join("\u0000");
  }
  return normalizeTextKey(row.Description) || "unknown";
}

function isPacContributor(row: IndianaCampaignFinanceContributionRow): boolean {
  const contributorType = normalizeTextKey(row.ContributorType);
  const contributorName = normalizeTextKey(row.Name);
  return PAC_CONTRIBUTOR_TYPE_PATTERN.test(contributorType) || /\bPAC\b/.test(contributorName);
}

function isOrganizationDonor(row: IndianaCampaignFinanceContributionRow): boolean {
  const contributorType = normalizeTextKey(row.ContributorType);
  return contributorType.length === 0 || !NON_ORGANIZATION_DONOR_TYPE_PATTERN.test(contributorType);
}

function buildPacCommitteeIdsByName(
  rows: readonly IndianaCampaignFinanceContributionRow[]
): Map<string, Set<string>> {
  const result = new Map<string, Set<string>>();
  for (const row of rows) {
    if (!isPacCommittee(row)) {
      continue;
    }
    const committeeName = normalizeTextKey(row.Committee);
    const committeeId = normalizeId(row.FileNumber);
    if (!committeeName || !committeeId) {
      continue;
    }
    const committeeIds = result.get(committeeName) ?? new Set<string>();
    committeeIds.add(committeeId);
    result.set(committeeName, committeeIds);
  }
  return result;
}

function collectPacCommitteeIdsThatBackCandidate(input: {
  candidateCommitteeId: string;
  electionYear: number;
  rows: readonly IndianaCampaignFinanceContributionRow[];
}): Set<string> {
  const pacCommitteeIdsByName = buildPacCommitteeIdsByName(input.rows);
  const pacCommitteeIds = new Set<string>();
  for (const row of input.rows) {
    if (
      normalizeId(row.FileNumber) !== input.candidateCommitteeId ||
      !isIndianaDirectDonorSupportReceipt({ row, electionYear: input.electionYear }) ||
      !isPacContributor(row)
    ) {
      continue;
    }

    const matchedIds = pacCommitteeIdsByName.get(normalizeTextKey(row.Name));
    for (const committeeId of matchedIds ?? []) {
      pacCommitteeIds.add(committeeId);
    }
  }
  return pacCommitteeIds;
}

function addPacBackedIndustryAggregates(input: {
  aggregates: Map<string, Aggregate>;
  candidateCommitteeId: string;
  electionYear: number;
  rows: readonly IndianaCampaignFinanceContributionRow[];
}): void {
  const pacCommitteeIds = collectPacCommitteeIdsThatBackCandidate({
    candidateCommitteeId: input.candidateCommitteeId,
    electionYear: input.electionYear,
    rows: input.rows,
  });
  if (pacCommitteeIds.size === 0) {
    return;
  }

  for (const row of input.rows) {
    if (
      !pacCommitteeIds.has(normalizeId(row.FileNumber)) ||
      !isPositiveCycleReceipt({ row, electionYear: input.electionYear }) ||
      !DIRECT_DONOR_SUPPORT_TYPES.has(normalizeTextKey(row.Type)) ||
      !isOrganizationDonor(row)
    ) {
      continue;
    }

    const amountCents = parseAmountCents(row.Amount);
    if (amountCents === null || amountCents <= 0) {
      continue;
    }
    const classification = classifyFinanceLabel({ rawLabel: row.Name, labelType: "donor" });
    if (!classification.industrySlug) {
      continue;
    }

    addAggregate(input.aggregates, {
      categoryType: "pac_backed_industry",
      categoryName: classification.industrySlug,
      amountCents,
      contributorKey: contributorIdentityKey(row),
    });
  }
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
}): IndianaFinanceDirectBreakdown[] {
  const byCategory = new Map<Aggregate["categoryType"], Aggregate[]>();
  for (const aggregate of input.aggregates) {
    const list = byCategory.get(aggregate.categoryType) ?? [];
    list.push(aggregate);
    byCategory.set(aggregate.categoryType, list);
  }

  const result: IndianaFinanceDirectBreakdown[] = [];
  const categoryOrder: Aggregate["categoryType"][] = ["occupation", "pac_backed_industry", "contribution_size"];
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

export function aggregateIndianaDirectContributions(
  input: IndianaDirectContributionAggregationInput
): IndianaDirectContributionAggregationResult {
  const committeeId = normalizeId(requireNonEmpty(input.committeeId, "Indiana committee id"));
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
    if (normalizeId(row.FileNumber) !== committeeId) {
      continue;
    }
    matchedContributionRowCount += 1;

    const amountCents = parseAmountCents(row.Amount);
    if (
      amountCents === null ||
      amountCents <= 0 ||
      !isCandidateCommittee(row) ||
      !isCycleYear({ rawDate: row.ContributionDate, electionYear })
    ) {
      skippedContributionRowCount += 1;
      continue;
    }

    totalReceiptsCents += amountCents;
    if (!isIndianaDirectDonorSupportReceipt({ row, electionYear })) {
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
  addPacBackedIndustryAggregates({
    aggregates,
    candidateCommitteeId: committeeId,
    electionYear,
    rows: input.contributionRows,
  });

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
