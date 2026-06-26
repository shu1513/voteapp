import {
  classifyFinanceLabel,
  normalizeFinanceLabel,
} from "../finance/financeLabelClassifier.js";
import { normalizePennsylvaniaCampaignFinanceExportYear } from "./pennsylvaniaCampaignFinanceArtifactCache.js";
import type {
  PennsylvaniaCampaignFinanceContributionRow,
  PennsylvaniaCampaignFinanceFilerRow,
} from "./pennsylvaniaCampaignFinanceReader.js";
import {
  resolvePennsylvaniaOutsideGroupFiler,
  type PennsylvaniaOutsideGroupFilerAlias,
} from "./pennsylvaniaOutsideGroupFilerResolver.js";

export type PennsylvaniaSupportOppose = "support" | "oppose";

export type PennsylvaniaOutsideSpendingGroup = {
  groupId: string;
  groupName: string;
  supportOppose: PennsylvaniaSupportOppose;
  amount: number;
  sourceUrl: string | null;
  electionId?: string | null;
  contributionFilerId?: string | null;
};

export type PennsylvaniaFinanceOutsideGroupBreakdown = {
  groupId: string;
  supportOppose: PennsylvaniaSupportOppose;
  categoryType: "donor" | "industry";
  categoryName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type PennsylvaniaOutsideGroupContributionAggregationInput = {
  electionYear: number;
  outsideGroups: readonly PennsylvaniaOutsideSpendingGroup[];
  filerRows: readonly PennsylvaniaCampaignFinanceFilerRow[];
  contributionRows: readonly PennsylvaniaCampaignFinanceContributionRow[];
  sourceUrl?: string | null;
  maxBreakdownsPerCategory?: number;
  minIndustryAmount?: number;
  aliases?: readonly PennsylvaniaOutsideGroupFilerAlias[];
};

export type PennsylvaniaOutsideGroupContributionAggregationResult = {
  outsideGroupBreakdowns: PennsylvaniaFinanceOutsideGroupBreakdown[];
  matchedContributionRowCount: number;
  includedContributionEventCount: number;
  skippedContributionEventCount: number;
};

type ContributionEvent = {
  rawDate: string;
  rawAmount: string;
};

type DonorAggregate = {
  groupId: string;
  supportOppose: PennsylvaniaSupportOppose;
  displayName: string;
  normalizedName: string;
  amountCents: number;
};

type IndustryAggregate = {
  groupId: string;
  supportOppose: PennsylvaniaSupportOppose;
  industrySlug: string;
  amountCents: number;
  donorKeys: Set<string>;
};

const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 50;
const DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS = 25_000 * 100;

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Pennsylvania outside group contribution ${fieldName}: ${normalized}`);
  }
  return normalized;
}

function normalizeMinAmount(value: number | undefined): number {
  if (value === undefined) {
    return DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Invalid Pennsylvania outside group contribution minIndustryAmount: ${value}`);
  }
  return Math.round(value * 100);
}

function normalizeId(value: string): string {
  return value.trim().toUpperCase();
}

function normalizeGroupId(value: string): string {
  return normalizeTextKey(value) || normalizeId(value);
}

function normalizeTextKey(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function parseAmountCents(raw: string): number | null {
  const normalized = raw.replace(/[$,]/g, "").trim();
  if (!normalized || !/^-?\d+(?:\.\d{1,4})?$/.test(normalized)) {
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

function parsePennsylvaniaContributionDateYear(raw: string): number | null {
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }
  const compactMatch = /^(\d{4})(\d{2})(\d{2})$/.exec(trimmed);
  if (compactMatch) {
    return Number(compactMatch[1]);
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

function isCycleYear(input: { rawDate: string; electionYear: number }): boolean {
  const year = parsePennsylvaniaContributionDateYear(input.rawDate);
  return year !== null && year >= input.electionYear - 1 && year <= input.electionYear;
}

function contributionEventsFromRow(row: PennsylvaniaCampaignFinanceContributionRow): ContributionEvent[] {
  return [
    { rawDate: row.CONTDATE1, rawAmount: row.CONTAMT1 },
    { rawDate: row.CONTDATE2, rawAmount: row.CONTAMT2 },
    { rawDate: row.CONTDATE3, rawAmount: row.CONTAMT3 },
  ].filter((event) => event.rawDate.trim().length > 0 || event.rawAmount.trim().length > 0);
}

function donorDisplayName(row: PennsylvaniaCampaignFinanceContributionRow): string {
  return row.CONTRIBUTOR.trim().replace(/\s+/g, " ");
}

function isOrganizationContributor(row: PennsylvaniaCampaignFinanceContributionRow): boolean {
  const displayName = donorDisplayName(row);
  if (!displayName) {
    return false;
  }
  if (row.OCCUPATION.trim()) {
    return false;
  }
  const normalizedName = normalizeTextKey(displayName);
  return /\b(INC|INCORPORATED|LLC|L L C|LP|LLP|LTD|CO|COMPANY|CORP|CORPORATION|PAC|COMMITTEE|ASSOCIATION|UNION|FOUNDATION|FUND|TRUST|PARTNERS|PARTNERSHIP|LOCAL)\b/.test(
    normalizedName
  );
}

function isOutsideDonorContributionEvent(input: {
  row: PennsylvaniaCampaignFinanceContributionRow;
  event: ContributionEvent;
  electionYear: number;
}): boolean {
  const amountCents = parseAmountCents(input.event.rawAmount);
  return (
    amountCents !== null &&
    amountCents > 0 &&
    isCycleYear({ rawDate: input.event.rawDate, electionYear: input.electionYear }) &&
    isOrganizationContributor(input.row)
  );
}

function filerIdSet(rows: readonly PennsylvaniaCampaignFinanceFilerRow[]): Set<string> {
  return new Set(rows.map((row) => normalizeId(row.FILERID)).filter(Boolean));
}

export function resolvePennsylvaniaOutsideGroupsForContributionAggregation(input: {
  outsideGroups: readonly PennsylvaniaOutsideSpendingGroup[];
  filerRows: readonly PennsylvaniaCampaignFinanceFilerRow[];
  aliases?: readonly PennsylvaniaOutsideGroupFilerAlias[];
}): PennsylvaniaOutsideSpendingGroup[] {
  const knownFilerIds = filerIdSet(input.filerRows);
  return input.outsideGroups.map((group) => {
    const explicitFilerId = normalizeId(group.contributionFilerId ?? "");
    if (explicitFilerId && knownFilerIds.has(explicitFilerId)) {
      return { ...group, groupId: explicitFilerId, contributionFilerId: explicitFilerId };
    }

    const groupId = normalizeId(group.groupId);
    if (groupId && knownFilerIds.has(groupId)) {
      return { ...group, groupId, contributionFilerId: groupId };
    }

    const resolution = resolvePennsylvaniaOutsideGroupFiler({
      organizationName: group.groupName,
      filerRows: input.filerRows,
      aliases: input.aliases,
    });
    if (resolution.status === "matched") {
      return {
        ...group,
        groupId: resolution.filerId,
        contributionFilerId: resolution.filerId,
      };
    }

    return {
      ...group,
      groupId: normalizeGroupId(group.groupId || group.groupName),
      contributionFilerId: null,
    };
  });
}

function groupKey(input: { groupId: string; supportOppose: PennsylvaniaSupportOppose }): string {
  return `${normalizeId(input.groupId)}\u0000${input.supportOppose}`;
}

function donorKey(input: { groupId: string; supportOppose: PennsylvaniaSupportOppose; normalizedName: string }): string {
  return `${normalizeId(input.groupId)}\u0000${input.supportOppose}\u0000${input.normalizedName}`;
}

function industryKey(input: { groupId: string; supportOppose: PennsylvaniaSupportOppose; industrySlug: string }): string {
  return `${normalizeId(input.groupId)}\u0000${input.supportOppose}\u0000${input.industrySlug}`;
}

function toBreakdowns(input: {
  donors: Iterable<DonorAggregate>;
  industries: Iterable<IndustryAggregate>;
  sourceUrl: string | null;
  maxBreakdownsPerCategory: number;
}): PennsylvaniaFinanceOutsideGroupBreakdown[] {
  const result: PennsylvaniaFinanceOutsideGroupBreakdown[] = [];

  for (const donor of [...input.donors]
    .sort((left, right) => right.amountCents - left.amountCents || left.displayName.localeCompare(right.displayName))
    .slice(0, input.maxBreakdownsPerCategory)) {
    result.push({
      groupId: donor.groupId,
      supportOppose: donor.supportOppose,
      categoryType: "donor",
      categoryName: donor.displayName,
      amount: centsToDollars(donor.amountCents),
      contributorCount: 1,
      sourceUrl: input.sourceUrl,
    });
  }

  for (const industry of [...input.industries]
    .sort((left, right) => right.amountCents - left.amountCents || left.industrySlug.localeCompare(right.industrySlug))
    .slice(0, input.maxBreakdownsPerCategory)) {
    result.push({
      groupId: industry.groupId,
      supportOppose: industry.supportOppose,
      categoryType: "industry",
      categoryName: industry.industrySlug,
      amount: centsToDollars(industry.amountCents),
      contributorCount: industry.donorKeys.size,
      sourceUrl: input.sourceUrl,
    });
  }

  return result;
}

export function aggregatePennsylvaniaOutsideGroupContributions(
  input: PennsylvaniaOutsideGroupContributionAggregationInput
): PennsylvaniaOutsideGroupContributionAggregationResult {
  const electionYear = normalizePennsylvaniaCampaignFinanceExportYear(input.electionYear);
  const maxBreakdownsPerCategory = normalizePositiveInteger(
    input.maxBreakdownsPerCategory,
    DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
    "maxBreakdownsPerCategory"
  );
  const minIndustryAmountCents = normalizeMinAmount(input.minIndustryAmount);
  const resolvedOutsideGroups = resolvePennsylvaniaOutsideGroupsForContributionAggregation({
    outsideGroups: input.outsideGroups,
    filerRows: input.filerRows,
    aliases: input.aliases,
  });
  const outsideGroupKeys = new Map<string, PennsylvaniaOutsideSpendingGroup>();
  for (const group of resolvedOutsideGroups) {
    const groupId = normalizeId(group.groupId);
    const contributionFilerId = normalizeId(group.contributionFilerId ?? "");
    if (groupId && contributionFilerId) {
      outsideGroupKeys.set(groupKey({ groupId, supportOppose: group.supportOppose }), {
        ...group,
        groupId,
        contributionFilerId,
      });
    }
  }
  const outsideGroupsByContributionFilerId = new Map<string, PennsylvaniaOutsideSpendingGroup[]>();
  for (const group of outsideGroupKeys.values()) {
    const contributionFilerId = normalizeId(group.contributionFilerId ?? "");
    const existing = outsideGroupsByContributionFilerId.get(contributionFilerId);
    if (existing) {
      existing.push(group);
    } else {
      outsideGroupsByContributionFilerId.set(contributionFilerId, [group]);
    }
  }

  if (outsideGroupKeys.size === 0) {
    return {
      outsideGroupBreakdowns: [],
      matchedContributionRowCount: 0,
      includedContributionEventCount: 0,
      skippedContributionEventCount: 0,
    };
  }

  const donors = new Map<string, DonorAggregate>();
  let matchedContributionRowCount = 0;
  let includedContributionEventCount = 0;
  let skippedContributionEventCount = 0;

  for (const row of input.contributionRows) {
    const contributionFilerId = normalizeId(row.FilerID);
    const matchingGroups = outsideGroupsByContributionFilerId.get(contributionFilerId) ?? [];
    if (matchingGroups.length === 0) {
      continue;
    }
    matchedContributionRowCount += 1;

    const displayName = donorDisplayName(row);
    const normalizedName = normalizeFinanceLabel(displayName, "donor");
    for (const event of contributionEventsFromRow(row)) {
      const amountCents = parseAmountCents(event.rawAmount);
      if (
        !displayName ||
        !normalizedName ||
        amountCents === null ||
        !isOutsideDonorContributionEvent({ row, event, electionYear })
      ) {
        skippedContributionEventCount += 1;
        continue;
      }

      includedContributionEventCount += 1;
      for (const group of matchingGroups) {
        const groupId = normalizeId(group.groupId);
        const key = donorKey({ groupId, supportOppose: group.supportOppose, normalizedName });
        const existing = donors.get(key);
        if (existing) {
          existing.amountCents += amountCents;
          continue;
        }
        donors.set(key, {
          groupId: group.groupId,
          supportOppose: group.supportOppose,
          displayName,
          normalizedName,
          amountCents,
        });
      }
    }
  }

  const industries = new Map<string, IndustryAggregate>();
  for (const donor of donors.values()) {
    if (donor.amountCents < minIndustryAmountCents) {
      continue;
    }
    const classification = classifyFinanceLabel({
      rawLabel: donor.displayName,
      labelType: "donor",
    });
    if (!classification.industrySlug) {
      continue;
    }
    const key = industryKey({
      groupId: donor.groupId,
      supportOppose: donor.supportOppose,
      industrySlug: classification.industrySlug,
    });
    const existing = industries.get(key);
    if (existing) {
      existing.amountCents += donor.amountCents;
      existing.donorKeys.add(donor.normalizedName);
      continue;
    }
    industries.set(key, {
      groupId: donor.groupId,
      supportOppose: donor.supportOppose,
      industrySlug: classification.industrySlug,
      amountCents: donor.amountCents,
      donorKeys: new Set([donor.normalizedName]),
    });
  }

  return {
    outsideGroupBreakdowns: toBreakdowns({
      donors: donors.values(),
      industries: industries.values(),
      sourceUrl: input.sourceUrl ?? null,
      maxBreakdownsPerCategory,
    }),
    matchedContributionRowCount,
    includedContributionEventCount,
    skippedContributionEventCount,
  };
}
