import { classifyFinanceLabel, normalizeFinanceLabel } from "../finance/financeLabelClassifier.js";
import { parseMarylandCfsMoney, type MarylandCfsContributionRow } from "./marylandCfsArtifactReader.js";
import type {
  MarylandFinanceOutsideGroupBreakdownInput,
  MarylandFinanceOutsideGroupInput,
  MarylandFinanceSupportOppose,
} from "./marylandFinanceWriter.js";

export type MarylandOutsideGroupContributionAggregationInput = {
  electionYear: number;
  outsideGroups: readonly MarylandFinanceOutsideGroupInput[];
  contributionRows: readonly MarylandCfsContributionRow[];
  sourceUrl?: string | null;
  minIndustryAmount?: number;
};

export type MarylandOutsideGroupContributionAggregationResult = {
  // ALL donor rows, uncapped (sorted by amount within each group). The sync
  // layer classifies every donor and only caps the PERSISTED donor display
  // rows — capping here would silently drop tail donors from the rebuilt
  // industry totals of a >cap-donor group.
  outsideGroupBreakdowns: MarylandFinanceOutsideGroupBreakdownInput[];
  matchedContributionRowCount: number;
  includedContributionRowCount: number;
  skippedContributionRowCount: number;
};

type DonorAggregate = {
  committeeId: string;
  supportOppose: MarylandFinanceSupportOppose;
  displayName: string;
  normalizedName: string;
  amountCents: number;
};

type IndustryAggregate = {
  committeeId: string;
  supportOppose: MarylandFinanceSupportOppose;
  industrySlug: string;
  amountCents: number;
  donorKeys: Set<string>;
};

const DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS = 0;

const OUTSIDE_DONOR_CONTRIBUTION_TYPES = new Set([
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

const ORGANIZATION_CONTRIBUTOR_TYPE_WORDS = [
  "ASSOCIATION",
  "BUSINESS",
  "CLUB",
  "COMMITTEE",
  "COMPANY",
  "CORPORATION",
  "ENTITY",
  "GROUP",
  "LABOR",
  "LLC",
  "NONPROFIT",
  "ORGANIZATION",
  "PAC",
  "PARTNERSHIP",
  "PARTY",
  "POLITICAL",
  "UNION",
] as const;

const ORGANIZATION_NAME_PATTERN =
  /\b(ASSOCIATION|BROTHERHOOD|BUILDERS?|BUSINESS|CLUB|CO|COMPANY|CORP|CORPORATION|FOUNDATION|GROUP|INC|INCORPORATED|INDUSTRIES|LABOR|LLC|L L C|LLP|L L P|LOCAL|PAC|PARTNERS|PARTNERSHIP|REALTY|REALTORS?|UNION)\b/i;

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Maryland outside group contribution election year: ${value}`);
  }
  return value;
}

function normalizeMinAmount(value: number | undefined): number {
  if (value === undefined) {
    return DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Invalid Maryland outside group contribution minIndustryAmount: ${value}`);
  }
  return Math.round(value * 100);
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
    .replace(/\b(THE|OF|FOR)\b/g, " ")
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

function isCycleYear(input: { rawDate: string; electionYear: number }): boolean {
  const year = parseMarylandCfsDateYear(input.rawDate);
  return year !== null && year >= input.electionYear - 1 && year <= input.electionYear;
}

function isOrganizationContributorType(value: string): boolean {
  const normalized = normalizeTextKey(value);
  if (!normalized || normalized === "INDIVIDUAL") {
    return false;
  }
  return ORGANIZATION_CONTRIBUTOR_TYPE_WORDS.some((word) => normalized.includes(word));
}

function organizationContributorDisplayName(row: MarylandCfsContributionRow): string | null {
  const companyName = row["Contributor Company Name"].trim();
  if (companyName) {
    return companyName;
  }

  const lastName = row["Contributor Last Name"].trim();
  const firstName = row["Contributor First Name"].trim();
  const middleName = row["Contributor Middle Name"].trim();
  if (!isOrganizationContributorType(row["Contributor Type"])) {
    return null;
  }
  if (firstName || middleName) {
    return null;
  }
  if (!lastName || !ORGANIZATION_NAME_PATTERN.test(lastName)) {
    return null;
  }
  return lastName;
}

function isOutsideDonorReceipt(input: { row: MarylandCfsContributionRow; electionYear: number }): boolean {
  const amountCents = parseAmountCents(input.row["Transaction Amount"]);
  return (
    amountCents !== null &&
    amountCents > 0 &&
    isCycleYear({ rawDate: input.row["Transaction Date"], electionYear: input.electionYear }) &&
    OUTSIDE_DONOR_CONTRIBUTION_TYPES.has(normalizeTextKey(input.row["Transaction Type"])) &&
    organizationContributorDisplayName(input.row) !== null
  );
}

function groupKey(input: { committeeId: string; supportOppose: MarylandFinanceSupportOppose }): string {
  return `${normalizeId(input.committeeId)}\u0000${input.supportOppose}`;
}

function donorKey(input: {
  committeeId: string;
  supportOppose: MarylandFinanceSupportOppose;
  normalizedName: string;
}): string {
  return `${normalizeId(input.committeeId)}\u0000${input.supportOppose}\u0000${input.normalizedName}`;
}

function industryKey(input: {
  committeeId: string;
  supportOppose: MarylandFinanceSupportOppose;
  industrySlug: string;
}): string {
  return `${normalizeId(input.committeeId)}\u0000${input.supportOppose}\u0000${input.industrySlug}`;
}

function addDonorAggregate(input: {
  donors: Map<string, DonorAggregate>;
  group: MarylandFinanceOutsideGroupInput;
  committeeId: string;
  displayName: string;
  normalizedName: string;
  amountCents: number;
}): void {
  const key = donorKey({
    committeeId: input.committeeId,
    supportOppose: input.group.supportOppose,
    normalizedName: input.normalizedName,
  });
  const existing = input.donors.get(key);
  if (existing) {
    existing.amountCents += input.amountCents;
    return;
  }
  input.donors.set(key, {
    committeeId: input.group.committeeId,
    supportOppose: input.group.supportOppose,
    displayName: input.displayName,
    normalizedName: input.normalizedName,
    amountCents: input.amountCents,
  });
}

function toBreakdowns(input: {
  donors: Iterable<DonorAggregate>;
  industries: Iterable<IndustryAggregate>;
  sourceUrl: string | null;
}): MarylandFinanceOutsideGroupBreakdownInput[] {
  const result: MarylandFinanceOutsideGroupBreakdownInput[] = [];
  const donorGroups = new Map<string, DonorAggregate[]>();
  const industryGroups = new Map<string, IndustryAggregate[]>();

  for (const donor of input.donors) {
    const key = groupKey(donor);
    const list = donorGroups.get(key) ?? [];
    list.push(donor);
    donorGroups.set(key, list);
  }
  for (const industry of input.industries) {
    const key = groupKey(industry);
    const list = industryGroups.get(key) ?? [];
    list.push(industry);
    industryGroups.set(key, list);
  }

  for (const donors of donorGroups.values()) {
    for (const donor of donors
      .sort((left, right) => right.amountCents - left.amountCents || left.displayName.localeCompare(right.displayName))) {
      result.push({
        committeeId: donor.committeeId,
        supportOppose: donor.supportOppose,
        categoryType: "donor",
        categoryName: donor.displayName,
        amount: centsToDollars(donor.amountCents),
        contributorCount: 1,
        sourceUrl: input.sourceUrl,
      });
    }
  }

  for (const industries of industryGroups.values()) {
    for (const industry of industries
      .sort((left, right) => right.amountCents - left.amountCents || left.industrySlug.localeCompare(right.industrySlug))) {
      result.push({
        committeeId: industry.committeeId,
        supportOppose: industry.supportOppose,
        categoryType: "industry",
        categoryName: industry.industrySlug,
        amount: centsToDollars(industry.amountCents),
        contributorCount: industry.donorKeys.size,
        sourceUrl: input.sourceUrl,
      });
    }
  }

  return result;
}

export function aggregateMarylandOutsideGroupContributions(
  input: MarylandOutsideGroupContributionAggregationInput
): MarylandOutsideGroupContributionAggregationResult {
  const electionYear = normalizeElectionYear(input.electionYear);
  const minIndustryAmountCents = normalizeMinAmount(input.minIndustryAmount);
  const outsideGroupsByCommitteeId = new Map<string, MarylandFinanceOutsideGroupInput[]>();
  for (const group of input.outsideGroups) {
    const committeeId = normalizeId(group.committeeId);
    if (!committeeId) {
      continue;
    }
    const existing = outsideGroupsByCommitteeId.get(committeeId);
    if (existing) {
      existing.push(group);
    } else {
      outsideGroupsByCommitteeId.set(committeeId, [group]);
    }
  }

  if (outsideGroupsByCommitteeId.size === 0) {
    return {
      outsideGroupBreakdowns: [],
      matchedContributionRowCount: 0,
      includedContributionRowCount: 0,
      skippedContributionRowCount: 0,
    };
  }

  const donors = new Map<string, DonorAggregate>();
  let matchedContributionRowCount = 0;
  let includedContributionRowCount = 0;
  let skippedContributionRowCount = 0;

  for (const row of input.contributionRows) {
    const committeeId = normalizeId(row["Filing Entity Id"]);
    const matchingGroups = outsideGroupsByCommitteeId.get(committeeId) ?? [];
    if (matchingGroups.length === 0) {
      continue;
    }
    matchedContributionRowCount += 1;

    const amountCents = parseAmountCents(row["Transaction Amount"]);
    const displayName = organizationContributorDisplayName(row);
    const normalizedName = displayName ? normalizeFinanceLabel(displayName, "donor") : "";
    if (
      !displayName ||
      !normalizedName ||
      amountCents === null ||
      !isOutsideDonorReceipt({ row, electionYear })
    ) {
      skippedContributionRowCount += 1;
      continue;
    }

    includedContributionRowCount += 1;
    for (const group of matchingGroups) {
      addDonorAggregate({
        donors,
        group,
        committeeId,
        displayName,
        normalizedName,
        amountCents,
      });
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
      committeeId: donor.committeeId,
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
      committeeId: donor.committeeId,
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
    }),
    matchedContributionRowCount,
    includedContributionRowCount,
    skippedContributionRowCount,
  };
}
