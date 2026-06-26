import {
  classifyFinanceLabel,
  normalizeFinanceLabel,
  type FinanceIndustrySlug,
  type FinanceLabelClassification,
} from "../finance/financeLabelClassifier.js";
import type { FinanceIndustryClassifier } from "../finance/financeIndustryClassificationService.js";
import {
  isUtahDirectDonorSupportReceipt,
} from "./utahDirectContributionAggregator.js";
import type { UtahDisclosuresTransactionRow } from "./utahDisclosuresClient.js";

export type UtahSupportingCommittee = {
  committeeName: string;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type UtahSupportingCommitteeIndustryBreakdown = {
  supportingCommitteeName: string;
  industrySlug: FinanceIndustrySlug;
  amount: number;
  contributorCount: number;
  sourceUrl: string | null;
};

export type UtahSupportingCommitteeIndustryAggregationInput = {
  electionYear: number;
  candidateTransactions: readonly UtahDisclosuresTransactionRow[];
  committeeTransactions: readonly UtahDisclosuresTransactionRow[];
  candidateCommitteeName?: string | null;
  candidateSourceUrl?: string | null;
  committeeSourceUrl?: string | null;
  maxSupportingCommittees?: number;
  maxIndustriesPerCommittee?: number;
  minIndustryAmount?: number;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  classifyIndustriesWithAi?: boolean;
};

export type UtahSupportingCommitteeIndustryAggregationResult = {
  supportingCommittees: UtahSupportingCommittee[];
  supportingCommitteeIndustryBreakdowns: UtahSupportingCommitteeIndustryBreakdown[];
  matchedCommitteeTransactionRowCount: number;
  includedOrganizationDonorRowCount: number;
  skippedCommitteeTransactionRowCount: number;
};

type SupportingCommitteeAggregate = {
  committeeName: string;
  normalizedName: string;
  amountCents: number;
  contributorKeys: Set<string>;
};

type DonorAggregate = {
  supportingCommitteeName: string;
  supportingCommitteeKey: string;
  donorName: string;
  normalizedDonorName: string;
  amountCents: number;
};

type IndustryAggregate = {
  supportingCommitteeName: string;
  supportingCommitteeKey: string;
  industrySlug: FinanceIndustrySlug;
  amountCents: number;
  donorKeys: Set<string>;
};

const DEFAULT_MAX_SUPPORTING_COMMITTEES = 20;
const DEFAULT_MAX_INDUSTRIES_PER_COMMITTEE = 10;
const DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS = 5_000 * 100;

const ORGANIZATION_NAME_PATTERN =
  /\b(PAC|COMMITTEE|PARTY|UNION|ASSOCIATION|COUNCIL|FOUNDATION|FUND|TRUST|INC|INCORPORATED|LLC|L L C|CORP|CORPORATION|COMPANY|CO|LTD|LIMITED|LLP|LP|GROUP|ORGANIZATION|CLUB|CHAMBER|LOCAL|LEAGUE|FEDERATION|ALLIANCE|COALITION|BANK|UNIVERSITY|HOSPITAL|BUILDERS?|CONSTRUCTION|ENERGY|HEALTH|REALTORS?)\b/;

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 1998 || value > 2100) {
    throw new Error(`Invalid Utah supporting committee industry election year: ${value}`);
  }
  return value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number, fieldName: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Utah supporting committee industry ${fieldName}: ${value}`);
  }
  return normalized;
}

function normalizeMinAmount(value: number | undefined): number {
  if (value === undefined) {
    return DEFAULT_MIN_INDUSTRY_AMOUNT_CENTS;
  }
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Invalid Utah supporting committee industry minIndustryAmount: ${value}`);
  }
  return Math.round(value * 100);
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

function normalizedCommitteeName(value: string): string {
  return normalizeFinanceLabel(
    value
      .replace(/\bPOLITICAL\s+ACTION\s+COMMITTEE\b/gi, "PAC")
      .replace(/\bP\s*A\s*C\b/gi, "PAC"),
    "committee"
  );
}

function normalizedDonorName(value: string): string {
  return normalizeFinanceLabel(value, "donor");
}

function isOrganizationLikeName(value: string): boolean {
  const normalized = normalizeTextKey(value);
  if (!normalized) {
    return false;
  }
  if (ORGANIZATION_NAME_PATTERN.test(normalized)) {
    return true;
  }
  return classifyFinanceLabel({ rawLabel: value, labelType: "donor" }).industrySlug !== null;
}

function contributorIdentityKey(transaction: UtahDisclosuresTransactionRow): string {
  const nameKey = normalizedDonorName(transaction.name ?? "");
  const addressKey = [
    transaction.address1,
    transaction.address2,
    transaction.city,
    transaction.state,
    transaction.zip,
  ]
    .map(normalizeTextKey)
    .filter(Boolean)
    .join("\u0000");
  return [nameKey, addressKey].filter(Boolean).join("\u0000") || normalizeTextKey(transaction.transactionId) || "unknown";
}

function addSupportingCommittee(
  committees: Map<string, SupportingCommitteeAggregate>,
  input: {
    committeeName: string;
    amountCents: number;
    contributorKey: string;
  }
): void {
  const committeeName = input.committeeName.trim().replace(/\s+/g, " ");
  const normalizedName = normalizedCommitteeName(committeeName);
  if (!committeeName || !normalizedName) {
    return;
  }
  const existing = committees.get(normalizedName);
  if (!existing) {
    committees.set(normalizedName, {
      committeeName,
      normalizedName,
      amountCents: input.amountCents,
      contributorKeys: new Set([input.contributorKey]),
    });
    return;
  }
  existing.amountCents += input.amountCents;
  existing.contributorKeys.add(input.contributorKey);
}

function collectSupportingCommittees(input: {
  candidateTransactions: readonly UtahDisclosuresTransactionRow[];
  electionYear: number;
  candidateCommitteeName?: string | null;
}): Map<string, SupportingCommitteeAggregate> {
  const committees = new Map<string, SupportingCommitteeAggregate>();
  for (const transaction of input.candidateTransactions) {
    const amountCents = amountToCents(transaction.amount);
    const contributorName = transaction.name?.trim() ?? "";
    if (
      amountCents === null ||
      amountCents <= 0 ||
      !contributorName ||
      !isOrganizationLikeName(contributorName) ||
      !isUtahDirectDonorSupportReceipt({
        transaction,
        electionYear: input.electionYear,
        committeeName: input.candidateCommitteeName,
      })
    ) {
      continue;
    }
    addSupportingCommittee(committees, {
      committeeName: contributorName,
      amountCents,
      contributorKey: contributorIdentityKey(transaction),
    });
  }
  return committees;
}

function addDonor(
  donors: Map<string, DonorAggregate>,
  input: {
    supportingCommitteeName: string;
    supportingCommitteeKey: string;
    donorName: string;
    normalizedDonorName: string;
    amountCents: number;
  }
): void {
  const key = `${input.supportingCommitteeKey}\u0000${input.normalizedDonorName}`;
  const existing = donors.get(key);
  if (!existing) {
    donors.set(key, input);
    return;
  }
  existing.amountCents += input.amountCents;
}

function collectIncomingOrganizationDonors(input: {
  committeeTransactions: readonly UtahDisclosuresTransactionRow[];
  electionYear: number;
  supportingCommitteesByKey: ReadonlyMap<string, SupportingCommitteeAggregate>;
}): {
  donors: Map<string, DonorAggregate>;
  matchedCommitteeTransactionRowCount: number;
  includedOrganizationDonorRowCount: number;
  skippedCommitteeTransactionRowCount: number;
} {
  const donors = new Map<string, DonorAggregate>();
  let matchedCommitteeTransactionRowCount = 0;
  let includedOrganizationDonorRowCount = 0;
  let skippedCommitteeTransactionRowCount = 0;

  for (const transaction of input.committeeTransactions) {
    const committeeKey = normalizedCommitteeName(transaction.entityName ?? "");
    const committee = input.supportingCommitteesByKey.get(committeeKey);
    if (!committee) {
      continue;
    }
    matchedCommitteeTransactionRowCount += 1;

    const amountCents = amountToCents(transaction.amount);
    const donorName = transaction.name?.trim() ?? "";
    const normalizedName = normalizedDonorName(donorName);
    if (
      amountCents === null ||
      amountCents <= 0 ||
      !donorName ||
      !normalizedName ||
      !isOrganizationLikeName(donorName) ||
      !isUtahDirectDonorSupportReceipt({
        transaction,
        electionYear: input.electionYear,
      })
    ) {
      skippedCommitteeTransactionRowCount += 1;
      continue;
    }

    includedOrganizationDonorRowCount += 1;
    addDonor(donors, {
      supportingCommitteeName: committee.committeeName,
      supportingCommitteeKey: committee.normalizedName,
      donorName,
      normalizedDonorName: normalizedName,
      amountCents,
    });
  }

  return {
    donors,
    matchedCommitteeTransactionRowCount,
    includedOrganizationDonorRowCount,
    skippedCommitteeTransactionRowCount,
  };
}

function classificationKey(normalizedName: string): string {
  return `donor\u0000${normalizedName}`;
}

async function classifyDonors(input: {
  donors: Iterable<DonorAggregate>;
  minIndustryAmountCents: number;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  classifyIndustriesWithAi: boolean;
}): Promise<Map<string, FinanceLabelClassification>> {
  const classifications = new Map<string, FinanceLabelClassification>();
  const aiCandidates = new Map<string, { rawLabel: string; normalizedLabel: string; amount: number }>();

  for (const donor of input.donors) {
    if (donor.amountCents < input.minIndustryAmountCents) {
      continue;
    }
    const classification = classifyFinanceLabel({ rawLabel: donor.donorName, labelType: "donor" });
    classifications.set(classificationKey(donor.normalizedDonorName), classification);
    if (!classification.industrySlug && input.classifyIndustriesWithAi && input.financeIndustryClassifier) {
      const existing = aiCandidates.get(donor.normalizedDonorName);
      if (existing) {
        existing.amount += centsToDollars(donor.amountCents);
      } else {
        aiCandidates.set(donor.normalizedDonorName, {
          rawLabel: donor.donorName,
          normalizedLabel: donor.normalizedDonorName,
          amount: centsToDollars(donor.amountCents),
        });
      }
    }
  }

  if (aiCandidates.size === 0 || !input.classifyIndustriesWithAi || !input.financeIndustryClassifier) {
    return classifications;
  }

  try {
    const aiClassifications = await input.financeIndustryClassifier({
      labels: [...aiCandidates.values()].map((candidate) => ({
        rawLabel: candidate.rawLabel,
        labelType: "donor" as const,
        normalizedLabel: candidate.normalizedLabel,
        amount: candidate.amount,
      })),
    });
    for (const classification of aiClassifications) {
      classifications.set(classificationKey(classification.normalizedLabel), classification);
    }
  } catch {
    // Supporting-committee industries are enrichment-only.
  }

  return classifications;
}

function addIndustry(
  industries: Map<string, IndustryAggregate>,
  input: {
    donor: DonorAggregate;
    industrySlug: FinanceIndustrySlug;
  }
): void {
  const key = `${input.donor.supportingCommitteeKey}\u0000${input.industrySlug}`;
  const existing = industries.get(key);
  if (!existing) {
    industries.set(key, {
      supportingCommitteeName: input.donor.supportingCommitteeName,
      supportingCommitteeKey: input.donor.supportingCommitteeKey,
      industrySlug: input.industrySlug,
      amountCents: input.donor.amountCents,
      donorKeys: new Set([input.donor.normalizedDonorName]),
    });
    return;
  }
  existing.amountCents += input.donor.amountCents;
  existing.donorKeys.add(input.donor.normalizedDonorName);
}

function toSupportingCommittees(input: {
  committees: Iterable<SupportingCommitteeAggregate>;
  sourceUrl: string | null;
  maxSupportingCommittees: number;
}): UtahSupportingCommittee[] {
  return [...input.committees]
    .sort((left, right) => right.amountCents - left.amountCents || left.committeeName.localeCompare(right.committeeName))
    .slice(0, input.maxSupportingCommittees)
    .map((committee) => ({
      committeeName: committee.committeeName,
      amount: centsToDollars(committee.amountCents),
      contributorCount: committee.contributorKeys.size,
      sourceUrl: input.sourceUrl,
    }));
}

function selectSupportingCommittees(input: {
  committees: Iterable<SupportingCommitteeAggregate>;
  maxSupportingCommittees: number;
}): SupportingCommitteeAggregate[] {
  return [...input.committees]
    .sort((left, right) => right.amountCents - left.amountCents || left.committeeName.localeCompare(right.committeeName))
    .slice(0, input.maxSupportingCommittees);
}

function toIndustryBreakdowns(input: {
  industries: Iterable<IndustryAggregate>;
  sourceUrl: string | null;
  maxIndustriesPerCommittee: number;
}): UtahSupportingCommitteeIndustryBreakdown[] {
  const byCommittee = new Map<string, IndustryAggregate[]>();
  for (const industry of input.industries) {
    const bucket = byCommittee.get(industry.supportingCommitteeKey) ?? [];
    bucket.push(industry);
    byCommittee.set(industry.supportingCommitteeKey, bucket);
  }

  const result: UtahSupportingCommitteeIndustryBreakdown[] = [];
  for (const bucket of [...byCommittee.values()].sort((left, right) =>
    (left[0]?.supportingCommitteeName ?? "").localeCompare(right[0]?.supportingCommitteeName ?? "")
  )) {
    for (const industry of bucket
      .sort((left, right) => right.amountCents - left.amountCents || left.industrySlug.localeCompare(right.industrySlug))
      .slice(0, input.maxIndustriesPerCommittee)) {
      result.push({
        supportingCommitteeName: industry.supportingCommitteeName,
        industrySlug: industry.industrySlug,
        amount: centsToDollars(industry.amountCents),
        contributorCount: industry.donorKeys.size,
        sourceUrl: input.sourceUrl,
      });
    }
  }
  return result;
}

export async function aggregateUtahSupportingCommitteeIndustries(
  input: UtahSupportingCommitteeIndustryAggregationInput
): Promise<UtahSupportingCommitteeIndustryAggregationResult> {
  const electionYear = normalizeElectionYear(input.electionYear);
  const maxSupportingCommittees = normalizePositiveInteger(
    input.maxSupportingCommittees,
    DEFAULT_MAX_SUPPORTING_COMMITTEES,
    "maxSupportingCommittees"
  );
  const maxIndustriesPerCommittee = normalizePositiveInteger(
    input.maxIndustriesPerCommittee,
    DEFAULT_MAX_INDUSTRIES_PER_COMMITTEE,
    "maxIndustriesPerCommittee"
  );
  const minIndustryAmountCents = normalizeMinAmount(input.minIndustryAmount);
  const supportingCommitteesByKey = collectSupportingCommittees({
    candidateTransactions: input.candidateTransactions,
    electionYear,
    candidateCommitteeName: input.candidateCommitteeName,
  });

  if (supportingCommitteesByKey.size === 0) {
    return {
      supportingCommittees: [],
      supportingCommitteeIndustryBreakdowns: [],
      matchedCommitteeTransactionRowCount: 0,
      includedOrganizationDonorRowCount: 0,
      skippedCommitteeTransactionRowCount: 0,
    };
  }

  const selectedSupportingCommittees = selectSupportingCommittees({
    committees: supportingCommitteesByKey.values(),
    maxSupportingCommittees,
  });
  const selectedSupportingCommitteesByKey = new Map(
    selectedSupportingCommittees.map((committee) => [committee.normalizedName, committee])
  );

  const incoming = collectIncomingOrganizationDonors({
    committeeTransactions: input.committeeTransactions,
    electionYear,
    supportingCommitteesByKey: selectedSupportingCommitteesByKey,
  });
  const classifications = await classifyDonors({
    donors: incoming.donors.values(),
    minIndustryAmountCents,
    financeIndustryClassifier: input.financeIndustryClassifier,
    classifyIndustriesWithAi: input.classifyIndustriesWithAi !== false,
  });

  const industries = new Map<string, IndustryAggregate>();
  for (const donor of incoming.donors.values()) {
    if (donor.amountCents < minIndustryAmountCents) {
      continue;
    }
    const classification = classifications.get(classificationKey(donor.normalizedDonorName));
    if (!classification?.industrySlug) {
      continue;
    }
    addIndustry(industries, { donor, industrySlug: classification.industrySlug });
  }

  return {
    supportingCommittees: toSupportingCommittees({
      committees: selectedSupportingCommittees,
      sourceUrl: input.candidateSourceUrl ?? null,
      maxSupportingCommittees,
    }),
    supportingCommitteeIndustryBreakdowns: toIndustryBreakdowns({
      industries: industries.values(),
      sourceUrl: input.committeeSourceUrl ?? null,
      maxIndustriesPerCommittee,
    }),
    matchedCommitteeTransactionRowCount: incoming.matchedCommitteeTransactionRowCount,
    includedOrganizationDonorRowCount: incoming.includedOrganizationDonorRowCount,
    skippedCommitteeTransactionRowCount: incoming.skippedCommitteeTransactionRowCount,
  };
}
