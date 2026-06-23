import type { Pool, PoolClient } from "pg";

import {
  classifyFinanceLabel,
  normalizeFinanceLabel,
  type FinanceLabelClassification,
} from "../finance/financeLabelClassifier.js";
import {
  buildFinanceIndustryBreakdownsFromClassifications,
  mergeFinanceLabelClassification,
  resolveFinanceIndustryClassifications,
  type FinanceIndustryClassifier,
} from "../finance/financeIndustryClassificationService.js";
import {
  normalizeHawaiiCandidateNameKeys,
  searchAndResolveHawaiiCandidateCommittee,
  type HawaiiCandidateCommitteeMatch,
  type HawaiiCandidateCommitteeResolution,
} from "./hawaiiCandidateCommitteeResolver.js";
import {
  getHawaiiCscContributionSizeAggregates,
  getHawaiiCscDirectOccupationAggregates,
  getHawaiiCscIndependentExpenditureGroups,
  getHawaiiCscNoncandidateCommitteeFunders,
  HAWAII_CSC_CANDIDATE_CONTRIBUTIONS_DATASET,
  HAWAII_CSC_DATA_BASE_URL,
  HAWAII_CSC_NONCANDIDATE_CONTRIBUTIONS_DATASET,
  HAWAII_CSC_NONCANDIDATE_EXPENDITURES_DATASET,
  type HawaiiCscAggregate,
  type HawaiiCscClientOptions,
  type HawaiiCscIndependentSpendingGroup,
} from "./hawaiiCscClient.js";
import {
  replaceHawaiiCandidateFinanceSnapshot,
  type HawaiiFinanceDirectBreakdownInput,
  type HawaiiFinanceLinkInput,
  type HawaiiFinanceOutsideGroupBreakdownInput,
  type HawaiiFinanceOutsideGroupInput,
  type HawaiiFinanceSummaryInput,
} from "./hawaiiFinanceWriter.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & Pick<Pool, "connect">;

type HawaiiCscDataClient = {
  searchAndResolveCandidateCommittee: (
    input: {
      candidateName: string;
      officeScope: string;
      officeName: string;
      electionYear: number;
      district?: string | null;
    },
    options?: HawaiiCscClientOptions
  ) => Promise<HawaiiCandidateCommitteeResolution>;
  getDirectOccupationAggregates: (
    input: { committeeId: string; electionPeriod: string; limit?: number },
    options?: HawaiiCscClientOptions
  ) => Promise<HawaiiCscAggregate[]>;
  getContributionSizeAggregates: (
    input: { committeeId: string; electionPeriod: string; limit?: number },
    options?: HawaiiCscClientOptions
  ) => Promise<HawaiiCscAggregate[]>;
  getIndependentExpenditureGroups: (
    input: { candidateName: string; electionYear: number; limit?: number },
    options?: HawaiiCscClientOptions
  ) => Promise<HawaiiCscIndependentSpendingGroup[]>;
  getNoncandidateCommitteeFunders: (
    input: { committeeId: string; electionPeriod: string; limit?: number },
    options?: HawaiiCscClientOptions
  ) => Promise<HawaiiCscAggregate[]>;
};

export type HawaiiCandidateFinanceSyncInput = {
  db: ConnectableQueryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  district?: string | null;
  sourceUrl?: string | null;
  cscClientOptions?: HawaiiCscClientOptions;
  cscClient?: Partial<HawaiiCscDataClient>;
  now?: Date;
  dryRun?: boolean;
  directMaxBreakdownsPerCategory?: number;
  outsideMaxGroups?: number;
  outsideMaxFundersPerGroup?: number;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
  trustedCommittee?: {
    committeeId: string;
    committeeName: string;
    electionPeriod: string;
    sourceUrl?: string | null;
    totalAmount?: number | null;
  };
};

type HawaiiCandidateFinanceSyncResolution =
  | HawaiiCandidateCommitteeResolution
  | ({ status: "matched" } & Omit<HawaiiCandidateCommitteeMatch, "totalAmount"> & { totalAmount?: number | null });

export type HawaiiCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  resolution: HawaiiCandidateFinanceSyncResolution;
  linkWritten: boolean;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  outsideGroupsWritten: number;
  outsideGroupBreakdownsWritten: number;
  totalReceipts: number | null;
  directContributionTotal: number | null;
  outsideSupportTotal: number | null;
  outsideOpposeTotal: number | null;
  directOccupationRowCount: number;
  directContributionSizeRowCount: number;
  outsideGroupCount: number;
  outsideFunderRowCount: number;
  skippedOutsideGroupFunderLookupCount: number;
};

const DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT = 25_000;
const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 20;
const DEFAULT_OUTSIDE_MAX_GROUPS = 20;
const DEFAULT_OUTSIDE_MAX_FUNDERS_PER_GROUP = 20;
const CANDIDATE_CONTRIBUTION_SOURCE_URL = `${HAWAII_CSC_DATA_BASE_URL}/${HAWAII_CSC_CANDIDATE_CONTRIBUTIONS_DATASET}.json`;
const NONCANDIDATE_CONTRIBUTION_SOURCE_URL = `${HAWAII_CSC_DATA_BASE_URL}/${HAWAII_CSC_NONCANDIDATE_CONTRIBUTIONS_DATASET}.json`;
const NONCANDIDATE_EXPENDITURE_SOURCE_URL = `${HAWAII_CSC_DATA_BASE_URL}/${HAWAII_CSC_NONCANDIDATE_EXPENDITURES_DATASET}.json`;

const DEFAULT_CSC_CLIENT: HawaiiCscDataClient = {
  searchAndResolveCandidateCommittee: searchAndResolveHawaiiCandidateCommittee,
  getDirectOccupationAggregates: getHawaiiCscDirectOccupationAggregates,
  getContributionSizeAggregates: getHawaiiCscContributionSizeAggregates,
  getIndependentExpenditureGroups: getHawaiiCscIndependentExpenditureGroups,
  getNoncandidateCommitteeFunders: getHawaiiCscNoncandidateCommitteeFunders,
};

type MatchedHawaiiCommitteeResolution = Extract<HawaiiCandidateFinanceSyncResolution, { status: "matched" }>;

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Hawaii finance election year: ${value}`);
  }
  return value;
}

function normalizeAiClassificationMinAmount(value: number | undefined): number {
  const normalized = value ?? DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT;
  if (!Number.isFinite(normalized) || normalized < 0) {
    throw new Error(`Invalid Hawaii finance AI classification minimum amount: ${value}`);
  }
  return normalized;
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Hawaii finance sync timestamp");
  }
  return normalized;
}

function normalizeCandidateNameForStorage(value: string): string {
  return [...normalizeHawaiiCandidateNameKeys(value)][0] ?? requireNonEmpty(value, "candidate name").replace(/\s+/g, " ").toUpperCase();
}

function mergeCscClient(client: Partial<HawaiiCscDataClient> | undefined): HawaiiCscDataClient {
  return { ...DEFAULT_CSC_CLIENT, ...(client ?? {}) };
}

function toMatchedTrustedCommittee(input: NonNullable<HawaiiCandidateFinanceSyncInput["trustedCommittee"]>): MatchedHawaiiCommitteeResolution {
  return {
    status: "matched",
    committeeId: requireNonEmpty(input.committeeId, "trusted Hawaii committee id"),
    committeeName: requireNonEmpty(input.committeeName, "trusted Hawaii committee name"),
    electionPeriod: requireNonEmpty(input.electionPeriod, "trusted Hawaii election period"),
    confidence: "exact",
    source: "csc_api",
    sourceUrl: input.sourceUrl ?? null,
    matchedSummaryRowCount: 0,
    ...(input.totalAmount !== undefined && input.totalAmount !== null ? { totalAmount: input.totalAmount } : {}),
  };
}

async function hydrateTrustedCommitteeTotals(input: {
  cscClient: HawaiiCscDataClient;
  cscClientOptions?: HawaiiCscClientOptions;
  trustedResolution: MatchedHawaiiCommitteeResolution;
  candidateName: string;
  officeScope: string;
  officeName: string;
  electionYear: number;
  district?: string | null;
}): Promise<MatchedHawaiiCommitteeResolution> {
  if (input.trustedResolution.totalAmount !== undefined && input.trustedResolution.totalAmount !== null) {
    return input.trustedResolution;
  }

  try {
    const resolved = await input.cscClient.searchAndResolveCandidateCommittee(
      {
        candidateName: input.candidateName,
        officeScope: input.officeScope,
        officeName: input.officeName,
        electionYear: input.electionYear,
        district: input.district,
      },
      input.cscClientOptions
    );
    if (
      resolved.status === "matched" &&
      resolved.committeeId.trim().toUpperCase() === input.trustedResolution.committeeId.trim().toUpperCase() &&
      resolved.electionPeriod.trim().toUpperCase() === input.trustedResolution.electionPeriod.trim().toUpperCase()
    ) {
      return {
        ...input.trustedResolution,
        totalAmount: resolved.totalAmount,
      };
    }
  } catch {
    // Totals are useful but not required; keep the trusted-link path resilient.
  }

  return input.trustedResolution;
}

function toFinanceLink(input: {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  district?: string | null;
  resolution: MatchedHawaiiCommitteeResolution;
  sourceUrl?: string | null;
  verifiedAt: Date;
}): HawaiiFinanceLinkInput {
  return {
    candidateId: requireNonEmpty(input.candidateId, "candidate id"),
    electionId: requireNonEmpty(input.electionId, "election id"),
    electionYear: input.electionYear,
    candidateNameNormalized: normalizeCandidateNameForStorage(input.candidateName),
    officeName: requireNonEmpty(input.officeName, "office name"),
    district: input.district ?? null,
    committeeId: requireNonEmpty(input.resolution.committeeId, "Hawaii committee id"),
    committeeName: requireNonEmpty(input.resolution.committeeName, "Hawaii committee name"),
    electionPeriod: requireNonEmpty(input.resolution.electionPeriod, "Hawaii election period"),
    linkStatus: "active",
    linkSource: "csc_api",
    sourceUrl: input.resolution.sourceUrl ?? input.sourceUrl ?? null,
    lastVerifiedAt: input.verifiedAt,
  };
}

function toDirectBreakdowns(input: {
  occupations: readonly HawaiiCscAggregate[];
  contributionSizes: readonly HawaiiCscAggregate[];
}): HawaiiFinanceDirectBreakdownInput[] {
  return [
    ...input.occupations.map((row) => ({
      categoryType: "occupation" as const,
      categoryName: row.categoryName,
      amount: row.amount,
      contributorCount: row.count,
      sourceUrl: CANDIDATE_CONTRIBUTION_SOURCE_URL,
    })),
    ...input.contributionSizes.map((row) => ({
      categoryType: "contribution_size" as const,
      categoryName: row.categoryName,
      amount: row.amount,
      contributorCount: row.count,
      sourceUrl: CANDIDATE_CONTRIBUTION_SOURCE_URL,
    })),
  ];
}

function toOutsideGroups(groups: readonly HawaiiCscIndependentSpendingGroup[]): HawaiiFinanceOutsideGroupInput[] {
  return groups.map((group) => ({
    committeeId: group.committeeId,
    committeeName: group.committeeName,
    supportOppose: group.supportOppose,
    amount: group.amount,
    sourceUrl: NONCANDIDATE_EXPENDITURE_SOURCE_URL,
  }));
}

function outsideBreakdownKey(input: HawaiiFinanceOutsideGroupBreakdownInput): string {
  const categoryKey =
    input.categoryType === "donor"
      ? normalizeFinanceLabel(input.categoryName, "donor")
      : input.categoryName.trim().toUpperCase();
  return `${input.committeeId.trim().toUpperCase()}\u0000${input.supportOppose}\u0000${input.categoryType}\u0000${categoryKey}`;
}

function addOutsideBreakdown(
  breakdowns: Map<string, HawaiiFinanceOutsideGroupBreakdownInput>,
  breakdown: HawaiiFinanceOutsideGroupBreakdownInput
): void {
  const key = outsideBreakdownKey(breakdown);
  const existing = breakdowns.get(key);
  if (!existing) {
    breakdowns.set(key, breakdown);
    return;
  }
  breakdowns.set(key, {
    ...existing,
    amount: Math.round((existing.amount + breakdown.amount) * 100) / 100,
    contributorCount:
      existing.contributorCount === null ||
      existing.contributorCount === undefined ||
      breakdown.contributorCount === null ||
      breakdown.contributorCount === undefined
        ? existing.contributorCount ?? breakdown.contributorCount ?? null
        : existing.contributorCount + breakdown.contributorCount,
    sourceUrl: existing.sourceUrl ?? breakdown.sourceUrl,
  });
}

function collectOutsideClassifications(
  breakdowns: Iterable<HawaiiFinanceOutsideGroupBreakdownInput>,
  minAmount: number
): Map<string, FinanceLabelClassification> {
  const classifications = new Map<string, FinanceLabelClassification>();
  for (const breakdown of breakdowns) {
    if (breakdown.categoryType !== "donor" || breakdown.amount < minAmount) {
      continue;
    }
    mergeFinanceLabelClassification(
      classifications,
      classifyFinanceLabel({ rawLabel: breakdown.categoryName, labelType: "donor" })
    );
  }
  return classifications;
}

function asClassifiableOutsideBreakdowns(breakdowns: Iterable<HawaiiFinanceOutsideGroupBreakdownInput>) {
  return [...breakdowns].map((breakdown) => ({
    committeeId: breakdown.committeeId,
    supportOppose: breakdown.supportOppose,
    categoryType: breakdown.categoryType,
    categoryName: breakdown.categoryName,
    amount: breakdown.amount,
    contributorCount: breakdown.contributorCount,
    sourceUrl: breakdown.sourceUrl,
  }));
}

async function enrichOutsideGroupIndustryBreakdowns(input: {
  db: Queryable;
  outsideGroupBreakdowns: readonly HawaiiFinanceOutsideGroupBreakdownInput[] | undefined;
  classifier: FinanceIndustryClassifier | undefined;
  aiClassificationMinAmount: number;
  dryRun: boolean;
}): Promise<{
  outsideGroupBreakdowns: HawaiiFinanceOutsideGroupBreakdownInput[] | undefined;
  classifications: FinanceLabelClassification[];
}> {
  if (!input.outsideGroupBreakdowns) {
    return { outsideGroupBreakdowns: undefined, classifications: [] };
  }

  const breakdowns = new Map<string, HawaiiFinanceOutsideGroupBreakdownInput>();
  for (const breakdown of input.outsideGroupBreakdowns) {
    if (breakdown.categoryType !== "industry") {
      addOutsideBreakdown(breakdowns, breakdown);
    }
  }

  const classifiableOutsideBreakdowns = asClassifiableOutsideBreakdowns(breakdowns.values());
  const classifications = collectOutsideClassifications(breakdowns.values(), input.aiClassificationMinAmount);
  await resolveFinanceIndustryClassifications({
    db: input.db,
    directBreakdowns: [],
    outsideBreakdowns: classifiableOutsideBreakdowns,
    classifications,
    classifier: input.classifier,
    minAmount: input.aiClassificationMinAmount,
    dryRun: input.dryRun,
  });

  const industryBreakdowns = buildFinanceIndustryBreakdownsFromClassifications({
    directBreakdowns: [],
    outsideBreakdowns: classifiableOutsideBreakdowns,
    classifications,
  });
  for (const breakdown of industryBreakdowns.outsideIndustryBreakdowns) {
    addOutsideBreakdown(breakdowns, {
      committeeId: breakdown.committeeId,
      supportOppose: breakdown.supportOppose,
      categoryType: "industry",
      categoryName: breakdown.categoryName,
      amount: breakdown.amount,
      contributorCount: breakdown.contributorCount,
      sourceUrl: breakdown.sourceUrl,
    });
  }

  return {
    outsideGroupBreakdowns: [...breakdowns.values()],
    classifications: [...classifications.values()],
  };
}

async function buildOutsideGroupBreakdowns(input: {
  cscClient: HawaiiCscDataClient;
  cscClientOptions?: HawaiiCscClientOptions;
  outsideGroups: readonly HawaiiCscIndependentSpendingGroup[];
  maxFundersPerGroup: number;
}): Promise<{
  breakdowns: HawaiiFinanceOutsideGroupBreakdownInput[];
  outsideFunderRowCount: number;
}> {
  const breakdowns: HawaiiFinanceOutsideGroupBreakdownInput[] = [];
  let outsideFunderRowCount = 0;

  for (const group of input.outsideGroups) {
    const funders = await input.cscClient.getNoncandidateCommitteeFunders(
      {
        committeeId: group.committeeId,
        electionPeriod: group.electionPeriod,
        limit: input.maxFundersPerGroup,
      },
      input.cscClientOptions
    );
    outsideFunderRowCount += funders.length;
    for (const funder of funders) {
      breakdowns.push({
        committeeId: group.committeeId,
        supportOppose: group.supportOppose,
        categoryType: "donor",
        categoryName: funder.categoryName,
        amount: funder.amount,
        contributorCount: funder.count,
        sourceUrl: NONCANDIDATE_CONTRIBUTION_SOURCE_URL,
      });
    }
  }

  return { breakdowns, outsideFunderRowCount };
}

function sumGroups(groups: readonly HawaiiCscIndependentSpendingGroup[], supportOppose: "support" | "oppose"): number {
  return (
    Math.round(
      groups
        .filter((group) => group.supportOppose === supportOppose)
        .reduce((sum, group) => sum + group.amount, 0) * 100
    ) / 100
  );
}

function toSummary(input: {
  resolution: MatchedHawaiiCommitteeResolution;
  outsideGroups: readonly HawaiiCscIndependentSpendingGroup[];
  fallbackSourceUrl?: string | null;
}): HawaiiFinanceSummaryInput {
  const totalReceipts = input.resolution.totalAmount ?? null;
  return {
    totalReceipts,
    directContributionTotal: totalReceipts,
    outsideSupportTotal: sumGroups(input.outsideGroups, "support"),
    outsideOpposeTotal: sumGroups(input.outsideGroups, "oppose"),
    sourceUrl: input.resolution.sourceUrl ?? input.fallbackSourceUrl ?? CANDIDATE_CONTRIBUTION_SOURCE_URL,
  };
}

function emptyResult(input: {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  resolution: HawaiiCandidateFinanceSyncResolution;
}): HawaiiCandidateFinanceSyncResult {
  return {
    candidateId: input.candidateId,
    electionId: input.electionId,
    electionYear: input.electionYear,
    dryRun: input.dryRun,
    resolution: input.resolution,
    linkWritten: false,
    summaryWritten: false,
    directBreakdownsWritten: 0,
    outsideGroupsWritten: 0,
    outsideGroupBreakdownsWritten: 0,
    totalReceipts: null,
    directContributionTotal: null,
    outsideSupportTotal: null,
    outsideOpposeTotal: null,
    directOccupationRowCount: 0,
    directContributionSizeRowCount: 0,
    outsideGroupCount: 0,
    outsideFunderRowCount: 0,
    skippedOutsideGroupFunderLookupCount: 0,
  };
}

export async function syncHawaiiCandidateFinance(
  input: HawaiiCandidateFinanceSyncInput
): Promise<HawaiiCandidateFinanceSyncResult> {
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const electionId = requireNonEmpty(input.electionId, "election id");
  const candidateName = requireNonEmpty(input.candidateName, "candidate name");
  const officeScope = requireNonEmpty(input.officeScope, "office scope");
  const officeName = requireNonEmpty(input.officeName, "office name");
  const electionYear = normalizeElectionYear(input.electionYear);
  const syncedAt = normalizeTimestamp(input.now);
  const dryRun = input.dryRun === true;
  const cscClient = mergeCscClient(input.cscClient);
  const aiClassificationMinAmount = normalizeAiClassificationMinAmount(input.aiClassificationMinAmount);

  const initialResolution = input.trustedCommittee
    ? toMatchedTrustedCommittee(input.trustedCommittee)
    : await cscClient.searchAndResolveCandidateCommittee(
        {
          candidateName,
          officeScope,
          officeName,
          electionYear,
          district: input.district,
        },
        input.cscClientOptions
      );

  const resolution =
    input.trustedCommittee && initialResolution.status === "matched"
      ? await hydrateTrustedCommitteeTotals({
          cscClient,
          cscClientOptions: input.cscClientOptions,
          trustedResolution: initialResolution,
          candidateName,
          officeScope,
          officeName,
          electionYear,
          district: input.district,
        })
      : initialResolution;

  if (resolution.status !== "matched") {
    return emptyResult({ candidateId, electionId, electionYear, dryRun, resolution });
  }

  const [occupations, contributionSizes, outsideGroups] = await Promise.all([
    cscClient.getDirectOccupationAggregates(
      {
        committeeId: resolution.committeeId,
        electionPeriod: resolution.electionPeriod,
        limit: input.directMaxBreakdownsPerCategory ?? DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
      },
      input.cscClientOptions
    ),
    cscClient.getContributionSizeAggregates(
      {
        committeeId: resolution.committeeId,
        electionPeriod: resolution.electionPeriod,
        limit: input.directMaxBreakdownsPerCategory ?? DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
      },
      input.cscClientOptions
    ),
    cscClient.getIndependentExpenditureGroups(
      {
        candidateName,
        electionYear,
        limit: input.outsideMaxGroups ?? DEFAULT_OUTSIDE_MAX_GROUPS,
      },
      input.cscClientOptions
    ),
  ]);

  const outsideGroupBreakdowns = await buildOutsideGroupBreakdowns({
    cscClient,
    cscClientOptions: input.cscClientOptions,
    outsideGroups,
    maxFundersPerGroup: input.outsideMaxFundersPerGroup ?? DEFAULT_OUTSIDE_MAX_FUNDERS_PER_GROUP,
  });
  const outsideIndustryFinance = await enrichOutsideGroupIndustryBreakdowns({
    db: input.db,
    outsideGroupBreakdowns: outsideGroupBreakdowns.breakdowns,
    classifier: input.financeIndustryClassifier,
    aiClassificationMinAmount,
    dryRun,
  });
  const summary = toSummary({ resolution, outsideGroups, fallbackSourceUrl: input.sourceUrl });
  const directBreakdowns = toDirectBreakdowns({ occupations, contributionSizes });
  const writerOutsideGroups = toOutsideGroups(outsideGroups);
  const link = toFinanceLink({
    candidateId,
    electionId,
    candidateName,
    electionYear,
    officeName,
    district: input.district,
    resolution,
    sourceUrl: input.sourceUrl,
    verifiedAt: syncedAt,
  });

  if (!dryRun) {
    await replaceHawaiiCandidateFinanceSnapshot({
      db: input.db,
      link,
      syncedAt,
      summary,
      directBreakdowns,
      outsideGroups: writerOutsideGroups,
      outsideGroupBreakdowns: outsideIndustryFinance.outsideGroupBreakdowns,
      classifications: outsideIndustryFinance.classifications,
    });
  }

  return {
    candidateId,
    electionId,
    electionYear,
    dryRun,
    resolution,
    linkWritten: !dryRun,
    summaryWritten: !dryRun,
    directBreakdownsWritten: dryRun ? 0 : directBreakdowns.length,
    outsideGroupsWritten: dryRun ? 0 : writerOutsideGroups.length,
    outsideGroupBreakdownsWritten: dryRun ? 0 : outsideIndustryFinance.outsideGroupBreakdowns?.length ?? 0,
    totalReceipts: summary.totalReceipts ?? null,
    directContributionTotal: summary.directContributionTotal ?? null,
    outsideSupportTotal: summary.outsideSupportTotal ?? null,
    outsideOpposeTotal: summary.outsideOpposeTotal ?? null,
    directOccupationRowCount: occupations.length,
    directContributionSizeRowCount: contributionSizes.length,
    outsideGroupCount: outsideGroups.length,
    outsideFunderRowCount: outsideGroupBreakdowns.outsideFunderRowCount,
    skippedOutsideGroupFunderLookupCount: 0,
  };
}
