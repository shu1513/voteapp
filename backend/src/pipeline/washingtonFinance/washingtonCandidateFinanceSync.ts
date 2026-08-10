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
  normalizeWashingtonCandidateNameKeys,
  searchAndResolveWashingtonCandidateCommittee,
  type WashingtonCandidateCommitteeResolution,
} from "./washingtonCandidateCommitteeResolver.js";
import { toWashingtonPdcOfficeSearchInput } from "./washingtonFinanceEligibleOffices.js";
import {
  replaceWashingtonCandidateFinanceSnapshot,
  type WashingtonFinanceDirectBreakdownInput,
  type WashingtonFinanceLinkInput,
  type WashingtonFinanceOutsideGroupBreakdownInput,
  type WashingtonFinanceOutsideGroupInput,
  type WashingtonFinanceSummaryInput,
} from "./washingtonFinanceWriter.js";
import {
  getWashingtonPdcContributionSizeAggregates,
  getWashingtonPdcDirectOccupationAggregates,
  getWashingtonPdcIndependentExpenditureGroups,
  getWashingtonPdcSponsorOrganizationFunders,
  getWashingtonPdcSponsorSummaryByName,
  type WashingtonPdcAggregate,
  type WashingtonPdcCandidateSummary,
  type WashingtonPdcClientOptions,
  type WashingtonPdcIndependentSpendingGroup,
} from "./washingtonPdcClient.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & Pick<Pool, "connect">;

type WashingtonPdcDataClient = {
  searchAndResolveCandidateCommittee: (
    input: {
      candidateName: string;
      officeScope: string;
      officeName: string;
      electionYear: number;
      legislativeDistrict?: string | null;
    },
    options?: WashingtonPdcClientOptions
  ) => Promise<WashingtonCandidateCommitteeResolution>;
  getDirectOccupationAggregates: (
    input: { filerId?: string | null; committeeId?: string | null; electionYear: number; limit?: number },
    options?: WashingtonPdcClientOptions
  ) => Promise<WashingtonPdcAggregate[]>;
  getContributionSizeAggregates: (
    input: { filerId?: string | null; committeeId?: string | null; electionYear: number; limit?: number },
    options?: WashingtonPdcClientOptions
  ) => Promise<WashingtonPdcAggregate[]>;
  getIndependentExpenditureGroups: (
    input: {
      candidateName: string;
      electionYear: number;
      office?: string | null;
      legislativeDistrict?: string | null;
      candidateFilerId?: string | null;
      candidateCommitteeId?: string | null;
      limit?: number;
    },
    options?: WashingtonPdcClientOptions
  ) => Promise<WashingtonPdcIndependentSpendingGroup[]>;
  getSponsorSummaryByName: (
    input: { sponsorName: string; electionYear: number; limit?: number },
    options?: WashingtonPdcClientOptions
  ) => Promise<WashingtonPdcCandidateSummary[]>;
  getSponsorOrganizationFunders: (
    input: { filerId?: string | null; committeeId?: string | null; electionYear: number; limit?: number },
    options?: WashingtonPdcClientOptions
  ) => Promise<WashingtonPdcAggregate[]>;
};

export type WashingtonCandidateFinanceSyncInput = {
  db: ConnectableQueryable;
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeScope: string;
  officeName: string;
  legislativeDistrict?: string | null;
  sourceUrl?: string | null;
  pdcClientOptions?: WashingtonPdcClientOptions;
  pdcClient?: Partial<WashingtonPdcDataClient>;
  now?: Date;
  dryRun?: boolean;
  directMaxBreakdownsPerCategory?: number;
  outsideMaxGroups?: number;
  // Display cap on persisted donor rows per (sponsor, direction);
  // classification always sees every funder.
  outsideMaxFundersPerGroup?: number;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
  trustedCommittee?: {
    filerId: string;
    committeeId: string;
    committeeName: string;
    candidacyId?: string | null;
    sourceUrl?: string | null;
    contributionsAmount?: number | null;
    expendituresAmount?: number | null;
  };
};

export type WashingtonCandidateFinanceSyncResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  resolution: WashingtonCandidateCommitteeResolution;
  linkWritten: boolean;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  outsideGroupsWritten: number;
  outsideGroupBreakdownsWritten: number;
  totalReceipts: number | null;
  directContributionTotal: number | null;
  totalDisbursements: number | null;
  outsideSupportTotal: number | null;
  outsideOpposeTotal: number | null;
  // Sum of the fetched C6 groups per direction. Drift against the headline
  // outside totals (which come from PDC's summary fields) is expected filing
  // skew; large drift means the group explanation is incomplete.
  outsideSupportGroupTotal: number | null;
  outsideOpposeGroupTotal: number | null;
  directOccupationRowCount: number;
  directContributionSizeRowCount: number;
  outsideGroupCount: number;
  outsideFunderRowCount: number;
  skippedOutsideGroupFunderLookupCount: number;
};

const DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT = 25_000;
const DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY = 20;
const DEFAULT_OUTSIDE_MAX_GROUPS = 20;
// Display cap on PERSISTED donor rows per (sponsor, direction), applied AFTER
// classification so a >cap-funder group still gets industry totals built from
// every funder. Industry rows are bounded by the slug set and never capped.
const DEFAULT_OUTSIDE_MAX_FUNDERS_PER_GROUP = 20;

const DEFAULT_PDC_CLIENT: WashingtonPdcDataClient = {
  searchAndResolveCandidateCommittee: searchAndResolveWashingtonCandidateCommittee,
  getDirectOccupationAggregates: getWashingtonPdcDirectOccupationAggregates,
  getContributionSizeAggregates: getWashingtonPdcContributionSizeAggregates,
  getIndependentExpenditureGroups: getWashingtonPdcIndependentExpenditureGroups,
  getSponsorSummaryByName: getWashingtonPdcSponsorSummaryByName,
  getSponsorOrganizationFunders: getWashingtonPdcSponsorOrganizationFunders,
};

type MatchedWashingtonCommitteeResolution = Extract<WashingtonCandidateCommitteeResolution, { status: "matched" }>;

type WashingtonSponsorCommitteeResolution =
  | { status: "matched"; filerId: string; committeeId: string; committeeName: string; sourceUrl: string | null }
  | { status: "skipped"; reason: "no_match" | "ambiguous" };

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 2000 || value > 2100) {
    throw new Error(`Invalid Washington finance election year: ${value}`);
  }
  return value;
}

function normalizeTimestamp(value: Date | undefined): Date {
  const normalized = value ?? new Date();
  if (Number.isNaN(normalized.getTime())) {
    throw new Error("Invalid Washington finance sync timestamp");
  }
  return normalized;
}

function normalizeAiClassificationMinAmount(value: number | undefined): number {
  const normalized = value ?? DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT;
  if (!Number.isFinite(normalized) || normalized < 0) {
    throw new Error(`Invalid Washington finance AI classification minimum amount: ${value}`);
  }
  return normalized;
}

function normalizeCandidateNameForStorage(value: string): string {
  return [...normalizeWashingtonCandidateNameKeys(value)][0] ?? requireNonEmpty(value, "candidate name").replace(/\s+/g, " ").toUpperCase();
}

function normalizeMaxFundersPerGroup(value: number | undefined): number {
  const normalized = value ?? DEFAULT_OUTSIDE_MAX_FUNDERS_PER_GROUP;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Washington finance outsideMaxFundersPerGroup: ${value}`);
  }
  return normalized;
}

function capDonorBreakdowns(
  breakdowns: readonly WashingtonFinanceOutsideGroupBreakdownInput[],
  maxDonorsPerGroup: number
): WashingtonFinanceOutsideGroupBreakdownInput[] {
  const donorsByGroup = new Map<string, WashingtonFinanceOutsideGroupBreakdownInput[]>();
  for (const breakdown of breakdowns) {
    if (breakdown.categoryType !== "donor") {
      continue;
    }
    const key = [breakdown.sponsorId.trim().toUpperCase(), breakdown.supportOppose].join(" | ");
    const list = donorsByGroup.get(key) ?? [];
    list.push(breakdown);
    donorsByGroup.set(key, list);
  }
  const kept = new Set<WashingtonFinanceOutsideGroupBreakdownInput>();
  for (const list of donorsByGroup.values()) {
    for (const donor of list
      .sort(
        (left, right) => right.amount - left.amount || left.categoryName.localeCompare(right.categoryName)
      )
      .slice(0, maxDonorsPerGroup)) {
      kept.add(donor);
    }
  }
  return breakdowns.filter((breakdown) => breakdown.categoryType !== "donor" || kept.has(breakdown));
}

function mergePdcClient(client: Partial<WashingtonPdcDataClient> | undefined): WashingtonPdcDataClient {
  return { ...DEFAULT_PDC_CLIENT, ...(client ?? {}) };
}

function toMatchedTrustedCommittee(input: NonNullable<WashingtonCandidateFinanceSyncInput["trustedCommittee"]>): MatchedWashingtonCommitteeResolution {
  return {
    status: "matched",
    filerId: requireNonEmpty(input.filerId, "trusted Washington filer id"),
    committeeId: requireNonEmpty(input.committeeId, "trusted Washington committee id"),
    committeeName: requireNonEmpty(input.committeeName, "trusted Washington committee name"),
    ...(input.candidacyId?.trim() ? { candidacyId: input.candidacyId.trim() } : {}),
    confidence: "exact",
    source: "pdc_api",
    sourceUrl: input.sourceUrl ?? null,
    matchedSummaryRowCount: 0,
    ...(input.contributionsAmount !== undefined && input.contributionsAmount !== null
      ? { contributionsAmount: input.contributionsAmount }
      : {}),
    ...(input.expendituresAmount !== undefined && input.expendituresAmount !== null
      ? { expendituresAmount: input.expendituresAmount }
      : {}),
  };
}

async function hydrateTrustedCommitteeTotals(input: {
  pdcClient: WashingtonPdcDataClient;
  pdcClientOptions?: WashingtonPdcClientOptions;
  trustedResolution: MatchedWashingtonCommitteeResolution;
  candidateName: string;
  officeScope: string;
  officeName: string;
  electionYear: number;
  legislativeDistrict?: string | null;
}): Promise<MatchedWashingtonCommitteeResolution> {
  // Always hydrate: the trusted-committee input cannot carry the summary IE
  // fields, and the headline outside totals read them. A search failure here
  // propagates and fails the sync (the batch retries the row later) — the old
  // swallow-and-continue path let the group-sum fallback overwrite PDC's own
  // outside totals whenever the summary lookup blipped.
  const resolved = await input.pdcClient.searchAndResolveCandidateCommittee(
    {
      candidateName: input.candidateName,
      officeScope: input.officeScope,
      officeName: input.officeName,
      electionYear: input.electionYear,
      legislativeDistrict: input.legislativeDistrict,
    },
    input.pdcClientOptions
  );
  if (
    resolved.status === "matched" &&
    resolved.filerId.trim().toUpperCase() === input.trustedResolution.filerId.trim().toUpperCase() &&
    resolved.committeeId.trim().toUpperCase() === input.trustedResolution.committeeId.trim().toUpperCase()
  ) {
    return {
      ...input.trustedResolution,
      ...(resolved.contributionsAmount !== undefined ? { contributionsAmount: resolved.contributionsAmount } : {}),
      ...(resolved.expendituresAmount !== undefined ? { expendituresAmount: resolved.expendituresAmount } : {}),
      ...(resolved.independentExpendituresForAmount !== undefined
        ? { independentExpendituresForAmount: resolved.independentExpendituresForAmount }
        : {}),
      ...(resolved.independentExpendituresAgainstAmount !== undefined
        ? { independentExpendituresAgainstAmount: resolved.independentExpendituresAgainstAmount }
        : {}),
    };
  }

  // Resolution landed elsewhere or nowhere (withdrawn candidate, renamed
  // committee): proceed with the trusted link and no summary totals — the
  // headline then falls back to the hard-ID C6 group sums.
  return input.trustedResolution;
}

function toFinanceLink(input: {
  candidateId: string;
  electionId: string;
  candidateName: string;
  electionYear: number;
  officeName: string;
  legislativeDistrict?: string | null;
  resolution: MatchedWashingtonCommitteeResolution;
  sourceUrl?: string | null;
  verifiedAt: Date;
}): WashingtonFinanceLinkInput {
  return {
    candidateId: requireNonEmpty(input.candidateId, "candidate id"),
    electionId: requireNonEmpty(input.electionId, "election id"),
    electionYear: input.electionYear,
    candidateNameNormalized: normalizeCandidateNameForStorage(input.candidateName),
    officeName: requireNonEmpty(input.officeName, "office name"),
    district: input.legislativeDistrict ?? null,
    filerId: requireNonEmpty(input.resolution.filerId, "Washington filer id"),
    committeeId: requireNonEmpty(input.resolution.committeeId, "Washington committee id"),
    committeeName: requireNonEmpty(input.resolution.committeeName, "Washington committee name"),
    candidacyId: input.resolution.candidacyId ?? null,
    linkStatus: "active",
    linkSource: "pdc_api",
    sourceUrl: input.resolution.sourceUrl ?? input.sourceUrl ?? null,
    lastVerifiedAt: input.verifiedAt,
  };
}

function toDirectBreakdowns(input: {
  occupations: readonly WashingtonPdcAggregate[];
  contributionSizes: readonly WashingtonPdcAggregate[];
}): WashingtonFinanceDirectBreakdownInput[] {
  return [
    ...input.occupations.map((row) => ({
      categoryType: "occupation" as const,
      categoryName: row.categoryName,
      amount: row.amount,
      contributorCount: row.count,
      sourceUrl: row.sourceUrl ?? null,
    })),
    ...input.contributionSizes.map((row) => ({
      categoryType: "contribution_size" as const,
      categoryName: row.categoryName,
      amount: row.amount,
      contributorCount: row.count,
      sourceUrl: row.sourceUrl ?? null,
    })),
  ];
}

function toOutsideGroups(groups: readonly WashingtonPdcIndependentSpendingGroup[]): WashingtonFinanceOutsideGroupInput[] {
  return groups.map((group) => ({
    sponsorId: group.sponsorId,
    sponsorName: group.sponsorName,
    supportOppose: group.supportOppose,
    amount: group.amount,
    sourceUrl: group.sourceUrl ?? null,
  }));
}

function resolveSponsorCommittee(summaries: readonly WashingtonPdcCandidateSummary[]): WashingtonSponsorCommitteeResolution {
  const usable = new Map<string, WashingtonPdcCandidateSummary>();
  for (const summary of summaries) {
    const filerId = summary.filerId.trim();
    const committeeId = summary.committeeId?.trim();
    if (!filerId || !committeeId) {
      continue;
    }
    usable.set(`${filerId}\u0000${committeeId}`, summary);
  }
  if (usable.size === 0) {
    return { status: "skipped", reason: "no_match" };
  }
  if (usable.size > 1) {
    return { status: "skipped", reason: "ambiguous" };
  }
  const summary = [...usable.values()][0];
  const filerId = summary.filerId.trim();
  const committeeId = summary.committeeId?.trim() ?? "";
  return {
    status: "matched",
    filerId,
    committeeId,
    committeeName: summary.filerName,
    sourceUrl: summary.sourceUrl ?? null,
  };
}

function donorBreakdownKey(input: WashingtonFinanceOutsideGroupBreakdownInput): string {
  const categoryKey =
    input.categoryType === "donor"
      ? normalizeFinanceLabel(input.categoryName, "donor")
      : input.categoryName.trim().toUpperCase();
  return `${input.sponsorId.trim().toUpperCase()}\u0000${input.supportOppose}\u0000${input.categoryType}\u0000${categoryKey}`;
}

function addOutsideBreakdown(
  breakdowns: Map<string, WashingtonFinanceOutsideGroupBreakdownInput>,
  breakdown: WashingtonFinanceOutsideGroupBreakdownInput
): void {
  const key = donorBreakdownKey(breakdown);
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
  breakdowns: Iterable<WashingtonFinanceOutsideGroupBreakdownInput>,
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

function asClassifiableOutsideBreakdowns(breakdowns: Iterable<WashingtonFinanceOutsideGroupBreakdownInput>) {
  return [...breakdowns].map((breakdown) => ({
    committeeId: breakdown.sponsorId,
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
  outsideGroupBreakdowns: readonly WashingtonFinanceOutsideGroupBreakdownInput[] | undefined;
  classifier: FinanceIndustryClassifier | undefined;
  aiClassificationMinAmount: number;
  maxDonorBreakdownsPerGroup: number;
  dryRun: boolean;
}): Promise<{
  outsideGroupBreakdowns: WashingtonFinanceOutsideGroupBreakdownInput[] | undefined;
  classifications: FinanceLabelClassification[];
}> {
  if (!input.outsideGroupBreakdowns) {
    return { outsideGroupBreakdowns: undefined, classifications: [] };
  }

  const breakdowns = new Map<string, WashingtonFinanceOutsideGroupBreakdownInput>();
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
      sponsorId: breakdown.committeeId,
      supportOppose: breakdown.supportOppose,
      categoryType: "industry",
      categoryName: breakdown.categoryName,
      amount: breakdown.amount,
      contributorCount: breakdown.contributorCount,
      sourceUrl: breakdown.sourceUrl,
    });
  }

  return {
    // Capped only HERE, after every funder fed the classifications and the
    // rebuilt industry rows above.
    outsideGroupBreakdowns: capDonorBreakdowns([...breakdowns.values()], input.maxDonorBreakdownsPerGroup),
    classifications: [...classifications.values()],
  };
}

async function buildOutsideGroupBreakdowns(input: {
  pdcClient: WashingtonPdcDataClient;
  pdcClientOptions?: WashingtonPdcClientOptions;
  outsideGroups: readonly WashingtonPdcIndependentSpendingGroup[];
  electionYear: number;
}): Promise<{
  breakdowns: WashingtonFinanceOutsideGroupBreakdownInput[];
  outsideFunderRowCount: number;
  skippedOutsideGroupFunderLookupCount: number;
}> {
  const breakdowns: WashingtonFinanceOutsideGroupBreakdownInput[] = [];
  let outsideFunderRowCount = 0;
  let skippedOutsideGroupFunderLookupCount = 0;

  for (const group of input.outsideGroups) {
    const sponsorSummaries = await input.pdcClient.getSponsorSummaryByName(
      { sponsorName: group.sponsorName, electionYear: input.electionYear, limit: 20 },
      input.pdcClientOptions
    );
    const sponsorResolution = resolveSponsorCommittee(sponsorSummaries);
    if (sponsorResolution.status !== "matched") {
      skippedOutsideGroupFunderLookupCount += 1;
      continue;
    }

    // No limit: every funder must reach the industry rebuild downstream.
    const funders = await input.pdcClient.getSponsorOrganizationFunders(
      {
        filerId: sponsorResolution.filerId,
        committeeId: sponsorResolution.committeeId,
        electionYear: input.electionYear,
      },
      input.pdcClientOptions
    );
    outsideFunderRowCount += funders.length;
    for (const funder of funders) {
      breakdowns.push({
        sponsorId: group.sponsorId,
        supportOppose: group.supportOppose,
        categoryType: "donor",
        categoryName: funder.categoryName,
        amount: funder.amount,
        contributorCount: funder.count,
        sourceUrl: funder.sourceUrl ?? sponsorResolution.sourceUrl ?? group.sourceUrl ?? null,
      });
    }
  }

  return { breakdowns, outsideFunderRowCount, skippedOutsideGroupFunderLookupCount };
}

function sumGroups(groups: readonly WashingtonPdcIndependentSpendingGroup[], supportOppose: "support" | "oppose"): number {
  return Math.round(groups.filter((group) => group.supportOppose === supportOppose).reduce((sum, group) => sum + group.amount, 0) * 100) / 100;
}

function toSummary(input: {
  resolution: MatchedWashingtonCommitteeResolution;
  outsideGroups: readonly WashingtonPdcIndependentSpendingGroup[];
  fallbackSourceUrl?: string | null;
}): WashingtonFinanceSummaryInput {
  // PDC candidate summaries expose one contribution total; they do not split
  // total receipts from direct donor receipts in this API path.
  const totalReceipts = input.resolution.contributionsAmount ?? null;
  return {
    totalReceipts,
    directContributionTotal: totalReceipts,
    totalDisbursements: input.resolution.expendituresAmount ?? null,
    // The summary dataset's per-candidate IE fields are PDC's own totals and
    // the authority for the headline numbers; summing the fetched top-N C6
    // groups undercounts. Group sums remain only as a fallback when the
    // summary row carried no IE fields (e.g. trusted-link hydration failed).
    outsideSupportTotal: input.resolution.independentExpendituresForAmount ?? sumGroups(input.outsideGroups, "support"),
    outsideOpposeTotal: input.resolution.independentExpendituresAgainstAmount ?? sumGroups(input.outsideGroups, "oppose"),
    sourceUrl: input.resolution.sourceUrl ?? input.fallbackSourceUrl ?? null,
  };
}

function emptyResult(input: {
  candidateId: string;
  electionId: string;
  electionYear: number;
  dryRun: boolean;
  resolution: WashingtonCandidateCommitteeResolution;
}): WashingtonCandidateFinanceSyncResult {
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
    totalDisbursements: null,
    outsideSupportTotal: null,
    outsideOpposeTotal: null,
    outsideSupportGroupTotal: null,
    outsideOpposeGroupTotal: null,
    directOccupationRowCount: 0,
    directContributionSizeRowCount: 0,
    outsideGroupCount: 0,
    outsideFunderRowCount: 0,
    skippedOutsideGroupFunderLookupCount: 0,
  };
}

export async function syncWashingtonCandidateFinance(
  input: WashingtonCandidateFinanceSyncInput
): Promise<WashingtonCandidateFinanceSyncResult> {
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const electionId = requireNonEmpty(input.electionId, "election id");
  const candidateName = requireNonEmpty(input.candidateName, "candidate name");
  const officeScope = requireNonEmpty(input.officeScope, "office scope");
  const officeName = requireNonEmpty(input.officeName, "office name");
  const electionYear = normalizeElectionYear(input.electionYear);
  const syncedAt = normalizeTimestamp(input.now);
  const dryRun = input.dryRun === true;
  const pdcClient = mergePdcClient(input.pdcClient);
  const aiClassificationMinAmount = normalizeAiClassificationMinAmount(input.aiClassificationMinAmount);
  const officeSearch = toWashingtonPdcOfficeSearchInput({
    officeScope,
    officeCanonicalName: officeName,
    legislativeDistrict: input.legislativeDistrict,
  });

  const initialResolution = input.trustedCommittee
    ? toMatchedTrustedCommittee(input.trustedCommittee)
    : await pdcClient.searchAndResolveCandidateCommittee(
        {
          candidateName,
          officeScope,
          officeName,
          electionYear,
          legislativeDistrict: input.legislativeDistrict,
        },
        input.pdcClientOptions
      );

  const resolution =
    input.trustedCommittee && initialResolution.status === "matched"
      ? await hydrateTrustedCommitteeTotals({
          pdcClient,
          pdcClientOptions: input.pdcClientOptions,
          trustedResolution: initialResolution,
          candidateName,
          officeScope,
          officeName,
          electionYear,
          legislativeDistrict: input.legislativeDistrict,
        })
      : initialResolution;

  if (resolution.status !== "matched") {
    return emptyResult({ candidateId, electionId, electionYear, dryRun, resolution });
  }

  const [occupations, contributionSizes, outsideGroups] = await Promise.all([
    pdcClient.getDirectOccupationAggregates(
      {
        filerId: resolution.filerId,
        committeeId: resolution.committeeId,
        electionYear,
        limit: input.directMaxBreakdownsPerCategory ?? DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
      },
      input.pdcClientOptions
    ),
    pdcClient.getContributionSizeAggregates(
      {
        filerId: resolution.filerId,
        committeeId: resolution.committeeId,
        electionYear,
        limit: input.directMaxBreakdownsPerCategory ?? DEFAULT_MAX_BREAKDOWNS_PER_CATEGORY,
      },
      input.pdcClientOptions
    ),
    pdcClient.getIndependentExpenditureGroups(
      {
        candidateName,
        electionYear,
        office: officeSearch?.pdcOffice ?? null,
        legislativeDistrict: officeSearch?.legislativeDistrict ?? null,
        // Hard IDs from the matched committee filter the C6 rows exactly;
        // the name/office inputs above only serve the unlinked fallback.
        candidateFilerId: resolution.filerId,
        candidateCommitteeId: resolution.committeeId,
        limit: input.outsideMaxGroups ?? DEFAULT_OUTSIDE_MAX_GROUPS,
      },
      input.pdcClientOptions
    ),
  ]);

  const outsideGroupBreakdowns = await buildOutsideGroupBreakdowns({
    pdcClient,
    pdcClientOptions: input.pdcClientOptions,
    outsideGroups,
    electionYear,
  });
  const outsideIndustryFinance = await enrichOutsideGroupIndustryBreakdowns({
    db: input.db,
    outsideGroupBreakdowns: outsideGroupBreakdowns.breakdowns,
    classifier: input.financeIndustryClassifier,
    aiClassificationMinAmount,
    maxDonorBreakdownsPerGroup: normalizeMaxFundersPerGroup(input.outsideMaxFundersPerGroup),
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
    legislativeDistrict: input.legislativeDistrict,
    resolution,
    sourceUrl: input.sourceUrl,
    verifiedAt: syncedAt,
  });

  if (!dryRun) {
    await replaceWashingtonCandidateFinanceSnapshot({
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
    totalDisbursements: summary.totalDisbursements ?? null,
    outsideSupportTotal: summary.outsideSupportTotal ?? null,
    outsideOpposeTotal: summary.outsideOpposeTotal ?? null,
    outsideSupportGroupTotal: sumGroups(outsideGroups, "support"),
    outsideOpposeGroupTotal: sumGroups(outsideGroups, "oppose"),
    directOccupationRowCount: occupations.length,
    directContributionSizeRowCount: contributionSizes.length,
    outsideGroupCount: outsideGroups.length,
    outsideFunderRowCount: outsideGroupBreakdowns.outsideFunderRowCount,
    skippedOutsideGroupFunderLookupCount: outsideGroupBreakdowns.skippedOutsideGroupFunderLookupCount,
  };
}
