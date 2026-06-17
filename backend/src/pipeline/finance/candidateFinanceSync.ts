import type { Pool, PoolClient } from "pg";

import { type OpenFecClientOptions } from "../presidential/openFecClient.js";
import {
  type OpenFecFinanceAggregate,
  type OpenFecFinanceCandidateTotals,
  type OpenFecFinanceCommittee,
  type OpenFecOutsideSpendingGroup,
  type OpenFecOutsideSpendingTotals,
  getCandidateTotals,
  getCommitteeAggregatesByEmployer,
  getCommitteeAggregatesByOccupation,
  getCommitteeAggregatesBySize,
  getOutsideSpendingTotalsByCandidate,
  listCandidateCommittees,
  listOutsideSpendingGroupsByCandidate,
} from "./openFecFinanceClient.js";
import {
  type FinanceClassificationConfidence,
  type FinanceClassificationSource,
  type FinanceLabelClassification,
  type FinanceLabelType,
  classifyFinanceLabel,
  normalizeFinanceLabel,
  type FinanceIndustrySlug,
} from "./financeLabelClassifier.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type ConnectableQueryable = Queryable & {
  connect?: () => Promise<PoolClient>;
};

type DirectBreakdownCategoryType = "occupation" | "employer" | "industry" | "contribution_size";
type OutsideBreakdownCategoryType = "occupation" | "employer" | "industry";

export type CandidateFinanceSyncInput = {
  db: Queryable;
  fecCandidateId: string;
  electionYear: number;
  openFecOptions: OpenFecClientOptions;
  now?: Date;
  perPage?: number;
  outsideGroupLimit?: number;
  includeOutside?: boolean;
  dryRun?: boolean;
  fecClient?: CandidateFinanceSyncFecClient;
  financeIndustryClassifier?: CandidateFinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
};

export type CandidateFinanceSyncFecClient = {
  getCandidateTotals: typeof getCandidateTotals;
  listCandidateCommittees: typeof listCandidateCommittees;
  getCommitteeAggregatesByEmployer: typeof getCommitteeAggregatesByEmployer;
  getCommitteeAggregatesByOccupation: typeof getCommitteeAggregatesByOccupation;
  getCommitteeAggregatesBySize: typeof getCommitteeAggregatesBySize;
  getOutsideSpendingTotalsByCandidate: typeof getOutsideSpendingTotalsByCandidate;
  listOutsideSpendingGroupsByCandidate: typeof listOutsideSpendingGroupsByCandidate;
};

export type CandidateFinanceIndustryClassificationCandidate = {
  rawLabel: string;
  labelType: Extract<FinanceLabelType, "employer" | "donor">;
  normalizedLabel: string;
  amount: number;
};

export type CandidateFinanceIndustryClassifier = (input: {
  labels: readonly CandidateFinanceIndustryClassificationCandidate[];
}) => Promise<FinanceLabelClassification[]>;

export type CandidateFinanceSyncResult = {
  fecCandidateId: string;
  electionYear: number;
  dryRun: boolean;
  directCommitteeCount: number;
  summaryWritten: boolean;
  directBreakdownsWritten: number;
  industryBreakdownsWritten: number;
  classificationsWritten: number;
  outsideIncluded: boolean;
  outsideGroupsWritten: number;
  outsideGroupBreakdownsWritten: number;
  outsideSupportTotal: number | null;
  outsideOpposeTotal: number | null;
};

type DirectBreakdown = {
  categoryType: DirectBreakdownCategoryType;
  categoryName: string;
  amount: number;
  contributorCount: number | null;
  sourceUrl: string | null;
};

type OutsideGroupBreakdown = {
  committeeId: string;
  supportOppose: "support" | "oppose";
  categoryType: OutsideBreakdownCategoryType;
  categoryName: string;
  amount: number;
  contributorCount: number | null;
  sourceUrl: string | null;
};

const DEFAULT_FINANCE_SYNC_PER_PAGE = 20;
const DEFAULT_OUTSIDE_GROUP_LIMIT = 10;
const DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT = 100_000;

const DEFAULT_FEC_CLIENT: CandidateFinanceSyncFecClient = {
  getCandidateTotals,
  listCandidateCommittees,
  getCommitteeAggregatesByEmployer,
  getCommitteeAggregatesByOccupation,
  getCommitteeAggregatesBySize,
  getOutsideSpendingTotalsByCandidate,
  listOutsideSpendingGroupsByCandidate,
};

function normalizeFecCandidateId(value: string): string {
  const normalized = value.trim().toUpperCase();
  if (!/^[HPS][0-9A-Z]{8}$/.test(normalized)) {
    throw new Error(`Invalid FEC candidate ID: ${value}`);
  }
  return normalized;
}

function normalizeElectionYear(value: number): number {
  if (!Number.isInteger(value) || value < 1970 || value > 2100) {
    throw new Error(`Invalid election year: ${value}`);
  }
  return value;
}

function normalizePerPage(value: number | undefined): number {
  const normalized = value ?? DEFAULT_FINANCE_SYNC_PER_PAGE;
  if (!Number.isInteger(normalized) || normalized <= 0 || normalized > 100) {
    throw new Error(`Finance sync perPage must be an integer between 1 and 100`);
  }
  return normalized;
}

function normalizeOutsideGroupLimit(value: number | undefined): number {
  const normalized = value ?? DEFAULT_OUTSIDE_GROUP_LIMIT;
  if (!Number.isInteger(normalized) || normalized <= 0 || normalized > 100) {
    throw new Error(`Finance sync outsideGroupLimit must be an integer between 1 and 100`);
  }
  return normalized;
}

function normalizeAiClassificationMinAmount(value: number | undefined): number {
  const normalized = value ?? DEFAULT_AI_CLASSIFICATION_MIN_AMOUNT;
  if (!Number.isFinite(normalized) || normalized < 0) {
    throw new Error("Finance sync aiClassificationMinAmount must be a nonnegative number");
  }
  return normalized;
}

function isDirectCampaignCommittee(committee: OpenFecFinanceCommittee): boolean {
  const designation = committee.designation?.trim().toUpperCase();
  return designation === "P" || designation === "A";
}

function aggregateKey(categoryType: DirectBreakdownCategoryType | OutsideBreakdownCategoryType, categoryName: string): string {
  if (categoryType === "employer") {
    return `${categoryType}\u0000${normalizeFinanceLabel(categoryName, "employer")}`;
  }
  if (categoryType === "occupation") {
    return `${categoryType}\u0000${normalizeFinanceLabel(categoryName, "occupation")}`;
  }
  return `${categoryType}\u0000${categoryName.trim().toUpperCase()}`;
}

function outsideBreakdownKey(input: OutsideGroupBreakdown): string {
  return `${input.committeeId}\u0000${input.supportOppose}\u0000${aggregateKey(input.categoryType, input.categoryName)}`;
}

function toNullableCount(value: number | undefined): number | null {
  if (value === undefined || !Number.isFinite(value) || value < 0) {
    return null;
  }
  return Math.trunc(value);
}

function combineCounts(left: number | null, right: number | null): number | null {
  if (left === null && right === null) {
    return null;
  }
  return (left ?? 0) + (right ?? 0);
}

function addBreakdown(breakdowns: Map<string, DirectBreakdown>, input: DirectBreakdown): void {
  const categoryName = input.categoryName.trim();
  if (categoryName.length === 0 || input.amount < 0) {
    return;
  }

  const key = aggregateKey(input.categoryType, categoryName);
  const existing = breakdowns.get(key);
  if (!existing) {
    breakdowns.set(key, { ...input, categoryName });
    return;
  }

  breakdowns.set(key, {
    ...existing,
    amount: existing.amount + input.amount,
    contributorCount: combineCounts(existing.contributorCount, input.contributorCount),
    sourceUrl: existing.sourceUrl ?? input.sourceUrl,
  });
}

function addOutsideBreakdown(breakdowns: Map<string, OutsideGroupBreakdown>, input: OutsideGroupBreakdown): void {
  const categoryName = input.categoryName.trim();
  if (categoryName.length === 0 || input.amount < 0) {
    return;
  }

  const normalized = { ...input, categoryName };
  const key = outsideBreakdownKey(normalized);
  const existing = breakdowns.get(key);
  if (!existing) {
    breakdowns.set(key, normalized);
    return;
  }

  breakdowns.set(key, {
    ...existing,
    amount: existing.amount + normalized.amount,
    contributorCount: combineCounts(existing.contributorCount, normalized.contributorCount),
    sourceUrl: existing.sourceUrl ?? normalized.sourceUrl,
  });
}

function addAggregateBreakdowns(
  breakdowns: Map<string, DirectBreakdown>,
  categoryType: Exclude<DirectBreakdownCategoryType, "industry">,
  aggregates: readonly OpenFecFinanceAggregate[]
): void {
  for (const aggregate of aggregates) {
    addBreakdown(breakdowns, {
      categoryType,
      categoryName: aggregate.label,
      amount: aggregate.amount,
      contributorCount: toNullableCount(aggregate.count),
      sourceUrl: null,
    });
  }
}

function addOutsideAggregateBreakdowns(input: {
  breakdowns: Map<string, OutsideGroupBreakdown>;
  group: OpenFecOutsideSpendingGroup;
  categoryType: Exclude<OutsideBreakdownCategoryType, "industry">;
  aggregates: readonly OpenFecFinanceAggregate[];
}): void {
  for (const aggregate of input.aggregates) {
    addOutsideBreakdown(input.breakdowns, {
      committeeId: input.group.committeeId,
      supportOppose: input.group.supportOppose,
      categoryType: input.categoryType,
      categoryName: aggregate.label,
      amount: aggregate.amount,
      contributorCount: toNullableCount(aggregate.count),
      sourceUrl: null,
    });
  }
}

function classificationKey(labelType: FinanceLabelType, normalizedLabel: string): string {
  return `${labelType}\u0000${normalizedLabel}`;
}

function collectClassifications(
  classifications: Map<string, FinanceLabelClassification>,
  labelType: FinanceLabelType,
  aggregates: readonly OpenFecFinanceAggregate[]
): FinanceLabelClassification[] {
  const collected: FinanceLabelClassification[] = [];
  for (const aggregate of aggregates) {
    const classification = classifyFinanceLabel({ rawLabel: aggregate.label, labelType });
    if (classification.normalizedLabel.length === 0) {
      continue;
    }
    const key = classificationKey(classification.labelType, classification.normalizedLabel);
    if (!classifications.has(key)) {
      classifications.set(key, classification);
    }
    collected.push(classification);
  }
  return collected;
}

async function fetchDirectBreakdowns(input: {
  fecClient: CandidateFinanceSyncFecClient;
  committees: readonly OpenFecFinanceCommittee[];
  electionYear: number;
  openFecOptions: OpenFecClientOptions;
  perPage: number;
}): Promise<{
  breakdowns: DirectBreakdown[];
  classifications: FinanceLabelClassification[];
}> {
  const breakdowns = new Map<string, DirectBreakdown>();
  const classifications = new Map<string, FinanceLabelClassification>();

  for (const committee of input.committees) {
    const aggregateInput = {
      committeeId: committee.committeeId,
      electionYear: input.electionYear,
      perPage: input.perPage,
    };

    const [employers, occupations, contributionSizes] = await Promise.all([
      input.fecClient.getCommitteeAggregatesByEmployer(aggregateInput, input.openFecOptions),
      input.fecClient.getCommitteeAggregatesByOccupation(aggregateInput, input.openFecOptions),
      input.fecClient.getCommitteeAggregatesBySize(aggregateInput, input.openFecOptions),
    ]);

    addAggregateBreakdowns(breakdowns, "employer", employers);
    addAggregateBreakdowns(breakdowns, "occupation", occupations);
    addAggregateBreakdowns(breakdowns, "contribution_size", contributionSizes);

    collectClassifications(classifications, "employer", employers);
    collectClassifications(classifications, "occupation", occupations);
  }

  return {
    breakdowns: [...breakdowns.values()],
    classifications: [...classifications.values()],
  };
}

async function fetchOutsideFinance(input: {
  fecClient: CandidateFinanceSyncFecClient;
  fecCandidateId: string;
  electionYear: number;
  openFecOptions: OpenFecClientOptions;
  perPage: number;
  outsideGroupLimit: number;
}): Promise<{
  outsideTotals: OpenFecOutsideSpendingTotals;
  outsideGroups: OpenFecOutsideSpendingGroup[];
  outsideBreakdowns: OutsideGroupBreakdown[];
  classifications: FinanceLabelClassification[];
}> {
  const classifications = new Map<string, FinanceLabelClassification>();
  const outsideBreakdowns = new Map<string, OutsideGroupBreakdown>();

  const outsideTotals = await input.fecClient.getOutsideSpendingTotalsByCandidate(
    input.fecCandidateId,
    input.electionYear,
    input.openFecOptions
  );
  const [supportGroups, opposeGroups] = await Promise.all([
    input.fecClient.listOutsideSpendingGroupsByCandidate(
      {
        fecCandidateId: input.fecCandidateId,
        electionYear: input.electionYear,
        supportOppose: "support",
        perPage: input.outsideGroupLimit,
      },
      input.openFecOptions
    ),
    input.fecClient.listOutsideSpendingGroupsByCandidate(
      {
        fecCandidateId: input.fecCandidateId,
        electionYear: input.electionYear,
        supportOppose: "oppose",
        perPage: input.outsideGroupLimit,
      },
      input.openFecOptions
    ),
  ]);
  const outsideGroups = [...supportGroups, ...opposeGroups];

  for (const group of outsideGroups) {
    const aggregateInput = {
      committeeId: group.committeeId,
      electionYear: input.electionYear,
      perPage: input.perPage,
    };
    const [employers, occupations] = await Promise.all([
      input.fecClient.getCommitteeAggregatesByEmployer(aggregateInput, input.openFecOptions),
      input.fecClient.getCommitteeAggregatesByOccupation(aggregateInput, input.openFecOptions),
    ]);

    addOutsideAggregateBreakdowns({ breakdowns: outsideBreakdowns, group, categoryType: "employer", aggregates: employers });
    addOutsideAggregateBreakdowns({ breakdowns: outsideBreakdowns, group, categoryType: "occupation", aggregates: occupations });

    collectClassifications(classifications, "employer", employers);
    collectClassifications(classifications, "occupation", occupations);
  }

  return {
    outsideTotals,
    outsideGroups,
    outsideBreakdowns: [...outsideBreakdowns.values()],
    classifications: [...classifications.values()],
  };
}

function toDirectBreakdownMap(breakdowns: readonly DirectBreakdown[]): Map<string, DirectBreakdown> {
  const map = new Map<string, DirectBreakdown>();
  for (const breakdown of breakdowns) {
    addBreakdown(map, breakdown);
  }
  return map;
}

function toOutsideBreakdownMap(breakdowns: readonly OutsideGroupBreakdown[]): Map<string, OutsideGroupBreakdown> {
  const map = new Map<string, OutsideGroupBreakdown>();
  for (const breakdown of breakdowns) {
    addOutsideBreakdown(map, breakdown);
  }
  return map;
}

function shouldReplaceClassification(
  existing: FinanceLabelClassification | undefined,
  next: FinanceLabelClassification
): boolean {
  if (!existing) {
    return true;
  }
  if (!existing.industrySlug && next.industrySlug) {
    return true;
  }
  return existing.classificationSource === "unknown" && next.classificationSource !== "unknown";
}

function mergeClassification(
  classifications: Map<string, FinanceLabelClassification>,
  classification: FinanceLabelClassification
): void {
  const key = classificationKey(classification.labelType, classification.normalizedLabel);
  if (shouldReplaceClassification(classifications.get(key), classification)) {
    classifications.set(key, classification);
  }
}

function collectAiClassificationCandidates(input: {
  directBreakdowns: Iterable<DirectBreakdown>;
  outsideBreakdowns: Iterable<OutsideGroupBreakdown>;
  classifications: Map<string, FinanceLabelClassification>;
  minAmount: number;
}): CandidateFinanceIndustryClassificationCandidate[] {
  const candidates = new Map<string, CandidateFinanceIndustryClassificationCandidate>();

  const addCandidate = (breakdown: { categoryType: string; categoryName: string; amount: number }): void => {
    if (breakdown.categoryType !== "employer" || breakdown.amount < input.minAmount) {
      return;
    }
    const normalizedLabel = normalizeFinanceLabel(breakdown.categoryName, "employer");
    if (!normalizedLabel) {
      return;
    }
    const key = classificationKey("employer", normalizedLabel);
    const classification = input.classifications.get(key);
    if (classification?.industrySlug) {
      return;
    }
    const existing = candidates.get(key);
    if (existing) {
      existing.amount += breakdown.amount;
      return;
    }
    candidates.set(key, {
      rawLabel: breakdown.categoryName,
      labelType: "employer",
      normalizedLabel,
      amount: breakdown.amount,
    });
  };

  for (const breakdown of input.directBreakdowns) {
    addCandidate(breakdown);
  }
  for (const breakdown of input.outsideBreakdowns) {
    addCandidate(breakdown);
  }

  return [...candidates.values()];
}

type FinanceClassificationRow = {
  raw_label: string;
  label_type: FinanceLabelType;
  normalized_label: string;
  industry_slug: FinanceIndustrySlug | null;
  confidence: FinanceClassificationConfidence;
  classification_source: FinanceClassificationSource;
};

function mapClassificationRow(row: FinanceClassificationRow): FinanceLabelClassification {
  return {
    rawLabel: row.raw_label,
    labelType: row.label_type,
    normalizedLabel: row.normalized_label,
    industrySlug: row.industry_slug,
    confidence: row.confidence,
    classificationSource: row.classification_source,
    matchedRule: null,
  };
}

async function loadCachedFinanceLabelClassifications(
  db: Queryable,
  labels: readonly CandidateFinanceIndustryClassificationCandidate[]
): Promise<FinanceLabelClassification[]> {
  if (labels.length === 0) {
    return [];
  }

  const valuesSql = labels
    .map((_label, index) => `($${index * 2 + 1}::text, $${index * 2 + 2}::text)`)
    .join(", ");
  const params = labels.flatMap((label) => [label.labelType, label.normalizedLabel]);
  const result = await db.query<FinanceClassificationRow>(
    `
      WITH requested(label_type, normalized_label) AS (
        VALUES ${valuesSql}
      )
      SELECT
        classification.raw_label,
        classification.label_type,
        classification.normalized_label,
        classification.industry_slug,
        classification.confidence,
        classification.classification_source
      FROM public.finance_label_classifications AS classification
      JOIN requested
        ON requested.label_type = classification.label_type
       AND requested.normalized_label = classification.normalized_label
    `,
    params
  );

  return result.rows.map(mapClassificationRow);
}

async function resolveFinanceIndustryClassifications(input: {
  db: Queryable;
  directBreakdowns: Iterable<DirectBreakdown>;
  outsideBreakdowns: Iterable<OutsideGroupBreakdown>;
  classifications: Map<string, FinanceLabelClassification>;
  classifier: CandidateFinanceIndustryClassifier | undefined;
  minAmount: number;
  dryRun: boolean;
}): Promise<void> {
  if (input.dryRun || !input.classifier) {
    return;
  }
  const directBreakdowns = [...input.directBreakdowns];
  const outsideBreakdowns = [...input.outsideBreakdowns];

  const initialCandidates = collectAiClassificationCandidates({
    directBreakdowns,
    outsideBreakdowns,
    classifications: input.classifications,
    minAmount: input.minAmount,
  });
  if (initialCandidates.length === 0) {
    return;
  }

  for (const classification of await loadCachedFinanceLabelClassifications(input.db, initialCandidates)) {
    mergeClassification(input.classifications, classification);
  }

  const remainingCandidates = collectAiClassificationCandidates({
    directBreakdowns,
    outsideBreakdowns,
    classifications: input.classifications,
    minAmount: input.minAmount,
  });
  if (remainingCandidates.length === 0) {
    return;
  }

  try {
    const aiClassifications = await input.classifier({ labels: remainingCandidates });
    for (const classification of aiClassifications) {
      mergeClassification(input.classifications, classification);
    }
  } catch {
    // Industry labels are enrichment-only. Keep the FEC sync successful if AI classification fails.
  }
}

function addIndustryBreakdownsFromClassifications(input: {
  directBreakdowns: Map<string, DirectBreakdown>;
  outsideBreakdowns: Map<string, OutsideGroupBreakdown>;
  classifications: Map<string, FinanceLabelClassification>;
}): void {
  const directEmployers = [...input.directBreakdowns.values()].filter(
    (breakdown) => breakdown.categoryType === "employer"
  );
  for (const breakdown of directEmployers) {
    const classification = input.classifications.get(
      classificationKey("employer", normalizeFinanceLabel(breakdown.categoryName, "employer"))
    );
    if (!classification?.industrySlug) {
      continue;
    }
    addBreakdown(input.directBreakdowns, {
      categoryType: "industry",
      categoryName: classification.industrySlug,
      amount: breakdown.amount,
      contributorCount: breakdown.contributorCount,
      sourceUrl: null,
    });
  }

  const outsideEmployers = [...input.outsideBreakdowns.values()].filter(
    (breakdown) => breakdown.categoryType === "employer"
  );
  for (const breakdown of outsideEmployers) {
    const classification = input.classifications.get(
      classificationKey("employer", normalizeFinanceLabel(breakdown.categoryName, "employer"))
    );
    if (!classification?.industrySlug) {
      continue;
    }
    addOutsideBreakdown(input.outsideBreakdowns, {
      committeeId: breakdown.committeeId,
      supportOppose: breakdown.supportOppose,
      categoryType: "industry",
      categoryName: classification.industrySlug,
      amount: breakdown.amount,
      contributorCount: breakdown.contributorCount,
      sourceUrl: null,
    });
  }
}

async function upsertSummary(input: {
  db: Queryable;
  totals: OpenFecFinanceCandidateTotals | null;
  outsideTotals: OpenFecOutsideSpendingTotals | null;
  fecCandidateId: string;
  electionYear: number;
  syncedAt: Date;
}): Promise<void> {
  if (!input.totals && !input.outsideTotals) {
    return;
  }

  await input.db.query(
    `
      INSERT INTO public.candidate_finance_summaries (
        fec_candidate_id,
        election_year,
        total_receipts,
        total_disbursements,
        cash_on_hand,
        debts_owed,
        individual_itemized_total,
        individual_unitemized_total,
        other_committee_contributions,
        transfers_from_affiliated_committees,
        outside_support_total,
        outside_oppose_total,
        source_url,
        last_synced_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::timestamptz)
      ON CONFLICT (fec_candidate_id, election_year)
      DO UPDATE SET
        total_receipts = EXCLUDED.total_receipts,
        total_disbursements = EXCLUDED.total_disbursements,
        cash_on_hand = EXCLUDED.cash_on_hand,
        debts_owed = EXCLUDED.debts_owed,
        individual_itemized_total = EXCLUDED.individual_itemized_total,
        individual_unitemized_total = EXCLUDED.individual_unitemized_total,
        other_committee_contributions = EXCLUDED.other_committee_contributions,
        transfers_from_affiliated_committees = EXCLUDED.transfers_from_affiliated_committees,
        outside_support_total = EXCLUDED.outside_support_total,
        outside_oppose_total = EXCLUDED.outside_oppose_total,
        source_url = EXCLUDED.source_url,
        last_synced_at = EXCLUDED.last_synced_at
    `,
    [
      input.fecCandidateId,
      input.electionYear,
      input.totals?.totalReceipts ?? null,
      input.totals?.totalDisbursements ?? null,
      input.totals?.cashOnHand ?? null,
      input.totals?.debtsOwed ?? null,
      input.totals?.individualItemizedTotal ?? null,
      input.totals?.individualUnitemizedTotal ?? null,
      input.totals?.otherCommitteeContributions ?? null,
      input.totals?.transfersFromAffiliatedCommittees ?? null,
      input.outsideTotals?.supportTotal ?? null,
      input.outsideTotals?.opposeTotal ?? null,
      input.totals?.sourceUrl ?? input.outsideTotals?.sourceUrl ?? null,
      input.syncedAt.toISOString(),
    ]
  );
}

async function upsertDirectBreakdown(input: {
  db: Queryable;
  fecCandidateId: string;
  electionYear: number;
  breakdown: DirectBreakdown;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.candidate_finance_direct_breakdowns (
        fec_candidate_id,
        election_year,
        category_type,
        category_name,
        amount,
        contributor_count,
        source_url,
        last_synced_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8::timestamptz)
      ON CONFLICT (fec_candidate_id, election_year, category_type, category_name)
      DO UPDATE SET
        amount = EXCLUDED.amount,
        contributor_count = EXCLUDED.contributor_count,
        source_url = EXCLUDED.source_url,
        last_synced_at = EXCLUDED.last_synced_at
    `,
    [
      input.fecCandidateId,
      input.electionYear,
      input.breakdown.categoryType,
      input.breakdown.categoryName,
      input.breakdown.amount,
      input.breakdown.contributorCount,
      input.breakdown.sourceUrl,
      input.syncedAt.toISOString(),
    ]
  );
}

async function upsertOutsideGroup(input: {
  db: Queryable;
  fecCandidateId: string;
  electionYear: number;
  group: OpenFecOutsideSpendingGroup;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.candidate_finance_outside_groups (
        fec_candidate_id,
        election_year,
        committee_id,
        committee_name,
        support_oppose,
        amount,
        source_url,
        last_synced_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8::timestamptz)
      ON CONFLICT (fec_candidate_id, election_year, committee_id, support_oppose)
      DO UPDATE SET
        committee_name = EXCLUDED.committee_name,
        amount = EXCLUDED.amount,
        source_url = EXCLUDED.source_url,
        last_synced_at = EXCLUDED.last_synced_at
    `,
    [
      input.fecCandidateId,
      input.electionYear,
      input.group.committeeId,
      input.group.committeeName,
      input.group.supportOppose,
      input.group.amount,
      input.group.sourceUrl,
      input.syncedAt.toISOString(),
    ]
  );
}

async function upsertOutsideGroupBreakdown(input: {
  db: Queryable;
  fecCandidateId: string;
  electionYear: number;
  breakdown: OutsideGroupBreakdown;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.candidate_finance_outside_group_breakdowns (
        fec_candidate_id,
        election_year,
        committee_id,
        support_oppose,
        category_type,
        category_name,
        amount,
        contributor_count,
        source_url,
        last_synced_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::timestamptz)
      ON CONFLICT (fec_candidate_id, election_year, committee_id, support_oppose, category_type, category_name)
      DO UPDATE SET
        amount = EXCLUDED.amount,
        contributor_count = EXCLUDED.contributor_count,
        source_url = EXCLUDED.source_url,
        last_synced_at = EXCLUDED.last_synced_at
    `,
    [
      input.fecCandidateId,
      input.electionYear,
      input.breakdown.committeeId,
      input.breakdown.supportOppose,
      input.breakdown.categoryType,
      input.breakdown.categoryName,
      input.breakdown.amount,
      input.breakdown.contributorCount,
      input.breakdown.sourceUrl,
      input.syncedAt.toISOString(),
    ]
  );
}

async function upsertClassification(input: {
  db: Queryable;
  classification: FinanceLabelClassification;
}): Promise<void> {
  await input.db.query(
    `
      INSERT INTO public.finance_label_classifications (
        raw_label,
        label_type,
        normalized_label,
        industry_slug,
        confidence,
        classification_source
      )
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (label_type, normalized_label)
      DO UPDATE SET
        raw_label = EXCLUDED.raw_label,
        industry_slug = EXCLUDED.industry_slug,
        confidence = EXCLUDED.confidence,
        classification_source = EXCLUDED.classification_source
    `,
    [
      input.classification.rawLabel,
      input.classification.labelType,
      input.classification.normalizedLabel,
      input.classification.industrySlug,
      input.classification.confidence,
      input.classification.classificationSource,
    ]
  );
}

async function deleteStaleDirectBreakdowns(input: {
  db: Queryable;
  fecCandidateId: string;
  electionYear: number;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      DELETE FROM public.candidate_finance_direct_breakdowns
      WHERE fec_candidate_id = $1
        AND election_year = $2
        AND last_synced_at < $3::timestamptz
    `,
    [input.fecCandidateId, input.electionYear, input.syncedAt.toISOString()]
  );
}

async function deleteStaleOutsideFinanceRows(input: {
  db: Queryable;
  fecCandidateId: string;
  electionYear: number;
  syncedAt: Date;
}): Promise<void> {
  await input.db.query(
    `
      DELETE FROM public.candidate_finance_outside_group_breakdowns
      WHERE fec_candidate_id = $1
        AND election_year = $2
        AND last_synced_at < $3::timestamptz
    `,
    [input.fecCandidateId, input.electionYear, input.syncedAt.toISOString()]
  );
  await input.db.query(
    `
      DELETE FROM public.candidate_finance_outside_groups
      WHERE fec_candidate_id = $1
        AND election_year = $2
        AND last_synced_at < $3::timestamptz
    `,
    [input.fecCandidateId, input.electionYear, input.syncedAt.toISOString()]
  );
}

function canOpenTransaction(db: Queryable): db is ConnectableQueryable & { connect: () => Promise<PoolClient> } {
  return typeof (db as ConnectableQueryable).connect === "function";
}

async function withFinanceWriteTransaction<T>(db: Queryable, work: (tx: Queryable) => Promise<T>): Promise<T> {
  if (!canOpenTransaction(db)) {
    return await work(db);
  }

  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const result = await work(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch {
      // Preserve the original write failure.
    }
    throw error;
  } finally {
    client.release();
  }
}

async function writeCandidateFinanceSync(input: {
  db: Queryable;
  totals: OpenFecFinanceCandidateTotals | null;
  outsideTotals: OpenFecOutsideSpendingTotals | null;
  fecCandidateId: string;
  electionYear: number;
  syncedAt: Date;
  directBreakdowns: Iterable<DirectBreakdown>;
  outsideGroups: readonly OpenFecOutsideSpendingGroup[];
  outsideBreakdowns: Iterable<OutsideGroupBreakdown>;
  classifications: Iterable<FinanceLabelClassification>;
  replaceOutsideFinanceRows: boolean;
}): Promise<void> {
  await withFinanceWriteTransaction(input.db, async (db) => {
    await upsertSummary({
      db,
      totals: input.totals,
      outsideTotals: input.outsideTotals,
      fecCandidateId: input.fecCandidateId,
      electionYear: input.electionYear,
      syncedAt: input.syncedAt,
    });
    for (const breakdown of input.directBreakdowns) {
      await upsertDirectBreakdown({ db, fecCandidateId: input.fecCandidateId, electionYear: input.electionYear, breakdown, syncedAt: input.syncedAt });
    }
    await deleteStaleDirectBreakdowns({
      db,
      fecCandidateId: input.fecCandidateId,
      electionYear: input.electionYear,
      syncedAt: input.syncedAt,
    });

    if (input.replaceOutsideFinanceRows) {
      for (const group of input.outsideGroups) {
        await upsertOutsideGroup({ db, fecCandidateId: input.fecCandidateId, electionYear: input.electionYear, group, syncedAt: input.syncedAt });
      }
      for (const breakdown of input.outsideBreakdowns) {
        await upsertOutsideGroupBreakdown({
          db,
          fecCandidateId: input.fecCandidateId,
          electionYear: input.electionYear,
          breakdown,
          syncedAt: input.syncedAt,
        });
      }
      await deleteStaleOutsideFinanceRows({
        db,
        fecCandidateId: input.fecCandidateId,
        electionYear: input.electionYear,
        syncedAt: input.syncedAt,
      });
    }

    for (const classification of input.classifications) {
      await upsertClassification({ db, classification });
    }
  });
}

export async function syncCandidateFinance(input: CandidateFinanceSyncInput): Promise<CandidateFinanceSyncResult> {
  const fecCandidateId = normalizeFecCandidateId(input.fecCandidateId);
  const electionYear = normalizeElectionYear(input.electionYear);
  const syncedAt = input.now ?? new Date();
  const perPage = normalizePerPage(input.perPage);
  const outsideGroupLimit = normalizeOutsideGroupLimit(input.outsideGroupLimit);
  const aiClassificationMinAmount = normalizeAiClassificationMinAmount(input.aiClassificationMinAmount);
  const includeOutside = input.includeOutside === true;
  const fecClient = input.fecClient ?? DEFAULT_FEC_CLIENT;

  const [totals, committees] = await Promise.all([
    fecClient.getCandidateTotals(fecCandidateId, electionYear, input.openFecOptions),
    fecClient.listCandidateCommittees(fecCandidateId, electionYear, input.openFecOptions),
  ]);
  const directCommittees = committees.filter(isDirectCampaignCommittee);
  const directFinance = await fetchDirectBreakdowns({
    fecClient,
    committees: directCommittees,
    electionYear,
    openFecOptions: input.openFecOptions,
    perPage,
  });
  const outsideFinance = includeOutside
    ? await fetchOutsideFinance({
        fecClient,
        fecCandidateId,
        electionYear,
        openFecOptions: input.openFecOptions,
        perPage,
        outsideGroupLimit,
      })
    : null;

  const classifications = new Map<string, FinanceLabelClassification>();
  for (const classification of [...directFinance.classifications, ...(outsideFinance?.classifications ?? [])]) {
    mergeClassification(classifications, classification);
  }
  const directBreakdowns = toDirectBreakdownMap(directFinance.breakdowns);
  const outsideBreakdowns = toOutsideBreakdownMap(outsideFinance?.outsideBreakdowns ?? []);

  await resolveFinanceIndustryClassifications({
    db: input.db,
    directBreakdowns: directBreakdowns.values(),
    outsideBreakdowns: outsideBreakdowns.values(),
    classifications,
    classifier: input.financeIndustryClassifier,
    minAmount: aiClassificationMinAmount,
    dryRun: input.dryRun === true,
  });

  addIndustryBreakdownsFromClassifications({
    directBreakdowns,
    outsideBreakdowns,
    classifications,
  });

  if (!input.dryRun) {
    await writeCandidateFinanceSync({
      db: input.db,
      totals,
      outsideTotals: outsideFinance?.outsideTotals ?? null,
      fecCandidateId,
      electionYear,
      syncedAt,
      directBreakdowns: directBreakdowns.values(),
      outsideGroups: outsideFinance?.outsideGroups ?? [],
      outsideBreakdowns: outsideBreakdowns.values(),
      classifications: classifications.values(),
      replaceOutsideFinanceRows: includeOutside,
    });
  }

  return {
    fecCandidateId,
    electionYear,
    dryRun: input.dryRun === true,
    directCommitteeCount: directCommittees.length,
    summaryWritten: Boolean((totals || outsideFinance?.outsideTotals) && !input.dryRun),
    directBreakdownsWritten: input.dryRun ? 0 : directBreakdowns.size,
    industryBreakdownsWritten: input.dryRun
      ? 0
      : [...directBreakdowns.values()].filter((breakdown) => breakdown.categoryType === "industry").length,
    classificationsWritten: input.dryRun ? 0 : classifications.size,
    outsideIncluded: includeOutside,
    outsideGroupsWritten: input.dryRun ? 0 : outsideFinance?.outsideGroups.length ?? 0,
    outsideGroupBreakdownsWritten: input.dryRun ? 0 : outsideBreakdowns.size,
    outsideSupportTotal: outsideFinance?.outsideTotals.supportTotal ?? null,
    outsideOpposeTotal: outsideFinance?.outsideTotals.opposeTotal ?? null,
  };
}
