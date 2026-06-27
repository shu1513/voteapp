import type { Pool, PoolClient } from "pg";

import type { FinanceIndustryClassifier } from "../finance/financeIndustryClassificationService.js";
import {
  DEFAULT_FLORIDA_CAMPAIGN_FINANCE_CACHE_DIR,
  readFloridaContributionExportArtifact,
} from "./floridaCampaignFinanceArtifactCache.js";
import type { FloridaContributionRow } from "./floridaCampaignFinanceRows.js";
import {
  syncFloridaCandidateFinance,
  type FloridaCandidateFinanceSyncInput,
  type FloridaCandidateFinanceSyncResult,
} from "./floridaCandidateFinanceSync.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type FloridaContributionArtifactReference = {
  cacheDir?: string | null;
  cacheKey: string;
};

export type FloridaCandidateFinanceBatchSyncItemInput = Omit<
  FloridaCandidateFinanceSyncInput,
  "db" | "now" | "dryRun" | "financeIndustryClassifier" | "contributionRows" | "outsideContributionRows"
> & {
  contributionRows?: readonly FloridaContributionRow[];
  contributionArtifact?: FloridaContributionArtifactReference | null;
  outsideContributionRows?: readonly FloridaContributionRow[];
  outsideContributionArtifact?: FloridaContributionArtifactReference | null;
};

export type FloridaCandidateFinanceBatchSyncInput = {
  db: Queryable;
  now?: Date;
  dryRun?: boolean;
  maxCandidates?: number;
  syncInputs?: readonly FloridaCandidateFinanceBatchSyncItemInput[];
  defaultArtifactCacheDir?: string | null;
  financeIndustryClassifier?: FinanceIndustryClassifier;
  aiClassificationMinAmount?: number;
  syncFloridaCandidateFinanceFn?: typeof syncFloridaCandidateFinance;
};

export type FloridaCandidateFinanceBatchSyncItemResult = {
  candidateId: string;
  electionId: string;
  electionYear: number;
  committeeId: string;
  ok: boolean;
  result?: FloridaCandidateFinanceSyncResult;
  error?: string;
};

export type FloridaCandidateFinanceBatchSyncResult = {
  dryRun: boolean;
  now: string;
  maxCandidates: number;
  dueCandidateCount: number;
  selectedCandidateCount: number;
  syncedCandidateCount: number;
  failedCandidateCount: number;
  results: FloridaCandidateFinanceBatchSyncItemResult[];
};

const DEFAULT_MAX_CANDIDATES = 25;

function assertValidDate(date: Date, label: string): void {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new Error(`Invalid Florida finance batch sync ${label}`);
  }
}

function normalizePositiveInteger(value: number | undefined, fallback: number, label: string): number {
  const normalized = value ?? fallback;
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw new Error(`Invalid Florida finance batch sync ${label}: ${value}`);
  }
  return normalized;
}

function normalizeArtifactCacheDir(value: string | null | undefined, fallback: string): string {
  const normalized = value?.trim();
  return normalized && normalized.length > 0 ? normalized : fallback;
}

async function loadRowsFromArtifact(input: {
  artifact: FloridaContributionArtifactReference | null | undefined;
  defaultArtifactCacheDir: string;
}): Promise<FloridaContributionRow[] | null> {
  if (!input.artifact) {
    return null;
  }
  const cacheKey = input.artifact.cacheKey.trim();
  if (!cacheKey) {
    throw new Error("Florida contribution artifact cacheKey is required");
  }
  const artifact = await readFloridaContributionExportArtifact({
    cacheDir: normalizeArtifactCacheDir(input.artifact.cacheDir, input.defaultArtifactCacheDir),
    cacheKey,
  });
  if (!artifact) {
    throw new Error(`Florida contribution export artifact not found: ${cacheKey}`);
  }
  return artifact.rows;
}

async function resolveContributionRows(input: {
  rows: readonly FloridaContributionRow[] | undefined;
  artifact: FloridaContributionArtifactReference | null | undefined;
  defaultArtifactCacheDir: string;
}): Promise<readonly FloridaContributionRow[]> {
  const artifactRows = await loadRowsFromArtifact({
    artifact: input.artifact,
    defaultArtifactCacheDir: input.defaultArtifactCacheDir,
  });
  return input.rows ?? artifactRows ?? [];
}

export async function syncFloridaCandidateFinanceBatch(
  input: FloridaCandidateFinanceBatchSyncInput
): Promise<FloridaCandidateFinanceBatchSyncResult> {
  const now = input.now ?? new Date();
  assertValidDate(now, "now");
  const dryRun = input.dryRun === true;
  const maxCandidates = normalizePositiveInteger(input.maxCandidates, DEFAULT_MAX_CANDIDATES, "maxCandidates");
  const dueCandidateCount = input.syncInputs?.length ?? 0;
  const selectedInputs = (input.syncInputs ?? []).slice(0, maxCandidates);
  const defaultArtifactCacheDir = normalizeArtifactCacheDir(
    input.defaultArtifactCacheDir,
    DEFAULT_FLORIDA_CAMPAIGN_FINANCE_CACHE_DIR
  );
  const syncOne = input.syncFloridaCandidateFinanceFn ?? syncFloridaCandidateFinance;
  const results: FloridaCandidateFinanceBatchSyncItemResult[] = [];

  for (const syncInput of selectedInputs) {
    try {
      const contributionRows = await resolveContributionRows({
        rows: syncInput.contributionRows,
        artifact: syncInput.contributionArtifact,
        defaultArtifactCacheDir,
      });
      const outsideContributionRows = await resolveContributionRows({
        rows: syncInput.outsideContributionRows,
        artifact: syncInput.outsideContributionArtifact,
        defaultArtifactCacheDir,
      });
      const result = await syncOne({
        ...syncInput,
        db: input.db,
        now,
        dryRun,
        contributionRows,
        outsideContributionRows,
        financeIndustryClassifier: input.financeIndustryClassifier,
        aiClassificationMinAmount: input.aiClassificationMinAmount ?? syncInput.aiClassificationMinAmount,
      });
      results.push({
        candidateId: syncInput.candidateId,
        electionId: syncInput.electionId,
        electionYear: syncInput.electionYear,
        committeeId: syncInput.trustedCommittee.committeeId,
        ok: true,
        result,
      });
    } catch (error) {
      results.push({
        candidateId: syncInput.candidateId,
        electionId: syncInput.electionId,
        electionYear: syncInput.electionYear,
        committeeId: syncInput.trustedCommittee.committeeId,
        ok: false,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  const syncedCandidateCount = results.filter((result) => result.ok).length;
  return {
    dryRun,
    now: now.toISOString(),
    maxCandidates,
    dueCandidateCount,
    selectedCandidateCount: selectedInputs.length,
    syncedCandidateCount,
    failedCandidateCount: results.length - syncedCandidateCount,
    results,
  };
}
