import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";
import { Pool } from "pg";

import { createFinanceIndustryClassifierFromEnv } from "../ai/classifyFinanceIndustry.js";
import { getPipelineEnv } from "../config/env.js";
import { isIllinoisCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueIllinoisCandidateFinance,
  type IllinoisCandidateFinanceBatchSyncResult,
} from "../pipeline/illinoisFinance/illinoisCandidateFinanceBatchSync.js";
import {
  createIllinoisSbeArtifactCandidateCommitteeResolver,
  loadIllinoisFinanceDataForDueRowFromArtifacts,
  loadIllinoisSbeArtifactDataSet,
} from "../pipeline/illinoisFinance/illinoisSbeArtifactDataSource.js";

export const ILLINOIS_CANDIDATE_FINANCE_SYNC_JOB_NAME = "illinois_candidate_finance_sync_due";
export const ILLINOIS_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID = "illinois_candidate_finance_sync_daily";

export type IllinoisCandidateFinanceSyncJobData = {
  dryRun?: boolean;
  force?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  aiClassifyIndustries?: boolean;
  aiClassificationMinAmount?: number;
  contributionCsvPaths?: string[];
  expenditureCsvPaths?: string[];
  contributionSourceUrl?: string;
  expenditureSourceUrl?: string;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type IllinoisCandidateFinanceSyncJobResult = IllinoisCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<IllinoisCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type IllinoisCandidateFinanceSyncEnqueueOptions = {
  jobId?: string;
};

type IllinoisCandidateFinanceSyncSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

const DISABLED_RESULT_DEFAULT_MAX_CANDIDATES = 0;
const DISABLED_RESULT_DEFAULT_STALE_AFTER_DAYS = 0;

type NormalizedIllinoisCandidateFinanceArtifactJobData = {
  contributionCsvPaths: string[];
  expenditureCsvPaths?: string[];
  contributionSourceUrl?: string;
  expenditureSourceUrl?: string;
};

function readSchedulerRuntimeConfig(): IllinoisCandidateFinanceSyncSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() ||
      "illinois_candidate_finance_sync_maintenance",
    dailyCron: process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() || "30 9 * * *",
    dailyTz: process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  if (value !== undefined && (!Number.isInteger(value) || value <= 0)) {
    throw new Error(`Invalid Illinois finance sync scheduler ${label}: ${value}`);
  }
}

function normalizeOptionalString(value: string | undefined, label: string): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error(`Invalid Illinois finance sync scheduler ${label}`);
  }
  return trimmed;
}

function normalizeOptionalStringArray(value: string[] | undefined, label: string): string[] | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Array.isArray(value)) {
    throw new Error(`Invalid Illinois finance sync scheduler ${label}`);
  }
  const normalized = value.map((item) => item.trim()).filter(Boolean);
  if (normalized.length !== value.length) {
    throw new Error(`Invalid Illinois finance sync scheduler ${label}`);
  }
  return normalized.length > 0 ? normalized : undefined;
}

function normalizeArtifactJobData(
  data: IllinoisCandidateFinanceSyncJobData
): NormalizedIllinoisCandidateFinanceArtifactJobData | undefined {
  const contributionCsvPaths = normalizeOptionalStringArray(data.contributionCsvPaths, "contributionCsvPaths");
  const expenditureCsvPaths = normalizeOptionalStringArray(data.expenditureCsvPaths, "expenditureCsvPaths");
  const contributionSourceUrl = normalizeOptionalString(data.contributionSourceUrl, "contributionSourceUrl");
  const expenditureSourceUrl = normalizeOptionalString(data.expenditureSourceUrl, "expenditureSourceUrl");

  if (!contributionCsvPaths) {
    if (expenditureCsvPaths || contributionSourceUrl || expenditureSourceUrl) {
      throw new Error("Illinois finance sync scheduler artifact options require contributionCsvPaths");
    }
    return undefined;
  }

  return {
    contributionCsvPaths,
    expenditureCsvPaths,
    contributionSourceUrl,
    expenditureSourceUrl,
  };
}

function getQueueConnection(): ConnectionOptions {
  const env = getPipelineEnv();
  return toConnectionOptions(env.REDIS_URL);
}

function getQueueName(): string {
  return readSchedulerRuntimeConfig().queueName;
}

function defaultJobOptions(): JobsOptions {
  return {
    removeOnComplete: 1000,
    removeOnFail: 1000,
  };
}

function normalizeOptionalJobId(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  if (!trimmed) {
    return undefined;
  }
  if (trimmed.includes(":")) {
    throw new Error("Illinois finance sync scheduler jobId must not contain ':'");
  }
  return trimmed;
}

function jobOptionsWithId(jobId: string | undefined): JobsOptions {
  const normalizedJobId = normalizeOptionalJobId(jobId);
  return normalizedJobId ? { ...defaultJobOptions(), jobId: normalizedJobId } : defaultJobOptions();
}

function assertValidJobOptions(data: IllinoisCandidateFinanceSyncJobData): void {
  assertPositiveInteger(data.maxCandidates, "maxCandidates");
  assertPositiveInteger(data.staleAfterDays, "staleAfterDays");
  assertPositiveInteger(data.electionLookbackDays, "electionLookbackDays");
  assertPositiveInteger(data.electionLookaheadDays, "electionLookaheadDays");
  assertPositiveInteger(data.aiClassificationMinAmount, "aiClassificationMinAmount");
  normalizeArtifactJobData(data);
}

export function createIllinoisCandidateFinanceSyncSchedulerQueue(): Queue<IllinoisCandidateFinanceSyncJobData> {
  return new Queue<IllinoisCandidateFinanceSyncJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export function buildIllinoisCandidateFinanceLinkedElectionSyncJobId(now = new Date()): string {
  if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
    throw new Error("Invalid Illinois finance linked-election sync job date");
  }
  return `illinois-candidate-finance-linked-election-sync-${now.toISOString().slice(0, 10)}`;
}

export async function upsertRecurringIllinoisCandidateFinanceSyncJobs(
  jobData: IllinoisCandidateFinanceSyncJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  const artifacts = normalizeArtifactJobData(jobData);
  if (!isIllinoisCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    const queue = createIllinoisCandidateFinanceSyncSchedulerQueue();
    try {
      await queue.removeJobScheduler(ILLINOIS_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const queue = createIllinoisCandidateFinanceSyncSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      ILLINOIS_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: ILLINOIS_CANDIDATE_FINANCE_SYNC_JOB_NAME,
        data: {
          dryRun: Boolean(jobData.dryRun),
          force: Boolean(jobData.force),
          maxCandidates: jobData.maxCandidates,
          staleAfterDays: jobData.staleAfterDays,
          electionLookbackDays: jobData.electionLookbackDays,
          electionLookaheadDays: jobData.electionLookaheadDays,
          aiClassifyIndustries: Boolean(jobData.aiClassifyIndustries),
          aiClassificationMinAmount: jobData.aiClassificationMinAmount,
          contributionCsvPaths: artifacts?.contributionCsvPaths,
          expenditureCsvPaths: artifacts?.expenditureCsvPaths,
          contributionSourceUrl: artifacts?.contributionSourceUrl,
          expenditureSourceUrl: artifacts?.expenditureSourceUrl,
          triggeredBy: "daily",
        },
        opts: defaultJobOptions(),
      }
    );
  } finally {
    await queue.close();
  }
}

export async function enqueueManualIllinoisCandidateFinanceSyncJob(
  jobData: IllinoisCandidateFinanceSyncJobData = {},
  options: IllinoisCandidateFinanceSyncEnqueueOptions = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  const artifacts = normalizeArtifactJobData(jobData);
  if (!isIllinoisCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const queue = createIllinoisCandidateFinanceSyncSchedulerQueue();

  try {
    const job = await queue.add(
      ILLINOIS_CANDIDATE_FINANCE_SYNC_JOB_NAME,
      {
        dryRun: Boolean(jobData.dryRun),
        force: Boolean(jobData.force),
        maxCandidates: jobData.maxCandidates,
        staleAfterDays: jobData.staleAfterDays,
        electionLookbackDays: jobData.electionLookbackDays,
        electionLookaheadDays: jobData.electionLookaheadDays,
        aiClassifyIndustries: Boolean(jobData.aiClassifyIndustries),
        aiClassificationMinAmount: jobData.aiClassificationMinAmount,
        contributionCsvPaths: artifacts?.contributionCsvPaths,
        expenditureCsvPaths: artifacts?.expenditureCsvPaths,
        contributionSourceUrl: artifacts?.contributionSourceUrl,
        expenditureSourceUrl: artifacts?.expenditureSourceUrl,
        triggeredBy: "manual",
        requestedAt: new Date().toISOString(),
      },
      jobOptionsWithId(options.jobId)
    );
    return job.id ?? "unknown";
  } finally {
    await queue.close();
  }
}

export async function runIllinoisCandidateFinanceSyncJob(
  data: IllinoisCandidateFinanceSyncJobData = {}
): Promise<IllinoisCandidateFinanceSyncJobResult> {
  assertValidJobOptions(data);
  const artifactJobData = normalizeArtifactJobData(data);
  const force = Boolean(data.force);
  const dryRun = Boolean(data.dryRun);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const now = new Date();
  const enabled = isIllinoisCampaignFinanceSyncEnabled(force);

  if (!data.triggeredBy) {
    console.warn("Illinois finance sync job missing triggeredBy; recording as unknown");
  }

  if (!enabled) {
    return {
      enabled: false,
      force,
      triggeredBy,
      dryRun,
      now: now.toISOString(),
      staleAfterDays: data.staleAfterDays ?? DISABLED_RESULT_DEFAULT_STALE_AFTER_DAYS,
      maxCandidates: data.maxCandidates ?? DISABLED_RESULT_DEFAULT_MAX_CANDIDATES,
      dueCandidateCount: 0,
      selectedCandidateCount: 0,
      syncedCandidateCount: 0,
      failedCandidateCount: 0,
      autoLinkAttemptedCount: 0,
      autoLinkLinkedCount: 0,
      results: [],
    };
  }

  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  try {
    const artifacts = artifactJobData ? await loadIllinoisSbeArtifactDataSet(artifactJobData) : null;
    const artifactCandidateCommitteeResolver = artifacts
      ? createIllinoisSbeArtifactCandidateCommitteeResolver(artifacts)
      : undefined;
    const result = await syncDueIllinoisCandidateFinance({
      db: pool,
      now,
      dryRun,
      maxCandidates: data.maxCandidates,
      staleAfterDays: data.staleAfterDays,
      electionLookbackDays: data.electionLookbackDays,
      electionLookaheadDays: data.electionLookaheadDays,
      resolveCandidateCommittee: artifactCandidateCommitteeResolver,
      loadIllinoisFinanceDataFn: artifacts
        ? async (row) => loadIllinoisFinanceDataForDueRowFromArtifacts({ row, artifacts })
        : undefined,
      financeIndustryClassifier:
        data.aiClassifyIndustries && !dryRun ? createFinanceIndustryClassifierFromEnv() : undefined,
      aiClassificationMinAmount: data.aiClassificationMinAmount,
    });

    return {
      enabled: true,
      force,
      triggeredBy,
      ...result,
    };
  } finally {
    await pool.end();
  }
}

export function createIllinoisCandidateFinanceSyncSchedulerWorker(): Worker<
  IllinoisCandidateFinanceSyncJobData,
  IllinoisCandidateFinanceSyncJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<IllinoisCandidateFinanceSyncJobData, IllinoisCandidateFinanceSyncJobResult> = async (
    job
  ) => {
    return runIllinoisCandidateFinanceSyncJob(job.data ?? {});
  };

  return new Worker<IllinoisCandidateFinanceSyncJobData, IllinoisCandidateFinanceSyncJobResult>(queueName, processor, {
    connection,
    concurrency: 1,
  });
}
