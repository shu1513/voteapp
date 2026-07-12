import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";
import { Pool } from "pg";

import { createFinanceIndustryClassifierFromEnv } from "../ai/classifyFinanceIndustry.js";
import { getPipelineEnv } from "../config/env.js";
import {
  isHoustonCampaignFinanceEnabled,
  isHoustonCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  syncDueHoustonCandidateFinance,
  type HoustonCandidateFinanceBatchSyncResult,
} from "../pipeline/houstonFinance/houstonCandidateFinanceBatchSync.js";

export const HOUSTON_CANDIDATE_FINANCE_SYNC_JOB_NAME = "houston_candidate_finance_sync_due";
export const HOUSTON_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID =
  "houston_candidate_finance_sync_daily";

export type HoustonCandidateFinanceSyncJobData = {
  dryRun?: boolean;
  force?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  rawDataZipPath?: string;
  rawDataCacheDir?: string;
  aiClassifyIndustries?: boolean;
  aiClassificationMinAmount?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type HoustonCandidateFinanceSyncJobResult = HoustonCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<HoustonCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type HoustonCandidateFinanceSyncEnqueueOptions = {
  jobId?: string;
};

type HoustonCandidateFinanceSyncSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

const DISABLED_RESULT_DEFAULT_MAX_CANDIDATES = 0;
const DISABLED_RESULT_DEFAULT_STALE_AFTER_DAYS = 0;

function readSchedulerRuntimeConfig(): HoustonCandidateFinanceSyncSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.HOUSTON_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() ||
      "houston_candidate_finance_sync_maintenance",
    dailyCron: process.env.HOUSTON_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() || "10 9 * * *",
    dailyTz: process.env.HOUSTON_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  if (value !== undefined && (!Number.isInteger(value) || value <= 0)) {
    throw new Error(`Invalid Houston finance sync scheduler ${label}: ${value}`);
  }
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
    throw new Error("Houston finance sync scheduler jobId must not contain ':'");
  }
  return trimmed;
}

function jobOptionsWithId(jobId: string | undefined): JobsOptions {
  const normalizedJobId = normalizeOptionalJobId(jobId);
  return normalizedJobId ? { ...defaultJobOptions(), jobId: normalizedJobId } : defaultJobOptions();
}

function assertValidJobOptions(data: HoustonCandidateFinanceSyncJobData): void {
  assertPositiveInteger(data.maxCandidates, "maxCandidates");
  assertPositiveInteger(data.staleAfterDays, "staleAfterDays");
  assertPositiveInteger(data.electionLookbackDays, "electionLookbackDays");
  assertPositiveInteger(data.electionLookaheadDays, "electionLookaheadDays");
  assertPositiveInteger(data.aiClassificationMinAmount, "aiClassificationMinAmount");
}

export function createHoustonCandidateFinanceSyncSchedulerQueue(): Queue<HoustonCandidateFinanceSyncJobData> {
  return new Queue<HoustonCandidateFinanceSyncJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export function buildHoustonCandidateFinanceLinkedElectionSyncJobId(now = new Date()): string {
  if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
    throw new Error("Invalid Houston finance linked-election sync job date");
  }
  return `houston-candidate-finance-linked-election-sync-${now.toISOString().slice(0, 10)}`;
}

export async function upsertRecurringHoustonCandidateFinanceSyncJobs(
  jobData: HoustonCandidateFinanceSyncJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isHoustonCampaignFinanceEnabled()) {
    const queue = createHoustonCandidateFinanceSyncSchedulerQueue();
    try {
      await queue.removeJobScheduler(HOUSTON_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const queue = createHoustonCandidateFinanceSyncSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      HOUSTON_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: HOUSTON_CANDIDATE_FINANCE_SYNC_JOB_NAME,
        data: {
          dryRun: Boolean(jobData.dryRun),
          force: Boolean(jobData.force),
          maxCandidates: jobData.maxCandidates,
          staleAfterDays: jobData.staleAfterDays,
          electionLookbackDays: jobData.electionLookbackDays,
          electionLookaheadDays: jobData.electionLookaheadDays,
          rawDataZipPath: jobData.rawDataZipPath,
          rawDataCacheDir: jobData.rawDataCacheDir,
          aiClassifyIndustries: Boolean(jobData.aiClassifyIndustries),
          aiClassificationMinAmount: jobData.aiClassificationMinAmount,
          triggeredBy: "daily",
        },
        opts: defaultJobOptions(),
      }
    );
  } finally {
    await queue.close();
  }
}

export async function enqueueManualHoustonCandidateFinanceSyncJob(
  jobData: HoustonCandidateFinanceSyncJobData = {},
  options: HoustonCandidateFinanceSyncEnqueueOptions = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isHoustonCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const queue = createHoustonCandidateFinanceSyncSchedulerQueue();

  try {
    const job = await queue.add(
      HOUSTON_CANDIDATE_FINANCE_SYNC_JOB_NAME,
      {
        dryRun: Boolean(jobData.dryRun),
        force: Boolean(jobData.force),
        maxCandidates: jobData.maxCandidates,
        staleAfterDays: jobData.staleAfterDays,
        electionLookbackDays: jobData.electionLookbackDays,
        electionLookaheadDays: jobData.electionLookaheadDays,
        rawDataZipPath: jobData.rawDataZipPath,
        rawDataCacheDir: jobData.rawDataCacheDir,
        aiClassifyIndustries: Boolean(jobData.aiClassifyIndustries),
        aiClassificationMinAmount: jobData.aiClassificationMinAmount,
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

export async function runHoustonCandidateFinanceSyncJob(
  data: HoustonCandidateFinanceSyncJobData = {}
): Promise<HoustonCandidateFinanceSyncJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const dryRun = Boolean(data.dryRun);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const now = new Date();
  const enabled = isHoustonCampaignFinanceSyncEnabled(force);

  if (!data.triggeredBy) {
    console.warn("Houston finance sync job missing triggeredBy; recording as unknown");
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
      outsideSourceAvailable: false,
      results: [],
    };
  }

  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  try {
    const result = await syncDueHoustonCandidateFinance({
      db: pool,
      now,
      dryRun,
      force,
      maxCandidates: data.maxCandidates,
      staleAfterDays: data.staleAfterDays,
      electionLookbackDays: data.electionLookbackDays,
      electionLookaheadDays: data.electionLookaheadDays,
      rawDataZipPath: data.rawDataZipPath,
      rawDataCacheDir: data.rawDataCacheDir,
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

export function createHoustonCandidateFinanceSyncSchedulerWorker(): Worker<
  HoustonCandidateFinanceSyncJobData,
  HoustonCandidateFinanceSyncJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<
    HoustonCandidateFinanceSyncJobData,
    HoustonCandidateFinanceSyncJobResult
  > = async (job) => {
    return runHoustonCandidateFinanceSyncJob(job.data ?? {});
  };

  return new Worker<HoustonCandidateFinanceSyncJobData, HoustonCandidateFinanceSyncJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
