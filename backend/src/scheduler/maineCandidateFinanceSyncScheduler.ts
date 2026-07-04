import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";
import { Pool } from "pg";

import { createFinanceIndustryClassifierFromEnv } from "../ai/classifyFinanceIndustry.js";
import { getPipelineEnv } from "../config/env.js";
import { isMaineCampaignFinanceEnabled, isMaineCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueMaineCandidateFinance,
  type MaineCandidateFinanceBatchSyncResult,
} from "../pipeline/maineFinance/maineCandidateFinanceBatchSync.js";

export const MAINE_CANDIDATE_FINANCE_SYNC_JOB_NAME = "maine_candidate_finance_sync_due";
export const MAINE_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID = "maine_candidate_finance_sync_daily";

export type MaineCandidateFinanceSyncJobData = {
  dryRun?: boolean;
  force?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  rawDataCacheDir?: string;
  aiClassifyIndustries?: boolean;
  aiClassificationMinAmount?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type MaineCandidateFinanceSyncJobResult = MaineCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<MaineCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type MaineCandidateFinanceSyncEnqueueOptions = {
  jobId?: string;
};

type MaineCandidateFinanceSyncSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

const DISABLED_RESULT_DEFAULT_MAX_CANDIDATES = 0;
const DISABLED_RESULT_DEFAULT_STALE_AFTER_DAYS = 0;

function readSchedulerRuntimeConfig(): MaineCandidateFinanceSyncSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.MAINE_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() ||
      "maine_candidate_finance_sync_maintenance",
    dailyCron: process.env.MAINE_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() || "55 9 * * *",
    dailyTz: process.env.MAINE_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  if (value !== undefined && (!Number.isInteger(value) || value <= 0)) {
    throw new Error(`Invalid Maine finance sync scheduler ${label}: ${value}`);
  }
}

function getQueueConnection(): ConnectionOptions {
  return toConnectionOptions(getPipelineEnv().REDIS_URL);
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
    throw new Error("Maine finance sync scheduler jobId must not contain ':'");
  }
  if (trimmed === "disabled" || trimmed === "unknown") {
    throw new Error("Maine finance sync scheduler jobId uses a reserved value");
  }
  return trimmed;
}

function jobOptionsWithId(jobId: string | undefined): JobsOptions {
  const normalizedJobId = normalizeOptionalJobId(jobId);
  return normalizedJobId ? { ...defaultJobOptions(), jobId: normalizedJobId } : defaultJobOptions();
}

function assertValidJobOptions(data: MaineCandidateFinanceSyncJobData): void {
  assertPositiveInteger(data.maxCandidates, "maxCandidates");
  assertPositiveInteger(data.staleAfterDays, "staleAfterDays");
  assertPositiveInteger(data.electionLookbackDays, "electionLookbackDays");
  assertPositiveInteger(data.electionLookaheadDays, "electionLookaheadDays");
  assertPositiveInteger(data.aiClassificationMinAmount, "aiClassificationMinAmount");
}

export function createMaineCandidateFinanceSyncSchedulerQueue(): Queue<MaineCandidateFinanceSyncJobData> {
  return new Queue<MaineCandidateFinanceSyncJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringMaineCandidateFinanceSyncJobs(
  jobData: MaineCandidateFinanceSyncJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isMaineCampaignFinanceEnabled()) {
    const queue = createMaineCandidateFinanceSyncSchedulerQueue();
    try {
      await queue.removeJobScheduler(MAINE_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const queue = createMaineCandidateFinanceSyncSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      MAINE_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: MAINE_CANDIDATE_FINANCE_SYNC_JOB_NAME,
        data: {
          dryRun: Boolean(jobData.dryRun),
          force: Boolean(jobData.force),
          maxCandidates: jobData.maxCandidates,
          staleAfterDays: jobData.staleAfterDays,
          electionLookbackDays: jobData.electionLookbackDays,
          electionLookaheadDays: jobData.electionLookaheadDays,
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

export async function enqueueManualMaineCandidateFinanceSyncJob(
  jobData: MaineCandidateFinanceSyncJobData = {},
  options: MaineCandidateFinanceSyncEnqueueOptions = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isMaineCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const opts = jobOptionsWithId(options.jobId);
  const queue = createMaineCandidateFinanceSyncSchedulerQueue();

  try {
    const job = await queue.add(
      MAINE_CANDIDATE_FINANCE_SYNC_JOB_NAME,
      {
        dryRun: Boolean(jobData.dryRun),
        force: Boolean(jobData.force),
        maxCandidates: jobData.maxCandidates,
        staleAfterDays: jobData.staleAfterDays,
        electionLookbackDays: jobData.electionLookbackDays,
        electionLookaheadDays: jobData.electionLookaheadDays,
        rawDataCacheDir: jobData.rawDataCacheDir,
        aiClassifyIndustries: Boolean(jobData.aiClassifyIndustries),
        aiClassificationMinAmount: jobData.aiClassificationMinAmount,
        triggeredBy: "manual",
        requestedAt: new Date().toISOString(),
      },
      opts
    );
    return job.id ?? "unknown";
  } finally {
    await queue.close();
  }
}

export async function runMaineCandidateFinanceSyncJob(
  data: MaineCandidateFinanceSyncJobData = {}
): Promise<MaineCandidateFinanceSyncJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const dryRun = Boolean(data.dryRun);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const now = new Date();
  const enabled = isMaineCampaignFinanceSyncEnabled(force);

  if (!data.triggeredBy) {
    console.warn("Maine finance sync job missing triggeredBy; recording as unknown");
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
      results: [],
    };
  }

  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  try {
    const result = await syncDueMaineCandidateFinance({
      db: pool,
      now,
      dryRun,
      maxCandidates: data.maxCandidates,
      staleAfterDays: data.staleAfterDays,
      electionLookbackDays: data.electionLookbackDays,
      electionLookaheadDays: data.electionLookaheadDays,
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

export function createMaineCandidateFinanceSyncSchedulerWorker(): Worker<
  MaineCandidateFinanceSyncJobData,
  MaineCandidateFinanceSyncJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<MaineCandidateFinanceSyncJobData, MaineCandidateFinanceSyncJobResult> = async (job) => {
    return runMaineCandidateFinanceSyncJob(job.data ?? {});
  };

  return new Worker<MaineCandidateFinanceSyncJobData, MaineCandidateFinanceSyncJobResult>(queueName, processor, {
    connection,
    concurrency: 1,
  });
}
