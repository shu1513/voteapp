import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";
import { Pool } from "pg";

import { createFinanceIndustryClassifierFromEnv } from "../ai/classifyFinanceIndustry.js";
import { getPipelineEnv } from "../config/env.js";
import {
  isTennesseeCampaignFinanceEnabled,
  isTennesseeCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  syncDueTennesseeCandidateFinance,
  type TennesseeCandidateFinanceBatchSyncResult,
} from "../pipeline/tennesseeFinance/tennesseeCandidateFinanceBatchSync.js";

export const TENNESSEE_CANDIDATE_FINANCE_SYNC_JOB_NAME = "tennessee_candidate_finance_sync_due";
export const TENNESSEE_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID =
  "tennessee_candidate_finance_sync_daily";

export type TennesseeCandidateFinanceSyncJobData = {
  dryRun?: boolean;
  force?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  aiClassifyIndustries?: boolean;
  aiClassificationMinAmount?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type TennesseeCandidateFinanceSyncJobResult = TennesseeCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<TennesseeCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type TennesseeCandidateFinanceSyncEnqueueOptions = {
  jobId?: string;
};

type TennesseeCandidateFinanceSyncSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

const DISABLED_RESULT_DEFAULT_MAX_CANDIDATES = 0;
const DISABLED_RESULT_DEFAULT_STALE_AFTER_DAYS = 0;

function readSchedulerRuntimeConfig(): TennesseeCandidateFinanceSyncSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.TENNESSEE_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() ||
      "tennessee_candidate_finance_sync_maintenance",
    dailyCron: process.env.TENNESSEE_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() || "35 9 * * *",
    dailyTz: process.env.TENNESSEE_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  if (value !== undefined && (!Number.isInteger(value) || value <= 0)) {
    throw new Error(`Invalid Tennessee finance sync scheduler ${label}: ${value}`);
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
    throw new Error("Tennessee finance sync scheduler jobId must not contain ':'");
  }
  return trimmed;
}

function jobOptionsWithId(jobId: string | undefined): JobsOptions {
  const normalizedJobId = normalizeOptionalJobId(jobId);
  return normalizedJobId ? { ...defaultJobOptions(), jobId: normalizedJobId } : defaultJobOptions();
}

function assertValidJobOptions(data: TennesseeCandidateFinanceSyncJobData): void {
  assertPositiveInteger(data.maxCandidates, "maxCandidates");
  assertPositiveInteger(data.staleAfterDays, "staleAfterDays");
  assertPositiveInteger(data.electionLookbackDays, "electionLookbackDays");
  assertPositiveInteger(data.electionLookaheadDays, "electionLookaheadDays");
  assertPositiveInteger(data.aiClassificationMinAmount, "aiClassificationMinAmount");
}

export function createTennesseeCandidateFinanceSyncSchedulerQueue(): Queue<TennesseeCandidateFinanceSyncJobData> {
  return new Queue<TennesseeCandidateFinanceSyncJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export function buildTennesseeCandidateFinanceLinkedElectionSyncJobId(now = new Date()): string {
  if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
    throw new Error("Invalid Tennessee finance linked-election sync job date");
  }
  return `tennessee-candidate-finance-linked-election-sync-${now.toISOString().slice(0, 10)}`;
}

export async function upsertRecurringTennesseeCandidateFinanceSyncJobs(
  jobData: TennesseeCandidateFinanceSyncJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isTennesseeCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    const queue = createTennesseeCandidateFinanceSyncSchedulerQueue();
    try {
      await queue.removeJobScheduler(TENNESSEE_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const queue = createTennesseeCandidateFinanceSyncSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      TENNESSEE_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: TENNESSEE_CANDIDATE_FINANCE_SYNC_JOB_NAME,
        data: {
          dryRun: Boolean(jobData.dryRun),
          force: Boolean(jobData.force),
          maxCandidates: jobData.maxCandidates,
          staleAfterDays: jobData.staleAfterDays,
          electionLookbackDays: jobData.electionLookbackDays,
          electionLookaheadDays: jobData.electionLookaheadDays,
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

export async function enqueueManualTennesseeCandidateFinanceSyncJob(
  jobData: TennesseeCandidateFinanceSyncJobData = {},
  options: TennesseeCandidateFinanceSyncEnqueueOptions = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isTennesseeCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const queue = createTennesseeCandidateFinanceSyncSchedulerQueue();

  try {
    const job = await queue.add(
      TENNESSEE_CANDIDATE_FINANCE_SYNC_JOB_NAME,
      {
        dryRun: Boolean(jobData.dryRun),
        force: Boolean(jobData.force),
        maxCandidates: jobData.maxCandidates,
        staleAfterDays: jobData.staleAfterDays,
        electionLookbackDays: jobData.electionLookbackDays,
        electionLookaheadDays: jobData.electionLookaheadDays,
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

export async function runTennesseeCandidateFinanceSyncJob(
  data: TennesseeCandidateFinanceSyncJobData = {}
): Promise<TennesseeCandidateFinanceSyncJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const dryRun = Boolean(data.dryRun);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const now = new Date();
  const enabled = isTennesseeCampaignFinanceSyncEnabled(force);

  if (!data.triggeredBy) {
    console.warn("Tennessee finance sync job missing triggeredBy; recording as unknown");
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
    const result = await syncDueTennesseeCandidateFinance({
      db: pool,
      now,
      dryRun,
      maxCandidates: data.maxCandidates,
      staleAfterDays: data.staleAfterDays,
      electionLookbackDays: data.electionLookbackDays,
      electionLookaheadDays: data.electionLookaheadDays,
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

export function createTennesseeCandidateFinanceSyncSchedulerWorker(): Worker<
  TennesseeCandidateFinanceSyncJobData,
  TennesseeCandidateFinanceSyncJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<TennesseeCandidateFinanceSyncJobData, TennesseeCandidateFinanceSyncJobResult> = async (
    job
  ) => {
    return runTennesseeCandidateFinanceSyncJob(job.data ?? {});
  };

  return new Worker<TennesseeCandidateFinanceSyncJobData, TennesseeCandidateFinanceSyncJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
