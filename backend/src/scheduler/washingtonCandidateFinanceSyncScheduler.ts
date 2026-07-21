import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";
import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import {
  isWashingtonCampaignFinanceEnabled,
  isWashingtonCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  syncDueWashingtonCandidateFinance,
  type WashingtonCandidateFinanceBatchSyncResult,
} from "../pipeline/washingtonFinance/washingtonCandidateFinanceBatchSync.js";

export const WASHINGTON_CANDIDATE_FINANCE_SYNC_JOB_NAME = "washington_candidate_finance_sync_due";
export const WASHINGTON_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID =
  "washington_candidate_finance_sync_daily";

export type WashingtonCandidateFinanceSyncJobData = {
  dryRun?: boolean;
  force?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type WashingtonCandidateFinanceSyncJobResult = WashingtonCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<WashingtonCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type WashingtonCandidateFinanceSyncEnqueueOptions = {
  jobId?: string;
};

type WashingtonCandidateFinanceSyncSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

const DISABLED_RESULT_DEFAULT_MAX_CANDIDATES = 0;
const DISABLED_RESULT_DEFAULT_STALE_AFTER_DAYS = 0;

function readSchedulerRuntimeConfig(): WashingtonCandidateFinanceSyncSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.WASHINGTON_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() ||
      "washington_candidate_finance_sync_maintenance",
    dailyCron: process.env.WASHINGTON_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() || "25 9 * * *",
    dailyTz: process.env.WASHINGTON_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  if (value !== undefined && (!Number.isInteger(value) || value <= 0)) {
    throw new Error(`Invalid Washington finance sync scheduler ${label}: ${value}`);
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
    throw new Error("Washington finance sync scheduler jobId must not contain ':'");
  }
  return trimmed;
}

function jobOptionsWithId(jobId: string | undefined): JobsOptions {
  const normalizedJobId = normalizeOptionalJobId(jobId);
  return normalizedJobId ? { ...defaultJobOptions(), jobId: normalizedJobId } : defaultJobOptions();
}

function assertValidJobOptions(data: WashingtonCandidateFinanceSyncJobData): void {
  assertPositiveInteger(data.maxCandidates, "maxCandidates");
  assertPositiveInteger(data.staleAfterDays, "staleAfterDays");
  assertPositiveInteger(data.electionLookbackDays, "electionLookbackDays");
  assertPositiveInteger(data.electionLookaheadDays, "electionLookaheadDays");
}

export function createWashingtonCandidateFinanceSyncSchedulerQueue(): Queue<WashingtonCandidateFinanceSyncJobData> {
  return new Queue<WashingtonCandidateFinanceSyncJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export function buildWashingtonCandidateFinanceLinkedElectionSyncJobId(now = new Date()): string {
  if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
    throw new Error("Invalid Washington finance linked-election sync job date");
  }
  return `washington-candidate-finance-linked-election-sync-${now.toISOString().slice(0, 10)}`;
}

export async function upsertRecurringWashingtonCandidateFinanceSyncJobs(
  jobData: WashingtonCandidateFinanceSyncJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isWashingtonCampaignFinanceEnabled()) {
    const queue = createWashingtonCandidateFinanceSyncSchedulerQueue();
    try {
      await queue.removeJobScheduler(WASHINGTON_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const queue = createWashingtonCandidateFinanceSyncSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      WASHINGTON_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: WASHINGTON_CANDIDATE_FINANCE_SYNC_JOB_NAME,
        data: {
          dryRun: Boolean(jobData.dryRun),
          force: Boolean(jobData.force),
          maxCandidates: jobData.maxCandidates,
          staleAfterDays: jobData.staleAfterDays,
          electionLookbackDays: jobData.electionLookbackDays,
          electionLookaheadDays: jobData.electionLookaheadDays,
          triggeredBy: "daily",
        },
        opts: defaultJobOptions(),
      }
    );
  } finally {
    await queue.close();
  }
}

export async function enqueueManualWashingtonCandidateFinanceSyncJob(
  jobData: WashingtonCandidateFinanceSyncJobData = {},
  options: WashingtonCandidateFinanceSyncEnqueueOptions = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isWashingtonCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const queue = createWashingtonCandidateFinanceSyncSchedulerQueue();

  try {
    const job = await queue.add(
      WASHINGTON_CANDIDATE_FINANCE_SYNC_JOB_NAME,
      {
        dryRun: Boolean(jobData.dryRun),
        force: Boolean(jobData.force),
        maxCandidates: jobData.maxCandidates,
        staleAfterDays: jobData.staleAfterDays,
        electionLookbackDays: jobData.electionLookbackDays,
        electionLookaheadDays: jobData.electionLookaheadDays,
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

export async function runWashingtonCandidateFinanceSyncJob(
  data: WashingtonCandidateFinanceSyncJobData = {}
): Promise<WashingtonCandidateFinanceSyncJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const dryRun = Boolean(data.dryRun);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const now = new Date();
  const enabled = isWashingtonCampaignFinanceSyncEnabled(force);

  if (!data.triggeredBy) {
    console.warn("Washington finance sync job missing triggeredBy; recording as unknown");
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
    const result = await syncDueWashingtonCandidateFinance({
      db: pool,
      now,
      dryRun,
      maxCandidates: data.maxCandidates,
      staleAfterDays: data.staleAfterDays,
      electionLookbackDays: data.electionLookbackDays,
      electionLookaheadDays: data.electionLookaheadDays,
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

export function createWashingtonCandidateFinanceSyncSchedulerWorker(): Worker<
  WashingtonCandidateFinanceSyncJobData,
  WashingtonCandidateFinanceSyncJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<
    WashingtonCandidateFinanceSyncJobData,
    WashingtonCandidateFinanceSyncJobResult
  > = async (job) => {
    return runWashingtonCandidateFinanceSyncJob(job.data ?? {});
  };

  return new Worker<WashingtonCandidateFinanceSyncJobData, WashingtonCandidateFinanceSyncJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
