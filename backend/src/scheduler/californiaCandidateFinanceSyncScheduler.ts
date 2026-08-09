import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";
import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import {
  isCaliforniaCampaignFinanceEnabled,
  isCaliforniaCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  syncDueCaliforniaCandidateFinance,
  type CaliforniaCandidateFinanceBatchSyncResult,
} from "../pipeline/californiaFinance/californiaCandidateFinanceBatchSync.js";

export const CALIFORNIA_CANDIDATE_FINANCE_SYNC_JOB_NAME = "california_candidate_finance_sync_due";
export const CALIFORNIA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID =
  "california_candidate_finance_sync_daily";

export type CaliforniaCandidateFinanceSyncJobData = {
  dryRun?: boolean;
  force?: boolean;
  includeOutside?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  timeoutMs?: number;
  rawDataZipPath?: string;
  rawDataCacheDir?: string;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type CaliforniaCandidateFinanceSyncJobResult = CaliforniaCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<CaliforniaCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type CaliforniaCandidateFinanceSyncEnqueueOptions = {
  jobId?: string;
};

type CaliforniaCandidateFinanceSyncSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

const DISABLED_RESULT_DEFAULT_MAX_CANDIDATES = 0;
const DISABLED_RESULT_DEFAULT_STALE_AFTER_DAYS = 0;

function readSchedulerRuntimeConfig(): CaliforniaCandidateFinanceSyncSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.CALIFORNIA_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() ||
      "california_candidate_finance_sync_maintenance",
    dailyCron: process.env.CALIFORNIA_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() || "55 8 * * *",
    dailyTz: process.env.CALIFORNIA_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  // isSafeInteger, not isInteger: Number("9007199254740993") silently rounds
  // to 2^53 and still passes isInteger.
  if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
    throw new Error(`Invalid California finance sync scheduler ${label}: ${value}`);
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
    throw new Error("California finance sync scheduler jobId must not contain ':'");
  }
  return trimmed;
}

function jobOptionsWithId(jobId: string | undefined): JobsOptions {
  const normalizedJobId = normalizeOptionalJobId(jobId);
  return normalizedJobId ? { ...defaultJobOptions(), jobId: normalizedJobId } : defaultJobOptions();
}

function assertValidJobOptions(data: CaliforniaCandidateFinanceSyncJobData): void {
  assertPositiveInteger(data.maxCandidates, "maxCandidates");
  assertPositiveInteger(data.staleAfterDays, "staleAfterDays");
  assertPositiveInteger(data.electionLookbackDays, "electionLookbackDays");
  assertPositiveInteger(data.electionLookaheadDays, "electionLookaheadDays");
  assertPositiveInteger(data.timeoutMs, "timeoutMs");
}

export function createCaliforniaCandidateFinanceSyncSchedulerQueue(): Queue<CaliforniaCandidateFinanceSyncJobData> {
  return new Queue<CaliforniaCandidateFinanceSyncJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export function buildCaliforniaCandidateFinanceLinkedElectionSyncJobId(now = new Date()): string {
  if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
    throw new Error("Invalid California finance linked-election sync job date");
  }
  return `california-candidate-finance-linked-election-sync-${now.toISOString().slice(0, 10)}`;
}

export async function upsertRecurringCaliforniaCandidateFinanceSyncJobs(
  jobData: CaliforniaCandidateFinanceSyncJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isCaliforniaCampaignFinanceEnabled()) {
    const queue = createCaliforniaCandidateFinanceSyncSchedulerQueue();
    try {
      await queue.removeJobScheduler(CALIFORNIA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const queue = createCaliforniaCandidateFinanceSyncSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      CALIFORNIA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: CALIFORNIA_CANDIDATE_FINANCE_SYNC_JOB_NAME,
        data: {
          dryRun: Boolean(jobData.dryRun),
          force: Boolean(jobData.force),
          includeOutside: jobData.includeOutside !== false,
          maxCandidates: jobData.maxCandidates,
          staleAfterDays: jobData.staleAfterDays,
          electionLookbackDays: jobData.electionLookbackDays,
          electionLookaheadDays: jobData.electionLookaheadDays,
          timeoutMs: jobData.timeoutMs,
          rawDataZipPath: jobData.rawDataZipPath,
          rawDataCacheDir: jobData.rawDataCacheDir,
          triggeredBy: "daily",
        },
        opts: defaultJobOptions(),
      }
    );
  } finally {
    await queue.close();
  }
}

export async function enqueueManualCaliforniaCandidateFinanceSyncJob(
  jobData: CaliforniaCandidateFinanceSyncJobData = {},
  options: CaliforniaCandidateFinanceSyncEnqueueOptions = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isCaliforniaCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const queue = createCaliforniaCandidateFinanceSyncSchedulerQueue();

  try {
    const job = await queue.add(
      CALIFORNIA_CANDIDATE_FINANCE_SYNC_JOB_NAME,
      {
        dryRun: Boolean(jobData.dryRun),
        force: Boolean(jobData.force),
        includeOutside: jobData.includeOutside !== false,
        maxCandidates: jobData.maxCandidates,
        staleAfterDays: jobData.staleAfterDays,
        electionLookbackDays: jobData.electionLookbackDays,
        electionLookaheadDays: jobData.electionLookaheadDays,
        timeoutMs: jobData.timeoutMs,
        rawDataZipPath: jobData.rawDataZipPath,
        rawDataCacheDir: jobData.rawDataCacheDir,
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

export async function runCaliforniaCandidateFinanceSyncJob(
  data: CaliforniaCandidateFinanceSyncJobData = {}
): Promise<CaliforniaCandidateFinanceSyncJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const dryRun = Boolean(data.dryRun);
  const includeOutside = data.includeOutside !== false;
  const triggeredBy = data.triggeredBy ?? "unknown";
  const now = new Date();
  const enabled = isCaliforniaCampaignFinanceSyncEnabled(force);

  if (!data.triggeredBy) {
    console.warn("California finance sync job missing triggeredBy; recording as unknown");
  }

  if (!enabled) {
    return {
      enabled: false,
      force,
      triggeredBy,
      dryRun,
      includeOutside,
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
    const result = await syncDueCaliforniaCandidateFinance({
      db: pool,
      now,
      dryRun,
      includeOutside,
      maxCandidates: data.maxCandidates,
      staleAfterDays: data.staleAfterDays,
      electionLookbackDays: data.electionLookbackDays,
      electionLookaheadDays: data.electionLookaheadDays,
      powerSearchOptions: data.timeoutMs ? { timeoutMs: data.timeoutMs } : undefined,
      rawDataZipPath: data.rawDataZipPath,
      rawDataCacheDir: data.rawDataCacheDir,
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

export function createCaliforniaCandidateFinanceSyncSchedulerWorker(): Worker<
  CaliforniaCandidateFinanceSyncJobData,
  CaliforniaCandidateFinanceSyncJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<
    CaliforniaCandidateFinanceSyncJobData,
    CaliforniaCandidateFinanceSyncJobResult
  > = async (job) => {
    return runCaliforniaCandidateFinanceSyncJob(job.data ?? {});
  };

  return new Worker<CaliforniaCandidateFinanceSyncJobData, CaliforniaCandidateFinanceSyncJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
