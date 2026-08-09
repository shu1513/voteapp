import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";
import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import {
  isNorthCarolinaCampaignFinanceEnabled,
  isNorthCarolinaCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  syncDueNorthCarolinaCandidateFinance,
  type NorthCarolinaCandidateFinanceBatchSyncResult,
} from "../pipeline/northCarolinaFinance/northCarolinaCandidateFinanceBatchSync.js";

export const NORTH_CAROLINA_CANDIDATE_FINANCE_SYNC_JOB_NAME = "north_carolina_candidate_finance_sync_due";
export const NORTH_CAROLINA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID =
  "north_carolina_candidate_finance_sync_daily";

export type NorthCarolinaCandidateFinanceSyncJobData = {
  dryRun?: boolean;
  force?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  rawDataCacheDir?: string;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type NorthCarolinaCandidateFinanceSyncJobResult = NorthCarolinaCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<NorthCarolinaCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type NorthCarolinaCandidateFinanceSyncEnqueueOptions = {
  jobId?: string;
};

type NorthCarolinaCandidateFinanceSyncSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

const DISABLED_RESULT_DEFAULT_MAX_CANDIDATES = 0;
const DISABLED_RESULT_DEFAULT_STALE_AFTER_DAYS = 0;

function readSchedulerRuntimeConfig(): NorthCarolinaCandidateFinanceSyncSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.NORTH_CAROLINA_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() ||
      "north_carolina_candidate_finance_sync_maintenance",
    // Offset from Ohio's 09:55 so the two state syncs never stack on one
    // worker host.
    dailyCron: process.env.NORTH_CAROLINA_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() || "25 10 * * *",
    dailyTz: process.env.NORTH_CAROLINA_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  // isSafeInteger, not isInteger: Number("9007199254740993") silently rounds
  // to 2^53 and still passes isInteger.
  if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
    throw new Error(`Invalid North Carolina finance sync scheduler ${label}: ${value}`);
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
    throw new Error("North Carolina finance sync scheduler jobId must not contain ':'");
  }
  if (trimmed === "disabled" || trimmed === "unknown") {
    throw new Error("North Carolina finance sync scheduler jobId uses a reserved value");
  }
  return trimmed;
}

function jobOptionsWithId(jobId: string | undefined): JobsOptions {
  const normalizedJobId = normalizeOptionalJobId(jobId);
  return normalizedJobId ? { ...defaultJobOptions(), jobId: normalizedJobId } : defaultJobOptions();
}

function assertValidJobOptions(data: NorthCarolinaCandidateFinanceSyncJobData): void {
  assertPositiveInteger(data.maxCandidates, "maxCandidates");
  assertPositiveInteger(data.staleAfterDays, "staleAfterDays");
  assertPositiveInteger(data.electionLookbackDays, "electionLookbackDays");
  assertPositiveInteger(data.electionLookaheadDays, "electionLookaheadDays");
}

export function createNorthCarolinaCandidateFinanceSyncSchedulerQueue(): Queue<NorthCarolinaCandidateFinanceSyncJobData> {
  return new Queue<NorthCarolinaCandidateFinanceSyncJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringNorthCarolinaCandidateFinanceSyncJobs(
  jobData: NorthCarolinaCandidateFinanceSyncJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isNorthCarolinaCampaignFinanceEnabled()) {
    const queue = createNorthCarolinaCandidateFinanceSyncSchedulerQueue();
    try {
      await queue.removeJobScheduler(NORTH_CAROLINA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const queue = createNorthCarolinaCandidateFinanceSyncSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      NORTH_CAROLINA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: NORTH_CAROLINA_CANDIDATE_FINANCE_SYNC_JOB_NAME,
        data: {
          dryRun: Boolean(jobData.dryRun),
          force: Boolean(jobData.force),
          maxCandidates: jobData.maxCandidates,
          staleAfterDays: jobData.staleAfterDays,
          electionLookbackDays: jobData.electionLookbackDays,
          electionLookaheadDays: jobData.electionLookaheadDays,
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

export async function enqueueManualNorthCarolinaCandidateFinanceSyncJob(
  jobData: NorthCarolinaCandidateFinanceSyncJobData = {},
  options: NorthCarolinaCandidateFinanceSyncEnqueueOptions = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isNorthCarolinaCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const queue = createNorthCarolinaCandidateFinanceSyncSchedulerQueue();

  try {
    const job = await queue.add(
      NORTH_CAROLINA_CANDIDATE_FINANCE_SYNC_JOB_NAME,
      {
        dryRun: Boolean(jobData.dryRun),
        force: Boolean(jobData.force),
        maxCandidates: jobData.maxCandidates,
        staleAfterDays: jobData.staleAfterDays,
        electionLookbackDays: jobData.electionLookbackDays,
        electionLookaheadDays: jobData.electionLookaheadDays,
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

export async function runNorthCarolinaCandidateFinanceSyncJob(
  data: NorthCarolinaCandidateFinanceSyncJobData = {}
): Promise<NorthCarolinaCandidateFinanceSyncJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const dryRun = Boolean(data.dryRun);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const now = new Date();
  const enabled = isNorthCarolinaCampaignFinanceSyncEnabled(force);

  if (!data.triggeredBy) {
    console.warn("North Carolina finance sync job missing triggeredBy; recording as unknown");
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
      outsideAggregationByYear: [],
      results: [],
    };
  }

  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  try {
    const result = await syncDueNorthCarolinaCandidateFinance({
      db: pool,
      now,
      dryRun,
      maxCandidates: data.maxCandidates,
      staleAfterDays: data.staleAfterDays,
      electionLookbackDays: data.electionLookbackDays,
      electionLookaheadDays: data.electionLookaheadDays,
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

export function createNorthCarolinaCandidateFinanceSyncSchedulerWorker(): Worker<
  NorthCarolinaCandidateFinanceSyncJobData,
  NorthCarolinaCandidateFinanceSyncJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<
    NorthCarolinaCandidateFinanceSyncJobData,
    NorthCarolinaCandidateFinanceSyncJobResult
  > = async (job) => {
    return runNorthCarolinaCandidateFinanceSyncJob(job.data ?? {});
  };

  return new Worker<NorthCarolinaCandidateFinanceSyncJobData, NorthCarolinaCandidateFinanceSyncJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
