import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";
import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import { isOhioCampaignFinanceEnabled, isOhioCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueOhioCandidateFinance,
  type OhioCandidateFinanceBatchSyncResult,
} from "../pipeline/ohioFinance/ohioCandidateFinanceBatchSync.js";

export const OHIO_CANDIDATE_FINANCE_SYNC_JOB_NAME = "ohio_candidate_finance_sync_due";
export const OHIO_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID = "ohio_candidate_finance_sync_daily";

export type OhioCandidateFinanceSyncJobData = {
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

export type OhioCandidateFinanceSyncJobResult = OhioCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<OhioCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type OhioCandidateFinanceSyncEnqueueOptions = {
  jobId?: string;
};

type OhioCandidateFinanceSyncSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

const DISABLED_RESULT_DEFAULT_MAX_CANDIDATES = 0;
const DISABLED_RESULT_DEFAULT_STALE_AFTER_DAYS = 0;

function readSchedulerRuntimeConfig(): OhioCandidateFinanceSyncSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.OHIO_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() ||
      "ohio_candidate_finance_sync_maintenance",
    dailyCron: process.env.OHIO_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() || "55 9 * * *",
    dailyTz: process.env.OHIO_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  // isSafeInteger, not isInteger: Number("9007199254740993") silently rounds
  // to 2^53 and still passes isInteger.
  if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
    throw new Error(`Invalid Ohio finance sync scheduler ${label}: ${value}`);
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
    throw new Error("Ohio finance sync scheduler jobId must not contain ':'");
  }
  if (trimmed === "disabled" || trimmed === "unknown") {
    throw new Error("Ohio finance sync scheduler jobId uses a reserved value");
  }
  return trimmed;
}

function jobOptionsWithId(jobId: string | undefined): JobsOptions {
  const normalizedJobId = normalizeOptionalJobId(jobId);
  return normalizedJobId ? { ...defaultJobOptions(), jobId: normalizedJobId } : defaultJobOptions();
}

function assertValidJobOptions(data: OhioCandidateFinanceSyncJobData): void {
  assertPositiveInteger(data.maxCandidates, "maxCandidates");
  assertPositiveInteger(data.staleAfterDays, "staleAfterDays");
  assertPositiveInteger(data.electionLookbackDays, "electionLookbackDays");
  assertPositiveInteger(data.electionLookaheadDays, "electionLookaheadDays");
}

export function createOhioCandidateFinanceSyncSchedulerQueue(): Queue<OhioCandidateFinanceSyncJobData> {
  return new Queue<OhioCandidateFinanceSyncJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringOhioCandidateFinanceSyncJobs(
  jobData: OhioCandidateFinanceSyncJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isOhioCampaignFinanceEnabled()) {
    const queue = createOhioCandidateFinanceSyncSchedulerQueue();
    try {
      await queue.removeJobScheduler(OHIO_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const queue = createOhioCandidateFinanceSyncSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      OHIO_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: OHIO_CANDIDATE_FINANCE_SYNC_JOB_NAME,
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

export async function enqueueManualOhioCandidateFinanceSyncJob(
  jobData: OhioCandidateFinanceSyncJobData = {},
  options: OhioCandidateFinanceSyncEnqueueOptions = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isOhioCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const queue = createOhioCandidateFinanceSyncSchedulerQueue();

  try {
    const job = await queue.add(
      OHIO_CANDIDATE_FINANCE_SYNC_JOB_NAME,
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

export async function runOhioCandidateFinanceSyncJob(
  data: OhioCandidateFinanceSyncJobData = {}
): Promise<OhioCandidateFinanceSyncJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const dryRun = Boolean(data.dryRun);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const now = new Date();
  const enabled = isOhioCampaignFinanceSyncEnabled(force);

  if (!data.triggeredBy) {
    console.warn("Ohio finance sync job missing triggeredBy; recording as unknown");
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
    const result = await syncDueOhioCandidateFinance({
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

export function createOhioCandidateFinanceSyncSchedulerWorker(): Worker<
  OhioCandidateFinanceSyncJobData,
  OhioCandidateFinanceSyncJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<OhioCandidateFinanceSyncJobData, OhioCandidateFinanceSyncJobResult> = async (
    job
  ) => {
    return runOhioCandidateFinanceSyncJob(job.data ?? {});
  };

  return new Worker<OhioCandidateFinanceSyncJobData, OhioCandidateFinanceSyncJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
