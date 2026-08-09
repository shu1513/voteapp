import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";
import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import {
  isColoradoCampaignFinanceEnabled,
  isColoradoCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  syncDueColoradoCandidateFinance,
  type ColoradoCandidateFinanceBatchSyncResult,
} from "../pipeline/coloradoFinance/coloradoCandidateFinanceBatchSync.js";

export const COLORADO_CANDIDATE_FINANCE_SYNC_JOB_NAME = "colorado_candidate_finance_sync_due";
export const COLORADO_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID =
  "colorado_candidate_finance_sync_daily";

export type ColoradoCandidateFinanceSyncJobData = {
  dryRun?: boolean;
  force?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  rawDataZipPath?: string;
  rawDataCacheDir?: string;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type ColoradoCandidateFinanceSyncJobResult = ColoradoCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<ColoradoCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type ColoradoCandidateFinanceSyncEnqueueOptions = {
  jobId?: string;
};

type ColoradoCandidateFinanceSyncSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

const DISABLED_RESULT_DEFAULT_MAX_CANDIDATES = 0;
const DISABLED_RESULT_DEFAULT_STALE_AFTER_DAYS = 0;

function readSchedulerRuntimeConfig(): ColoradoCandidateFinanceSyncSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.COLORADO_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() ||
      "colorado_candidate_finance_sync_maintenance",
    dailyCron: process.env.COLORADO_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() || "10 9 * * *",
    dailyTz: process.env.COLORADO_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  // isSafeInteger, not isInteger: Number("9007199254740993") silently rounds
  // to 2^53 and still passes isInteger.
  if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
    throw new Error(`Invalid Colorado finance sync scheduler ${label}: ${value}`);
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
    throw new Error("Colorado finance sync scheduler jobId must not contain ':'");
  }
  return trimmed;
}

function jobOptionsWithId(jobId: string | undefined): JobsOptions {
  const normalizedJobId = normalizeOptionalJobId(jobId);
  return normalizedJobId ? { ...defaultJobOptions(), jobId: normalizedJobId } : defaultJobOptions();
}

function assertValidJobOptions(data: ColoradoCandidateFinanceSyncJobData): void {
  assertPositiveInteger(data.maxCandidates, "maxCandidates");
  assertPositiveInteger(data.staleAfterDays, "staleAfterDays");
  assertPositiveInteger(data.electionLookbackDays, "electionLookbackDays");
  assertPositiveInteger(data.electionLookaheadDays, "electionLookaheadDays");
}

export function createColoradoCandidateFinanceSyncSchedulerQueue(): Queue<ColoradoCandidateFinanceSyncJobData> {
  return new Queue<ColoradoCandidateFinanceSyncJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export function buildColoradoCandidateFinanceLinkedElectionSyncJobId(now = new Date()): string {
  if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
    throw new Error("Invalid Colorado finance linked-election sync job date");
  }
  return `colorado-candidate-finance-linked-election-sync-${now.toISOString().slice(0, 10)}`;
}

export async function upsertRecurringColoradoCandidateFinanceSyncJobs(
  jobData: ColoradoCandidateFinanceSyncJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isColoradoCampaignFinanceEnabled()) {
    const queue = createColoradoCandidateFinanceSyncSchedulerQueue();
    try {
      await queue.removeJobScheduler(COLORADO_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const queue = createColoradoCandidateFinanceSyncSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      COLORADO_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: COLORADO_CANDIDATE_FINANCE_SYNC_JOB_NAME,
        data: {
          dryRun: Boolean(jobData.dryRun),
          force: Boolean(jobData.force),
          maxCandidates: jobData.maxCandidates,
          staleAfterDays: jobData.staleAfterDays,
          electionLookbackDays: jobData.electionLookbackDays,
          electionLookaheadDays: jobData.electionLookaheadDays,
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

export async function enqueueManualColoradoCandidateFinanceSyncJob(
  jobData: ColoradoCandidateFinanceSyncJobData = {},
  options: ColoradoCandidateFinanceSyncEnqueueOptions = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isColoradoCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const queue = createColoradoCandidateFinanceSyncSchedulerQueue();

  try {
    const job = await queue.add(
      COLORADO_CANDIDATE_FINANCE_SYNC_JOB_NAME,
      {
        dryRun: Boolean(jobData.dryRun),
        force: Boolean(jobData.force),
        maxCandidates: jobData.maxCandidates,
        staleAfterDays: jobData.staleAfterDays,
        electionLookbackDays: jobData.electionLookbackDays,
        electionLookaheadDays: jobData.electionLookaheadDays,
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

export async function runColoradoCandidateFinanceSyncJob(
  data: ColoradoCandidateFinanceSyncJobData = {}
): Promise<ColoradoCandidateFinanceSyncJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const dryRun = Boolean(data.dryRun);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const now = new Date();
  const enabled = isColoradoCampaignFinanceSyncEnabled(force);

  if (!data.triggeredBy) {
    console.warn("Colorado finance sync job missing triggeredBy; recording as unknown");
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
    const result = await syncDueColoradoCandidateFinance({
      db: pool,
      now,
      dryRun,
      maxCandidates: data.maxCandidates,
      staleAfterDays: data.staleAfterDays,
      electionLookbackDays: data.electionLookbackDays,
      electionLookaheadDays: data.electionLookaheadDays,
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

export function createColoradoCandidateFinanceSyncSchedulerWorker(): Worker<
  ColoradoCandidateFinanceSyncJobData,
  ColoradoCandidateFinanceSyncJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<
    ColoradoCandidateFinanceSyncJobData,
    ColoradoCandidateFinanceSyncJobResult
  > = async (job) => {
    return runColoradoCandidateFinanceSyncJob(job.data ?? {});
  };

  return new Worker<ColoradoCandidateFinanceSyncJobData, ColoradoCandidateFinanceSyncJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
