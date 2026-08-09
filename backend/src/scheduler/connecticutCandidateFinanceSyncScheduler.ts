import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";
import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import {
  isConnecticutCampaignFinanceEnabled,
  isConnecticutCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  syncDueConnecticutCandidateFinance,
  type ConnecticutCandidateFinanceBatchSyncResult,
} from "../pipeline/connecticutFinance/connecticutCandidateFinanceBatchSync.js";

export const CONNECTICUT_CANDIDATE_FINANCE_SYNC_JOB_NAME = "connecticut_candidate_finance_sync_due";
export const CONNECTICUT_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID =
  "connecticut_candidate_finance_sync_daily";

export type ConnecticutCandidateFinanceSyncJobData = {
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

export type ConnecticutCandidateFinanceSyncJobResult = ConnecticutCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<ConnecticutCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type ConnecticutCandidateFinanceSyncEnqueueOptions = {
  jobId?: string;
};

type ConnecticutCandidateFinanceSyncSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

const DISABLED_RESULT_DEFAULT_MAX_CANDIDATES = 0;
const DISABLED_RESULT_DEFAULT_STALE_AFTER_DAYS = 0;

function readSchedulerRuntimeConfig(): ConnecticutCandidateFinanceSyncSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.CONNECTICUT_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() ||
      "connecticut_candidate_finance_sync_maintenance",
    dailyCron: process.env.CONNECTICUT_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() || "35 9 * * *",
    dailyTz: process.env.CONNECTICUT_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  // isSafeInteger, not isInteger: Number("9007199254740993") silently rounds
  // to 2^53 and still passes isInteger.
  if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
    throw new Error(`Invalid Connecticut finance sync scheduler ${label}: ${value}`);
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
    throw new Error("Connecticut finance sync scheduler jobId must not contain ':'");
  }
  return trimmed;
}

function jobOptionsWithId(jobId: string | undefined): JobsOptions {
  const normalizedJobId = normalizeOptionalJobId(jobId);
  return normalizedJobId ? { ...defaultJobOptions(), jobId: normalizedJobId } : defaultJobOptions();
}

function assertValidJobOptions(data: ConnecticutCandidateFinanceSyncJobData): void {
  assertPositiveInteger(data.maxCandidates, "maxCandidates");
  assertPositiveInteger(data.staleAfterDays, "staleAfterDays");
  assertPositiveInteger(data.electionLookbackDays, "electionLookbackDays");
  assertPositiveInteger(data.electionLookaheadDays, "electionLookaheadDays");
}

export function createConnecticutCandidateFinanceSyncSchedulerQueue(): Queue<ConnecticutCandidateFinanceSyncJobData> {
  return new Queue<ConnecticutCandidateFinanceSyncJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export function buildConnecticutCandidateFinanceLinkedElectionSyncJobId(now = new Date()): string {
  if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
    throw new Error("Invalid Connecticut finance linked-election sync job date");
  }
  return `connecticut-candidate-finance-linked-election-sync-${now.toISOString().slice(0, 10)}`;
}

export async function upsertRecurringConnecticutCandidateFinanceSyncJobs(
  jobData: ConnecticutCandidateFinanceSyncJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isConnecticutCampaignFinanceEnabled()) {
    const queue = createConnecticutCandidateFinanceSyncSchedulerQueue();
    try {
      await queue.removeJobScheduler(CONNECTICUT_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const queue = createConnecticutCandidateFinanceSyncSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      CONNECTICUT_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: CONNECTICUT_CANDIDATE_FINANCE_SYNC_JOB_NAME,
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

export async function enqueueManualConnecticutCandidateFinanceSyncJob(
  jobData: ConnecticutCandidateFinanceSyncJobData = {},
  options: ConnecticutCandidateFinanceSyncEnqueueOptions = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isConnecticutCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const queue = createConnecticutCandidateFinanceSyncSchedulerQueue();

  try {
    const job = await queue.add(
      CONNECTICUT_CANDIDATE_FINANCE_SYNC_JOB_NAME,
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

export async function runConnecticutCandidateFinanceSyncJob(
  data: ConnecticutCandidateFinanceSyncJobData = {}
): Promise<ConnecticutCandidateFinanceSyncJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const dryRun = Boolean(data.dryRun);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const now = new Date();
  const enabled = isConnecticutCampaignFinanceSyncEnabled(force);

  if (!data.triggeredBy) {
    console.warn("Connecticut finance sync job missing triggeredBy; recording as unknown");
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
    const result = await syncDueConnecticutCandidateFinance({
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

export function createConnecticutCandidateFinanceSyncSchedulerWorker(): Worker<
  ConnecticutCandidateFinanceSyncJobData,
  ConnecticutCandidateFinanceSyncJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<
    ConnecticutCandidateFinanceSyncJobData,
    ConnecticutCandidateFinanceSyncJobResult
  > = async (job) => {
    return runConnecticutCandidateFinanceSyncJob(job.data ?? {});
  };

  return new Worker<ConnecticutCandidateFinanceSyncJobData, ConnecticutCandidateFinanceSyncJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
