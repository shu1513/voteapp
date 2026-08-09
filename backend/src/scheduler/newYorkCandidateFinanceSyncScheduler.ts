import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";
import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import {
  isNewYorkCampaignFinanceEnabled,
  isNewYorkCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  syncDueNewYorkCandidateFinance,
  type NewYorkCandidateFinanceBatchSyncResult,
} from "../pipeline/newYorkFinance/newYorkCandidateFinanceBatchSync.js";

export const NEW_YORK_CANDIDATE_FINANCE_SYNC_JOB_NAME = "new_york_candidate_finance_sync_due";
export const NEW_YORK_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID =
  "new_york_candidate_finance_sync_daily";

export type NewYorkCandidateFinanceSyncJobData = {
  dryRun?: boolean;
  force?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type NewYorkCandidateFinanceSyncJobResult = NewYorkCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<NewYorkCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type NewYorkCandidateFinanceSyncEnqueueOptions = {
  jobId?: string;
};

type NewYorkCandidateFinanceSyncSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

const DISABLED_RESULT_DEFAULT_MAX_CANDIDATES = 0;
const DISABLED_RESULT_DEFAULT_STALE_AFTER_DAYS = 0;

function readSchedulerRuntimeConfig(): NewYorkCandidateFinanceSyncSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.NEW_YORK_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() ||
      "new_york_candidate_finance_sync_maintenance",
    dailyCron: process.env.NEW_YORK_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() || "40 9 * * *",
    dailyTz: process.env.NEW_YORK_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  // isSafeInteger, not isInteger: Number("9007199254740993") silently rounds
  // to 2^53 and still passes isInteger.
  if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
    throw new Error(`Invalid New York finance sync scheduler ${label}: ${value}`);
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
    throw new Error("New York finance sync scheduler jobId must not contain ':'");
  }
  return trimmed;
}

function jobOptionsWithId(jobId: string | undefined): JobsOptions {
  const normalizedJobId = normalizeOptionalJobId(jobId);
  return normalizedJobId ? { ...defaultJobOptions(), jobId: normalizedJobId } : defaultJobOptions();
}

function assertValidJobOptions(data: NewYorkCandidateFinanceSyncJobData): void {
  assertPositiveInteger(data.maxCandidates, "maxCandidates");
  assertPositiveInteger(data.staleAfterDays, "staleAfterDays");
  assertPositiveInteger(data.electionLookbackDays, "electionLookbackDays");
  assertPositiveInteger(data.electionLookaheadDays, "electionLookaheadDays");
}

export function createNewYorkCandidateFinanceSyncSchedulerQueue(): Queue<NewYorkCandidateFinanceSyncJobData> {
  return new Queue<NewYorkCandidateFinanceSyncJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export function buildNewYorkCandidateFinanceLinkedElectionSyncJobId(now = new Date()): string {
  if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
    throw new Error("Invalid New York finance linked-election sync job date");
  }
  return `new-york-candidate-finance-linked-election-sync-${now.toISOString().slice(0, 10)}`;
}

export async function upsertRecurringNewYorkCandidateFinanceSyncJobs(
  jobData: NewYorkCandidateFinanceSyncJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isNewYorkCampaignFinanceEnabled()) {
    const queue = createNewYorkCandidateFinanceSyncSchedulerQueue();
    try {
      await queue.removeJobScheduler(NEW_YORK_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const queue = createNewYorkCandidateFinanceSyncSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      NEW_YORK_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: NEW_YORK_CANDIDATE_FINANCE_SYNC_JOB_NAME,
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

export async function enqueueManualNewYorkCandidateFinanceSyncJob(
  jobData: NewYorkCandidateFinanceSyncJobData = {},
  options: NewYorkCandidateFinanceSyncEnqueueOptions = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isNewYorkCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const queue = createNewYorkCandidateFinanceSyncSchedulerQueue();

  try {
    const job = await queue.add(
      NEW_YORK_CANDIDATE_FINANCE_SYNC_JOB_NAME,
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

export async function runNewYorkCandidateFinanceSyncJob(
  data: NewYorkCandidateFinanceSyncJobData = {}
): Promise<NewYorkCandidateFinanceSyncJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const dryRun = Boolean(data.dryRun);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const now = new Date();
  const enabled = isNewYorkCampaignFinanceSyncEnabled(force);

  if (!data.triggeredBy) {
    console.warn("New York finance sync job missing triggeredBy; recording as unknown");
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
    const result = await syncDueNewYorkCandidateFinance({
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

export function createNewYorkCandidateFinanceSyncSchedulerWorker(): Worker<
  NewYorkCandidateFinanceSyncJobData,
  NewYorkCandidateFinanceSyncJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<
    NewYorkCandidateFinanceSyncJobData,
    NewYorkCandidateFinanceSyncJobResult
  > = async (job) => {
    return runNewYorkCandidateFinanceSyncJob(job.data ?? {});
  };

  return new Worker<NewYorkCandidateFinanceSyncJobData, NewYorkCandidateFinanceSyncJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
