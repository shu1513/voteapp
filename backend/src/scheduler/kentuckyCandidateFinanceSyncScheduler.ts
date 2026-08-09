import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";
import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import {
  isKentuckyCampaignFinanceEnabled,
  isKentuckyCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  syncDueKentuckyCandidateFinance,
  type KentuckyCandidateFinanceBatchSyncResult,
} from "../pipeline/kentuckyFinance/kentuckyCandidateFinanceBatchSync.js";

export const KENTUCKY_CANDIDATE_FINANCE_SYNC_JOB_NAME = "kentucky_candidate_finance_sync_due";
export const KENTUCKY_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID =
  "kentucky_candidate_finance_sync_daily";

export type KentuckyCandidateFinanceSyncJobData = {
  dryRun?: boolean;
  force?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  autoLinkMissingLinks?: boolean;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type KentuckyCandidateFinanceSyncJobResult = KentuckyCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<KentuckyCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type KentuckyCandidateFinanceSyncEnqueueOptions = {
  jobId?: string;
};

type KentuckyCandidateFinanceSyncSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

const DISABLED_RESULT_DEFAULT_MAX_CANDIDATES = 0;
const DISABLED_RESULT_DEFAULT_STALE_AFTER_DAYS = 0;

function readSchedulerRuntimeConfig(): KentuckyCandidateFinanceSyncSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.KENTUCKY_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() ||
      "kentucky_candidate_finance_sync_maintenance",
    dailyCron: process.env.KENTUCKY_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() || "35 9 * * *",
    dailyTz: process.env.KENTUCKY_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  // isSafeInteger, not isInteger: Number("9007199254740993") silently rounds
  // to 2^53 and still passes isInteger.
  if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
    throw new Error(`Invalid Kentucky finance sync scheduler ${label}: ${value}`);
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
    throw new Error("Kentucky finance sync scheduler jobId must not contain ':'");
  }
  return trimmed;
}

function jobOptionsWithId(jobId: string | undefined): JobsOptions {
  const normalizedJobId = normalizeOptionalJobId(jobId);
  return normalizedJobId ? { ...defaultJobOptions(), jobId: normalizedJobId } : defaultJobOptions();
}

function assertValidJobOptions(data: KentuckyCandidateFinanceSyncJobData): void {
  assertPositiveInteger(data.maxCandidates, "maxCandidates");
  assertPositiveInteger(data.staleAfterDays, "staleAfterDays");
  assertPositiveInteger(data.electionLookbackDays, "electionLookbackDays");
  assertPositiveInteger(data.electionLookaheadDays, "electionLookaheadDays");
}

function shouldAutoLinkMissingLinks(data: KentuckyCandidateFinanceSyncJobData): boolean {
  return data.autoLinkMissingLinks !== false;
}

export function createKentuckyCandidateFinanceSyncSchedulerQueue(): Queue<KentuckyCandidateFinanceSyncJobData> {
  return new Queue<KentuckyCandidateFinanceSyncJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export function buildKentuckyCandidateFinanceLinkedElectionSyncJobId(now = new Date()): string {
  if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
    throw new Error("Invalid Kentucky finance linked-election sync job date");
  }
  return `kentucky-candidate-finance-linked-election-sync-${now.toISOString().slice(0, 10)}`;
}

export async function upsertRecurringKentuckyCandidateFinanceSyncJobs(
  jobData: KentuckyCandidateFinanceSyncJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isKentuckyCampaignFinanceEnabled()) {
    const queue = createKentuckyCandidateFinanceSyncSchedulerQueue();
    try {
      await queue.removeJobScheduler(KENTUCKY_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const queue = createKentuckyCandidateFinanceSyncSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      KENTUCKY_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: KENTUCKY_CANDIDATE_FINANCE_SYNC_JOB_NAME,
        data: {
          dryRun: Boolean(jobData.dryRun),
          force: Boolean(jobData.force),
          maxCandidates: jobData.maxCandidates,
          staleAfterDays: jobData.staleAfterDays,
          electionLookbackDays: jobData.electionLookbackDays,
          electionLookaheadDays: jobData.electionLookaheadDays,
          autoLinkMissingLinks: shouldAutoLinkMissingLinks(jobData),
          triggeredBy: "daily",
        },
        opts: defaultJobOptions(),
      }
    );
  } finally {
    await queue.close();
  }
}

export async function enqueueManualKentuckyCandidateFinanceSyncJob(
  jobData: KentuckyCandidateFinanceSyncJobData = {},
  options: KentuckyCandidateFinanceSyncEnqueueOptions = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isKentuckyCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const queue = createKentuckyCandidateFinanceSyncSchedulerQueue();

  try {
    const job = await queue.add(
      KENTUCKY_CANDIDATE_FINANCE_SYNC_JOB_NAME,
      {
        dryRun: Boolean(jobData.dryRun),
        force: Boolean(jobData.force),
        maxCandidates: jobData.maxCandidates,
        staleAfterDays: jobData.staleAfterDays,
        electionLookbackDays: jobData.electionLookbackDays,
        electionLookaheadDays: jobData.electionLookaheadDays,
        autoLinkMissingLinks: shouldAutoLinkMissingLinks(jobData),
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

export async function runKentuckyCandidateFinanceSyncJob(
  data: KentuckyCandidateFinanceSyncJobData = {}
): Promise<KentuckyCandidateFinanceSyncJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const dryRun = Boolean(data.dryRun);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const now = new Date();
  const enabled = isKentuckyCampaignFinanceSyncEnabled(force);

  if (!data.triggeredBy) {
    console.warn("Kentucky finance sync job missing triggeredBy; recording as unknown");
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
      autoLinkFailedCount: 0,
      results: [],
    };
  }

  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  try {
    const result = await syncDueKentuckyCandidateFinance({
      db: pool,
      now,
      dryRun,
      maxCandidates: data.maxCandidates,
      staleAfterDays: data.staleAfterDays,
      electionLookbackDays: data.electionLookbackDays,
      electionLookaheadDays: data.electionLookaheadDays,
      autoLinkMissingLinks: shouldAutoLinkMissingLinks(data),
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

export function createKentuckyCandidateFinanceSyncSchedulerWorker(): Worker<
  KentuckyCandidateFinanceSyncJobData,
  KentuckyCandidateFinanceSyncJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<KentuckyCandidateFinanceSyncJobData, KentuckyCandidateFinanceSyncJobResult> = async (
    job
  ) => {
    return runKentuckyCandidateFinanceSyncJob(job.data ?? {});
  };

  return new Worker<KentuckyCandidateFinanceSyncJobData, KentuckyCandidateFinanceSyncJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
