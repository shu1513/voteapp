import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";
import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import { isMontanaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueMontanaCandidateFinance,
  type MontanaCandidateFinanceBatchSyncResult,
} from "../pipeline/montanaFinance/montanaCandidateFinanceBatchSync.js";

export const MONTANA_CANDIDATE_FINANCE_SYNC_JOB_NAME = "montana_candidate_finance_sync_due";
export const MONTANA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID = "montana_candidate_finance_sync_daily";

export type MontanaCandidateFinanceSyncJobData = {
  dryRun?: boolean;
  force?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type MontanaCandidateFinanceSyncJobResult = MontanaCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<MontanaCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type MontanaCandidateFinanceSyncEnqueueOptions = {
  jobId?: string;
};

type MontanaCandidateFinanceSyncSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

function readSchedulerRuntimeConfig(): MontanaCandidateFinanceSyncSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.MONTANA_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() ||
      "montana_candidate_finance_sync_maintenance",
    dailyCron: process.env.MONTANA_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() || "45 9 * * *",
    dailyTz: process.env.MONTANA_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  // isSafeInteger, not isInteger: Number("9007199254740993") silently rounds
  // to 2^53 and still passes isInteger.
  if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
    throw new Error(`Invalid Montana finance sync scheduler ${label}: ${value}`);
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
    throw new Error("Montana finance sync scheduler jobId must not contain ':'");
  }
  return trimmed;
}

function jobOptionsWithId(jobId: string | undefined): JobsOptions {
  const normalizedJobId = normalizeOptionalJobId(jobId);
  return normalizedJobId ? { ...defaultJobOptions(), jobId: normalizedJobId } : defaultJobOptions();
}

function assertValidJobOptions(data: MontanaCandidateFinanceSyncJobData): void {
  assertPositiveInteger(data.maxCandidates, "maxCandidates");
  assertPositiveInteger(data.staleAfterDays, "staleAfterDays");
  assertPositiveInteger(data.electionLookbackDays, "electionLookbackDays");
  assertPositiveInteger(data.electionLookaheadDays, "electionLookaheadDays");
}

export function createMontanaCandidateFinanceSyncSchedulerQueue(): Queue<MontanaCandidateFinanceSyncJobData> {
  return new Queue<MontanaCandidateFinanceSyncJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringMontanaCandidateFinanceSyncJobs(
  jobData: MontanaCandidateFinanceSyncJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isMontanaCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    const queue = createMontanaCandidateFinanceSyncSchedulerQueue();
    try {
      await queue.removeJobScheduler(MONTANA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const queue = createMontanaCandidateFinanceSyncSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      MONTANA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: MONTANA_CANDIDATE_FINANCE_SYNC_JOB_NAME,
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

export async function enqueueManualMontanaCandidateFinanceSyncJob(
  jobData: MontanaCandidateFinanceSyncJobData = {},
  options: MontanaCandidateFinanceSyncEnqueueOptions = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isMontanaCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const queue = createMontanaCandidateFinanceSyncSchedulerQueue();

  try {
    const job = await queue.add(
      MONTANA_CANDIDATE_FINANCE_SYNC_JOB_NAME,
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

export async function runMontanaCandidateFinanceSyncJob(
  data: MontanaCandidateFinanceSyncJobData = {}
): Promise<MontanaCandidateFinanceSyncJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const dryRun = Boolean(data.dryRun);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const now = new Date();
  const enabled = isMontanaCampaignFinanceSyncEnabled(force);

  if (!data.triggeredBy) {
    console.warn("Montana finance sync job missing triggeredBy; recording as unknown");
  }

  if (!enabled) {
    return {
      enabled: false,
      force,
      triggeredBy,
      dryRun,
      rawDataRefreshEnabled: false,
      autoLinkResults: [],
      totalDueRows: 0,
      attempted: 0,
      succeeded: 0,
      failed: 0,
      candidates: [],
    };
  }

  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  try {
    const result = await syncDueMontanaCandidateFinance({
      db: pool,
      now,
      dryRun,
      forceRawDataRefresh: force,
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

export function createMontanaCandidateFinanceSyncSchedulerWorker(): Worker<
  MontanaCandidateFinanceSyncJobData,
  MontanaCandidateFinanceSyncJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<MontanaCandidateFinanceSyncJobData, MontanaCandidateFinanceSyncJobResult> = async (
    job
  ) => {
    return runMontanaCandidateFinanceSyncJob(job.data ?? {});
  };

  return new Worker<MontanaCandidateFinanceSyncJobData, MontanaCandidateFinanceSyncJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
