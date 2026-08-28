import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";
import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import {
  isSouthCarolinaCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  syncDueSouthCarolinaCandidateFinance,
  type SouthCarolinaCandidateFinanceBatchSyncResult,
} from "../pipeline/southCarolinaFinance/southCarolinaCandidateFinanceBatchSync.js";

export const SOUTH_CAROLINA_CANDIDATE_FINANCE_SYNC_JOB_NAME = "south_carolina_candidate_finance_sync_due";
export const SOUTH_CAROLINA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID =
  "south_carolina_candidate_finance_sync_daily";

export type SouthCarolinaCandidateFinanceSyncJobData = {
  dryRun?: boolean;
  force?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type SouthCarolinaCandidateFinanceSyncJobResult = SouthCarolinaCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<SouthCarolinaCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type SouthCarolinaCandidateFinanceSyncEnqueueOptions = {
  jobId?: string;
};

type SouthCarolinaCandidateFinanceSyncSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

function readSchedulerRuntimeConfig(): SouthCarolinaCandidateFinanceSyncSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.SOUTH_CAROLINA_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() ||
      "south_carolina_candidate_finance_sync_maintenance",
    dailyCron: process.env.SOUTH_CAROLINA_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() || "35 9 * * *",
    dailyTz: process.env.SOUTH_CAROLINA_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  // isSafeInteger, not isInteger: Number("9007199254740993") silently rounds
  // to 2^53 and still passes isInteger.
  if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
    throw new Error(`Invalid South Carolina finance sync scheduler ${label}: ${value}`);
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
    throw new Error("South Carolina finance sync scheduler jobId must not contain ':'");
  }
  return trimmed;
}

function jobOptionsWithId(jobId: string | undefined): JobsOptions {
  const normalizedJobId = normalizeOptionalJobId(jobId);
  return normalizedJobId ? { ...defaultJobOptions(), jobId: normalizedJobId } : defaultJobOptions();
}

function assertValidJobOptions(data: SouthCarolinaCandidateFinanceSyncJobData): void {
  assertPositiveInteger(data.maxCandidates, "maxCandidates");
  assertPositiveInteger(data.staleAfterDays, "staleAfterDays");
  assertPositiveInteger(data.electionLookbackDays, "electionLookbackDays");
  assertPositiveInteger(data.electionLookaheadDays, "electionLookaheadDays");
}

export function createSouthCarolinaCandidateFinanceSyncSchedulerQueue(): Queue<SouthCarolinaCandidateFinanceSyncJobData> {
  return new Queue<SouthCarolinaCandidateFinanceSyncJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringSouthCarolinaCandidateFinanceSyncJobs(
  jobData: SouthCarolinaCandidateFinanceSyncJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isSouthCarolinaCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    const queue = createSouthCarolinaCandidateFinanceSyncSchedulerQueue();
    try {
      await queue.removeJobScheduler(SOUTH_CAROLINA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const queue = createSouthCarolinaCandidateFinanceSyncSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      SOUTH_CAROLINA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: SOUTH_CAROLINA_CANDIDATE_FINANCE_SYNC_JOB_NAME,
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

export async function enqueueManualSouthCarolinaCandidateFinanceSyncJob(
  jobData: SouthCarolinaCandidateFinanceSyncJobData = {},
  options: SouthCarolinaCandidateFinanceSyncEnqueueOptions = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isSouthCarolinaCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const queue = createSouthCarolinaCandidateFinanceSyncSchedulerQueue();

  try {
    const job = await queue.add(
      SOUTH_CAROLINA_CANDIDATE_FINANCE_SYNC_JOB_NAME,
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

export async function runSouthCarolinaCandidateFinanceSyncJob(
  data: SouthCarolinaCandidateFinanceSyncJobData = {}
): Promise<SouthCarolinaCandidateFinanceSyncJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const dryRun = Boolean(data.dryRun);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const now = new Date();
  const enabled = isSouthCarolinaCampaignFinanceSyncEnabled(force);

  if (!data.triggeredBy) {
    console.warn("South Carolina finance sync job missing triggeredBy; recording as unknown");
  }

  if (!enabled) {
    return {
      enabled: false,
      force,
      triggeredBy,
      dryRun,
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
    const result = await syncDueSouthCarolinaCandidateFinance({
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

export function createSouthCarolinaCandidateFinanceSyncSchedulerWorker(): Worker<
  SouthCarolinaCandidateFinanceSyncJobData,
  SouthCarolinaCandidateFinanceSyncJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<
    SouthCarolinaCandidateFinanceSyncJobData,
    SouthCarolinaCandidateFinanceSyncJobResult
  > = async (job) => {
    return runSouthCarolinaCandidateFinanceSyncJob(job.data ?? {});
  };

  return new Worker<SouthCarolinaCandidateFinanceSyncJobData, SouthCarolinaCandidateFinanceSyncJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
