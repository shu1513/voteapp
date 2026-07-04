import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";
import { Pool } from "pg";

import { createFinanceIndustryClassifierFromEnv } from "../ai/classifyFinanceIndustry.js";
import { getPipelineEnv } from "../config/env.js";
import {
  isDistrictOfColumbiaCampaignFinanceEnabled,
  isDistrictOfColumbiaCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  syncDueDistrictOfColumbiaCandidateFinance,
  type DistrictOfColumbiaCandidateFinanceBatchSyncResult,
} from "../pipeline/districtOfColumbiaFinance/districtOfColumbiaCandidateFinanceBatchSync.js";

export const DISTRICT_OF_COLUMBIA_CANDIDATE_FINANCE_SYNC_JOB_NAME =
  "district_of_columbia_candidate_finance_sync_due";
export const DISTRICT_OF_COLUMBIA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID =
  "district_of_columbia_candidate_finance_sync_daily";

export type DistrictOfColumbiaCandidateFinanceSyncJobData = {
  dryRun?: boolean;
  force?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  aiClassifyIndustries?: boolean;
  aiClassificationMinAmount?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type DistrictOfColumbiaCandidateFinanceSyncJobResult = DistrictOfColumbiaCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<DistrictOfColumbiaCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type DistrictOfColumbiaCandidateFinanceSyncEnqueueOptions = {
  jobId?: string;
};

type DistrictOfColumbiaCandidateFinanceSyncSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

const DISABLED_RESULT_DEFAULT_MAX_CANDIDATES = 0;
const DISABLED_RESULT_DEFAULT_STALE_AFTER_DAYS = 0;

function readSchedulerRuntimeConfig(): DistrictOfColumbiaCandidateFinanceSyncSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.DISTRICT_OF_COLUMBIA_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() ||
      "district_of_columbia_candidate_finance_sync_maintenance",
    dailyCron: process.env.DISTRICT_OF_COLUMBIA_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() || "45 9 * * *",
    dailyTz: process.env.DISTRICT_OF_COLUMBIA_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  if (value !== undefined && (!Number.isInteger(value) || value <= 0)) {
    throw new Error(`Invalid D.C. finance sync scheduler ${label}: ${value}`);
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
    throw new Error("D.C. finance sync scheduler jobId must not contain ':'");
  }
  return trimmed;
}

function jobOptionsWithId(jobId: string | undefined): JobsOptions {
  const normalizedJobId = normalizeOptionalJobId(jobId);
  return normalizedJobId ? { ...defaultJobOptions(), jobId: normalizedJobId } : defaultJobOptions();
}

function assertValidJobOptions(data: DistrictOfColumbiaCandidateFinanceSyncJobData): void {
  assertPositiveInteger(data.maxCandidates, "maxCandidates");
  assertPositiveInteger(data.staleAfterDays, "staleAfterDays");
  assertPositiveInteger(data.electionLookbackDays, "electionLookbackDays");
  assertPositiveInteger(data.electionLookaheadDays, "electionLookaheadDays");
  assertPositiveInteger(data.aiClassificationMinAmount, "aiClassificationMinAmount");
}

export function createDistrictOfColumbiaCandidateFinanceSyncSchedulerQueue(): Queue<DistrictOfColumbiaCandidateFinanceSyncJobData> {
  return new Queue<DistrictOfColumbiaCandidateFinanceSyncJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export function buildDistrictOfColumbiaCandidateFinanceLinkedElectionSyncJobId(now = new Date()): string {
  if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
    throw new Error("Invalid D.C. finance linked-election sync job date");
  }
  return `district-of-columbia-candidate-finance-linked-election-sync-${now.toISOString().slice(0, 10)}`;
}

export async function upsertRecurringDistrictOfColumbiaCandidateFinanceSyncJobs(
  jobData: DistrictOfColumbiaCandidateFinanceSyncJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isDistrictOfColumbiaCampaignFinanceEnabled()) {
    const queue = createDistrictOfColumbiaCandidateFinanceSyncSchedulerQueue();
    try {
      await queue.removeJobScheduler(DISTRICT_OF_COLUMBIA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const queue = createDistrictOfColumbiaCandidateFinanceSyncSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      DISTRICT_OF_COLUMBIA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: DISTRICT_OF_COLUMBIA_CANDIDATE_FINANCE_SYNC_JOB_NAME,
        data: {
          dryRun: Boolean(jobData.dryRun),
          force: Boolean(jobData.force),
          maxCandidates: jobData.maxCandidates,
          staleAfterDays: jobData.staleAfterDays,
          electionLookbackDays: jobData.electionLookbackDays,
          electionLookaheadDays: jobData.electionLookaheadDays,
          aiClassifyIndustries: Boolean(jobData.aiClassifyIndustries),
          aiClassificationMinAmount: jobData.aiClassificationMinAmount,
          triggeredBy: "daily",
        },
        opts: defaultJobOptions(),
      }
    );
  } finally {
    await queue.close();
  }
}

export async function enqueueManualDistrictOfColumbiaCandidateFinanceSyncJob(
  jobData: DistrictOfColumbiaCandidateFinanceSyncJobData = {},
  options: DistrictOfColumbiaCandidateFinanceSyncEnqueueOptions = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isDistrictOfColumbiaCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const queue = createDistrictOfColumbiaCandidateFinanceSyncSchedulerQueue();

  try {
    const job = await queue.add(
      DISTRICT_OF_COLUMBIA_CANDIDATE_FINANCE_SYNC_JOB_NAME,
      {
        dryRun: Boolean(jobData.dryRun),
        force: Boolean(jobData.force),
        maxCandidates: jobData.maxCandidates,
        staleAfterDays: jobData.staleAfterDays,
        electionLookbackDays: jobData.electionLookbackDays,
        electionLookaheadDays: jobData.electionLookaheadDays,
        aiClassifyIndustries: Boolean(jobData.aiClassifyIndustries),
        aiClassificationMinAmount: jobData.aiClassificationMinAmount,
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

export async function runDistrictOfColumbiaCandidateFinanceSyncJob(
  data: DistrictOfColumbiaCandidateFinanceSyncJobData = {}
): Promise<DistrictOfColumbiaCandidateFinanceSyncJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const dryRun = Boolean(data.dryRun);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const now = new Date();
  const enabled = isDistrictOfColumbiaCampaignFinanceSyncEnabled(force);

  if (!data.triggeredBy) {
    console.warn("D.C. finance sync job missing triggeredBy; recording as unknown");
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
    const result = await syncDueDistrictOfColumbiaCandidateFinance({
      db: pool,
      now,
      dryRun,
      maxCandidates: data.maxCandidates,
      staleAfterDays: data.staleAfterDays,
      electionLookbackDays: data.electionLookbackDays,
      electionLookaheadDays: data.electionLookaheadDays,
      financeIndustryClassifier:
        data.aiClassifyIndustries && !dryRun ? createFinanceIndustryClassifierFromEnv() : undefined,
      aiClassificationMinAmount: data.aiClassificationMinAmount,
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

export function createDistrictOfColumbiaCandidateFinanceSyncSchedulerWorker(): Worker<
  DistrictOfColumbiaCandidateFinanceSyncJobData,
  DistrictOfColumbiaCandidateFinanceSyncJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<
    DistrictOfColumbiaCandidateFinanceSyncJobData,
    DistrictOfColumbiaCandidateFinanceSyncJobResult
  > = async (job) => {
    return runDistrictOfColumbiaCandidateFinanceSyncJob(job.data ?? {});
  };

  return new Worker<DistrictOfColumbiaCandidateFinanceSyncJobData, DistrictOfColumbiaCandidateFinanceSyncJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
