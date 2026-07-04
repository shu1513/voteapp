import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";
import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import {
  isArizonaCampaignFinanceEnabled,
  isArizonaCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  syncDueArizonaCandidateFinance,
  type ArizonaCandidateFinanceBatchSyncResult,
} from "../pipeline/arizonaFinance/arizonaCandidateFinanceBatchSync.js";

export const ARIZONA_CANDIDATE_FINANCE_SYNC_JOB_NAME = "arizona_candidate_finance_sync_due";
export const ARIZONA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID =
  "arizona_candidate_finance_sync_daily";

export type ArizonaCandidateFinanceSyncJobData = {
  dryRun?: boolean;
  force?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  timeoutMs?: number;
  directIncomeLimit?: number;
  independentExpenditureLimitPerPosition?: number;
  outsideGroupIncomeLimitPerGroup?: number;
  outsideMaxGroups?: number;
  directMaxBreakdownsPerCategory?: number;
  outsideMaxBreakdownsPerCategory?: number;
  minIndustryAmount?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type ArizonaCandidateFinanceSyncJobResult = ArizonaCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<ArizonaCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type ArizonaCandidateFinanceSyncEnqueueOptions = {
  jobId?: string;
};

type ArizonaCandidateFinanceSyncSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

const DISABLED_RESULT_DEFAULT_MAX_CANDIDATES = 0;
const DISABLED_RESULT_DEFAULT_STALE_AFTER_DAYS = 0;

function readSchedulerRuntimeConfig(): ArizonaCandidateFinanceSyncSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.ARIZONA_CANDIDATE_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() ||
      "arizona_candidate_finance_sync_maintenance",
    dailyCron: process.env.ARIZONA_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() || "35 9 * * *",
    dailyTz: process.env.ARIZONA_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  if (value !== undefined && (!Number.isInteger(value) || value <= 0)) {
    throw new Error(`Invalid Arizona candidate finance sync scheduler ${label}: ${value}`);
  }
}

function assertNonNegativeNumber(value: number | undefined, label: string): void {
  if (value !== undefined && (!Number.isFinite(value) || value < 0)) {
    throw new Error(`Invalid Arizona candidate finance sync scheduler ${label}: ${value}`);
  }
}

function assertValidJobOptions(data: ArizonaCandidateFinanceSyncJobData): void {
  assertPositiveInteger(data.maxCandidates, "maxCandidates");
  assertPositiveInteger(data.staleAfterDays, "staleAfterDays");
  assertPositiveInteger(data.electionLookbackDays, "electionLookbackDays");
  assertPositiveInteger(data.electionLookaheadDays, "electionLookaheadDays");
  assertPositiveInteger(data.timeoutMs, "timeoutMs");
  assertPositiveInteger(data.directIncomeLimit, "directIncomeLimit");
  assertPositiveInteger(data.independentExpenditureLimitPerPosition, "independentExpenditureLimitPerPosition");
  assertPositiveInteger(data.outsideGroupIncomeLimitPerGroup, "outsideGroupIncomeLimitPerGroup");
  assertPositiveInteger(data.outsideMaxGroups, "outsideMaxGroups");
  assertPositiveInteger(data.directMaxBreakdownsPerCategory, "directMaxBreakdownsPerCategory");
  assertPositiveInteger(data.outsideMaxBreakdownsPerCategory, "outsideMaxBreakdownsPerCategory");
  assertNonNegativeNumber(data.minIndustryAmount, "minIndustryAmount");
}

function defaultJobOptions(): JobsOptions {
  return {
    removeOnComplete: 1000,
    removeOnFail: 1000,
  };
}

function getQueueName(): string {
  return readSchedulerRuntimeConfig().queueName;
}

function getQueueConnection(): ConnectionOptions {
  return toConnectionOptions(getPipelineEnv().REDIS_URL);
}

export function createArizonaCandidateFinanceSyncSchedulerQueue(): Queue<ArizonaCandidateFinanceSyncJobData> {
  return new Queue<ArizonaCandidateFinanceSyncJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

function normalizeOptionalJobId(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  if (!trimmed) {
    return undefined;
  }
  if (trimmed.includes(":")) {
    throw new Error("Arizona candidate finance sync scheduler jobId must not contain ':'");
  }
  return trimmed;
}

function jobOptionsWithId(jobId: string | undefined): JobsOptions {
  const normalizedJobId = normalizeOptionalJobId(jobId);
  return normalizedJobId ? { ...defaultJobOptions(), jobId: normalizedJobId } : defaultJobOptions();
}

export function buildArizonaCandidateFinanceLinkedElectionSyncJobId(now = new Date()): string {
  if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
    throw new Error("Invalid Arizona candidate finance linked-election sync job date");
  }
  return `arizona-candidate-finance-linked-election-sync-${now.toISOString().slice(0, 10)}`;
}

export async function upsertRecurringArizonaCandidateFinanceSyncJobs(
  jobData: ArizonaCandidateFinanceSyncJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isArizonaCampaignFinanceEnabled()) {
    const queue = createArizonaCandidateFinanceSyncSchedulerQueue();
    try {
      await queue.removeJobScheduler(ARIZONA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const queue = createArizonaCandidateFinanceSyncSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      ARIZONA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: ARIZONA_CANDIDATE_FINANCE_SYNC_JOB_NAME,
        data: {
          dryRun: Boolean(jobData.dryRun),
          force: Boolean(jobData.force),
          maxCandidates: jobData.maxCandidates,
          staleAfterDays: jobData.staleAfterDays,
          electionLookbackDays: jobData.electionLookbackDays,
          electionLookaheadDays: jobData.electionLookaheadDays,
          timeoutMs: jobData.timeoutMs,
          directIncomeLimit: jobData.directIncomeLimit,
          independentExpenditureLimitPerPosition: jobData.independentExpenditureLimitPerPosition,
          outsideGroupIncomeLimitPerGroup: jobData.outsideGroupIncomeLimitPerGroup,
          outsideMaxGroups: jobData.outsideMaxGroups,
          directMaxBreakdownsPerCategory: jobData.directMaxBreakdownsPerCategory,
          outsideMaxBreakdownsPerCategory: jobData.outsideMaxBreakdownsPerCategory,
          minIndustryAmount: jobData.minIndustryAmount,
          triggeredBy: "daily",
        },
        opts: defaultJobOptions(),
      }
    );
  } finally {
    await queue.close();
  }
}

export async function enqueueManualArizonaCandidateFinanceSyncJob(
  jobData: ArizonaCandidateFinanceSyncJobData = {},
  options: ArizonaCandidateFinanceSyncEnqueueOptions = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isArizonaCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    return "disabled";
  }
  const queue = createArizonaCandidateFinanceSyncSchedulerQueue();
  try {
    const job = await queue.add(
      ARIZONA_CANDIDATE_FINANCE_SYNC_JOB_NAME,
      {
        dryRun: Boolean(jobData.dryRun),
        force: Boolean(jobData.force),
        maxCandidates: jobData.maxCandidates,
        staleAfterDays: jobData.staleAfterDays,
        electionLookbackDays: jobData.electionLookbackDays,
        electionLookaheadDays: jobData.electionLookaheadDays,
        timeoutMs: jobData.timeoutMs,
        directIncomeLimit: jobData.directIncomeLimit,
        independentExpenditureLimitPerPosition: jobData.independentExpenditureLimitPerPosition,
        outsideGroupIncomeLimitPerGroup: jobData.outsideGroupIncomeLimitPerGroup,
        outsideMaxGroups: jobData.outsideMaxGroups,
        directMaxBreakdownsPerCategory: jobData.directMaxBreakdownsPerCategory,
        outsideMaxBreakdownsPerCategory: jobData.outsideMaxBreakdownsPerCategory,
        minIndustryAmount: jobData.minIndustryAmount,
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

export async function runArizonaCandidateFinanceSyncJob(
  data: ArizonaCandidateFinanceSyncJobData = {}
): Promise<ArizonaCandidateFinanceSyncJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const dryRun = Boolean(data.dryRun);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const now = new Date();
  const enabled = isArizonaCampaignFinanceSyncEnabled(force);

  if (!data.triggeredBy) {
    console.warn("Arizona candidate finance sync job missing triggeredBy; recording as unknown");
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
    const result = await syncDueArizonaCandidateFinance({
      db: pool,
      now,
      dryRun,
      maxCandidates: data.maxCandidates,
      staleAfterDays: data.staleAfterDays,
      electionLookbackDays: data.electionLookbackDays,
      electionLookaheadDays: data.electionLookaheadDays,
      spotlightClientOptions: data.timeoutMs ? { timeoutMs: data.timeoutMs } : undefined,
      directIncomeLimit: data.directIncomeLimit,
      independentExpenditureLimitPerPosition: data.independentExpenditureLimitPerPosition,
      outsideGroupIncomeLimitPerGroup: data.outsideGroupIncomeLimitPerGroup,
      outsideMaxGroups: data.outsideMaxGroups,
      directMaxBreakdownsPerCategory: data.directMaxBreakdownsPerCategory,
      outsideMaxBreakdownsPerCategory: data.outsideMaxBreakdownsPerCategory,
      minIndustryAmount: data.minIndustryAmount,
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

export function createArizonaCandidateFinanceSyncSchedulerWorker(): Worker<
  ArizonaCandidateFinanceSyncJobData,
  ArizonaCandidateFinanceSyncJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<
    ArizonaCandidateFinanceSyncJobData,
    ArizonaCandidateFinanceSyncJobResult
  > = async (job) => {
    return runArizonaCandidateFinanceSyncJob(job.data ?? {});
  };

  return new Worker<ArizonaCandidateFinanceSyncJobData, ArizonaCandidateFinanceSyncJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
