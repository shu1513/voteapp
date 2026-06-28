import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { Pool } from "pg";

import { createFinanceIndustryClassifierFromEnv } from "../ai/classifyFinanceIndustry.js";
import { getPipelineEnv } from "../config/env.js";
import { isIllinoisCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueIllinoisCandidateFinance,
  type IllinoisCandidateFinanceBatchSyncResult,
} from "../pipeline/illinoisFinance/illinoisCandidateFinanceBatchSync.js";

export const ILLINOIS_CANDIDATE_FINANCE_SYNC_JOB_NAME = "illinois_candidate_finance_sync_due";
export const ILLINOIS_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID = "illinois_candidate_finance_sync_daily";

export type IllinoisCandidateFinanceSyncJobData = {
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

export type IllinoisCandidateFinanceSyncJobResult = IllinoisCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<IllinoisCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type IllinoisCandidateFinanceSyncEnqueueOptions = {
  jobId?: string;
};

type IllinoisCandidateFinanceSyncSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

const DISABLED_RESULT_DEFAULT_MAX_CANDIDATES = 0;
const DISABLED_RESULT_DEFAULT_STALE_AFTER_DAYS = 0;

function readSchedulerRuntimeConfig(): IllinoisCandidateFinanceSyncSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() ||
      "illinois_candidate_finance_sync_maintenance",
    dailyCron: process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() || "30 9 * * *",
    dailyTz: process.env.ILLINOIS_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  if (value !== undefined && (!Number.isInteger(value) || value <= 0)) {
    throw new Error(`Invalid Illinois finance sync scheduler ${label}: ${value}`);
  }
}

function toConnectionOptions(redisUrl: string): ConnectionOptions {
  const parsed = new URL(redisUrl);
  if (parsed.protocol !== "redis:" && parsed.protocol !== "rediss:") {
    throw new Error(`Unsupported REDIS_URL protocol: ${parsed.protocol}`);
  }
  const parsedPort = parsed.port ? Number.parseInt(parsed.port, 10) : 6379;
  const dbSegment = parsed.pathname.length > 1 ? parsed.pathname.slice(1) : "0";
  if (!/^\d+$/.test(dbSegment)) {
    throw new Error(`Invalid REDIS_URL db index: ${parsed.pathname}`);
  }
  const parsedDb = Number(dbSegment);

  if (!Number.isInteger(parsedPort) || parsedPort <= 0) {
    throw new Error(`Invalid REDIS_URL port: ${parsed.port}`);
  }
  if (!Number.isInteger(parsedDb) || parsedDb < 0) {
    throw new Error(`Invalid REDIS_URL db index: ${parsed.pathname}`);
  }

  const opts: ConnectionOptions = {
    host: parsed.hostname,
    port: parsedPort,
    db: parsedDb,
    maxRetriesPerRequest: null,
  };
  if (parsed.username) {
    opts.username = decodeURIComponent(parsed.username);
  }
  if (parsed.password) {
    opts.password = decodeURIComponent(parsed.password);
  }
  if (parsed.protocol === "rediss:") {
    opts.tls = {};
  }
  return opts;
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
    throw new Error("Illinois finance sync scheduler jobId must not contain ':'");
  }
  return trimmed;
}

function jobOptionsWithId(jobId: string | undefined): JobsOptions {
  const normalizedJobId = normalizeOptionalJobId(jobId);
  return normalizedJobId ? { ...defaultJobOptions(), jobId: normalizedJobId } : defaultJobOptions();
}

function assertValidJobOptions(data: IllinoisCandidateFinanceSyncJobData): void {
  assertPositiveInteger(data.maxCandidates, "maxCandidates");
  assertPositiveInteger(data.staleAfterDays, "staleAfterDays");
  assertPositiveInteger(data.electionLookbackDays, "electionLookbackDays");
  assertPositiveInteger(data.electionLookaheadDays, "electionLookaheadDays");
  assertPositiveInteger(data.aiClassificationMinAmount, "aiClassificationMinAmount");
}

export function createIllinoisCandidateFinanceSyncSchedulerQueue(): Queue<IllinoisCandidateFinanceSyncJobData> {
  return new Queue<IllinoisCandidateFinanceSyncJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export function buildIllinoisCandidateFinanceLinkedElectionSyncJobId(now = new Date()): string {
  if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
    throw new Error("Invalid Illinois finance linked-election sync job date");
  }
  return `illinois-candidate-finance-linked-election-sync-${now.toISOString().slice(0, 10)}`;
}

export async function upsertRecurringIllinoisCandidateFinanceSyncJobs(
  jobData: IllinoisCandidateFinanceSyncJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isIllinoisCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    const queue = createIllinoisCandidateFinanceSyncSchedulerQueue();
    try {
      await queue.removeJobScheduler(ILLINOIS_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const queue = createIllinoisCandidateFinanceSyncSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      ILLINOIS_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: ILLINOIS_CANDIDATE_FINANCE_SYNC_JOB_NAME,
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

export async function enqueueManualIllinoisCandidateFinanceSyncJob(
  jobData: IllinoisCandidateFinanceSyncJobData = {},
  options: IllinoisCandidateFinanceSyncEnqueueOptions = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isIllinoisCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const queue = createIllinoisCandidateFinanceSyncSchedulerQueue();

  try {
    const job = await queue.add(
      ILLINOIS_CANDIDATE_FINANCE_SYNC_JOB_NAME,
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

export async function runIllinoisCandidateFinanceSyncJob(
  data: IllinoisCandidateFinanceSyncJobData = {}
): Promise<IllinoisCandidateFinanceSyncJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const dryRun = Boolean(data.dryRun);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const now = new Date();
  const enabled = isIllinoisCampaignFinanceSyncEnabled(force);

  if (!data.triggeredBy) {
    console.warn("Illinois finance sync job missing triggeredBy; recording as unknown");
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
    const result = await syncDueIllinoisCandidateFinance({
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

export function createIllinoisCandidateFinanceSyncSchedulerWorker(): Worker<
  IllinoisCandidateFinanceSyncJobData,
  IllinoisCandidateFinanceSyncJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<IllinoisCandidateFinanceSyncJobData, IllinoisCandidateFinanceSyncJobResult> = async (
    job
  ) => {
    return runIllinoisCandidateFinanceSyncJob(job.data ?? {});
  };

  return new Worker<IllinoisCandidateFinanceSyncJobData, IllinoisCandidateFinanceSyncJobResult>(queueName, processor, {
    connection,
    concurrency: 1,
  });
}
