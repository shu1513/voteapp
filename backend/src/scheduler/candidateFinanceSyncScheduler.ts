import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import { isCandidateFinanceEnabled, isCandidateFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueCandidateFinance,
  type CandidateFinanceBatchSyncResult,
} from "../pipeline/finance/candidateFinanceBatchSync.js";
import { DEFAULT_OPEN_FEC_TIMEOUT_MS, readOpenFecApiKeysFromEnv } from "../pipeline/presidential/openFecClient.js";

export const CANDIDATE_FINANCE_SYNC_JOB_NAME = "candidate_finance_sync_due";
export const CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID = "candidate_finance_sync_daily";

export type CandidateFinanceSyncJobData = {
  dryRun?: boolean;
  force?: boolean;
  includeOutside?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  perPage?: number;
  outsideGroupLimit?: number;
  timeoutMs?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type CandidateFinanceSyncJobResult = CandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<CandidateFinanceSyncJobData["triggeredBy"]>;
};

type CandidateFinanceSyncSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

const DISABLED_RESULT_DEFAULT_MAX_CANDIDATES = 0;
const DISABLED_RESULT_DEFAULT_STALE_AFTER_DAYS = 0;

function readSchedulerRuntimeConfig(): CandidateFinanceSyncSchedulerRuntimeConfig {
  return {
    queueName: process.env.CANDIDATE_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() || "candidate_finance_sync_maintenance",
    dailyCron: process.env.CANDIDATE_FINANCE_SYNC_DAILY_CRON?.trim() || "45 8 * * *",
    dailyTz: process.env.CANDIDATE_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  if (value !== undefined && (!Number.isInteger(value) || value <= 0)) {
    throw new Error(`Invalid candidate finance sync scheduler ${label}: ${value}`);
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

function assertValidJobOptions(data: CandidateFinanceSyncJobData): void {
  assertPositiveInteger(data.maxCandidates, "maxCandidates");
  assertPositiveInteger(data.staleAfterDays, "staleAfterDays");
  assertPositiveInteger(data.electionLookbackDays, "electionLookbackDays");
  assertPositiveInteger(data.electionLookaheadDays, "electionLookaheadDays");
  assertPositiveInteger(data.perPage, "perPage");
  assertPositiveInteger(data.outsideGroupLimit, "outsideGroupLimit");
  assertPositiveInteger(data.timeoutMs, "timeoutMs");
}

export function createCandidateFinanceSyncSchedulerQueue(): Queue<CandidateFinanceSyncJobData> {
  return new Queue<CandidateFinanceSyncJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringCandidateFinanceSyncJobs(
  jobData: CandidateFinanceSyncJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isCandidateFinanceEnabled()) {
    const queue = createCandidateFinanceSyncSchedulerQueue();
    try {
      await queue.removeJobScheduler(CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const queue = createCandidateFinanceSyncSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: CANDIDATE_FINANCE_SYNC_JOB_NAME,
        data: {
          dryRun: Boolean(jobData.dryRun),
          force: Boolean(jobData.force),
          includeOutside: Boolean(jobData.includeOutside),
          maxCandidates: jobData.maxCandidates,
          staleAfterDays: jobData.staleAfterDays,
          electionLookbackDays: jobData.electionLookbackDays,
          electionLookaheadDays: jobData.electionLookaheadDays,
          perPage: jobData.perPage,
          outsideGroupLimit: jobData.outsideGroupLimit,
          timeoutMs: jobData.timeoutMs,
          triggeredBy: "daily",
        },
        opts: defaultJobOptions(),
      }
    );
  } finally {
    await queue.close();
  }
}

export async function enqueueManualCandidateFinanceSyncJob(
  jobData: CandidateFinanceSyncJobData = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isCandidateFinanceEnabled()) {
    return "disabled";
  }

  const queue = createCandidateFinanceSyncSchedulerQueue();

  try {
    const job = await queue.add(
      CANDIDATE_FINANCE_SYNC_JOB_NAME,
      {
        dryRun: Boolean(jobData.dryRun),
        force: Boolean(jobData.force),
        includeOutside: Boolean(jobData.includeOutside),
        maxCandidates: jobData.maxCandidates,
        staleAfterDays: jobData.staleAfterDays,
        electionLookbackDays: jobData.electionLookbackDays,
        electionLookaheadDays: jobData.electionLookaheadDays,
        perPage: jobData.perPage,
        outsideGroupLimit: jobData.outsideGroupLimit,
        timeoutMs: jobData.timeoutMs,
        triggeredBy: "manual",
        requestedAt: new Date().toISOString(),
      },
      defaultJobOptions()
    );
    return job.id ?? "unknown";
  } finally {
    await queue.close();
  }
}

export async function runCandidateFinanceSyncJob(
  data: CandidateFinanceSyncJobData = {}
): Promise<CandidateFinanceSyncJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const dryRun = Boolean(data.dryRun);
  const includeOutside = Boolean(data.includeOutside);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const now = new Date();
  const enabled = isCandidateFinanceSyncEnabled(force);

  if (!data.triggeredBy) {
    console.warn("candidate_finance sync job missing triggeredBy; recording as unknown");
  }

  if (!enabled) {
    return {
      enabled: false,
      force,
      triggeredBy,
      dryRun,
      includeOutside,
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
  const apiKeys = readOpenFecApiKeysFromEnv();
  if (apiKeys.length === 0) {
    throw new Error("No OpenFEC API keys configured. Set FEC_API_KEY_1 or FEC_API_KEY.");
  }

  const pool = new Pool({ connectionString: env.DATABASE_URL });
  try {
    const result = await syncDueCandidateFinance({
      db: pool,
      openFecOptions: { apiKeys, timeoutMs: data.timeoutMs ?? DEFAULT_OPEN_FEC_TIMEOUT_MS },
      now,
      dryRun,
      includeOutside,
      maxCandidates: data.maxCandidates,
      staleAfterDays: data.staleAfterDays,
      electionLookbackDays: data.electionLookbackDays,
      electionLookaheadDays: data.electionLookaheadDays,
      perPage: data.perPage,
      outsideGroupLimit: data.outsideGroupLimit,
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

export function createCandidateFinanceSyncSchedulerWorker(): Worker<
  CandidateFinanceSyncJobData,
  CandidateFinanceSyncJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<CandidateFinanceSyncJobData, CandidateFinanceSyncJobResult> = async (job) => {
    return runCandidateFinanceSyncJob(job.data ?? {});
  };

  return new Worker<CandidateFinanceSyncJobData, CandidateFinanceSyncJobResult>(queueName, processor, {
    connection,
    concurrency: 1,
  });
}
