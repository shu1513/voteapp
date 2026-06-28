import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import { isLouisianaCampaignFinanceSyncEnabled } from "../config/featureFlags.js";

export const LOUISIANA_CANDIDATE_FINANCE_SYNC_JOB_NAME = "louisiana_candidate_finance_sync_due";
export const LOUISIANA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID =
  "louisiana_candidate_finance_sync_daily";
const LOUISIANA_CANDIDATE_FINANCE_BATCH_SYNC_MODULE =
  "../pipeline/louisianaFinance/louisianaCandidateFinanceBatchSync.js";

export type LouisianaCandidateFinanceSyncJobData = {
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

export type LouisianaCandidateFinanceBatchSyncResult = {
  dryRun: boolean;
  now: string;
  staleAfterDays: number;
  maxCandidates: number;
  dueCandidateCount: number;
  selectedCandidateCount: number;
  syncedCandidateCount: number;
  failedCandidateCount: number;
  autoLinkAttemptedCount: number;
  autoLinkLinkedCount: number;
  results: unknown[];
};

export type LouisianaCandidateFinanceSyncJobResult = LouisianaCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<LouisianaCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type LouisianaCandidateFinanceSyncEnqueueOptions = {
  jobId?: string;
};

type LouisianaCandidateFinanceSyncSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

const DISABLED_RESULT_DEFAULT_MAX_CANDIDATES = 0;
const DISABLED_RESULT_DEFAULT_STALE_AFTER_DAYS = 0;

function readSchedulerRuntimeConfig(): LouisianaCandidateFinanceSyncSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.LOUISIANA_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() ||
      "louisiana_candidate_finance_sync_maintenance",
    dailyCron: process.env.LOUISIANA_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() || "55 9 * * *",
    dailyTz: process.env.LOUISIANA_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  if (value !== undefined && (!Number.isInteger(value) || value <= 0)) {
    throw new Error(`Invalid Louisiana finance sync scheduler ${label}: ${value}`);
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
    throw new Error("Louisiana finance sync scheduler jobId must not contain ':'");
  }
  return trimmed;
}

function jobOptionsWithId(jobId: string | undefined): JobsOptions {
  const normalizedJobId = normalizeOptionalJobId(jobId);
  return normalizedJobId ? { ...defaultJobOptions(), jobId: normalizedJobId } : defaultJobOptions();
}

function assertValidJobOptions(data: LouisianaCandidateFinanceSyncJobData): void {
  assertPositiveInteger(data.maxCandidates, "maxCandidates");
  assertPositiveInteger(data.staleAfterDays, "staleAfterDays");
  assertPositiveInteger(data.electionLookbackDays, "electionLookbackDays");
  assertPositiveInteger(data.electionLookaheadDays, "electionLookaheadDays");
}

async function loadLouisianaCandidateFinanceBatchSyncModule(): Promise<{
  syncDueLouisianaCandidateFinance: (input: {
    db: Pool;
    now: Date;
    dryRun: boolean;
    maxCandidates?: number;
    staleAfterDays?: number;
    electionLookbackDays?: number;
    electionLookaheadDays?: number;
    rawDataCacheDir?: string;
  }) => Promise<LouisianaCandidateFinanceBatchSyncResult>;
}> {
  return (await import(LOUISIANA_CANDIDATE_FINANCE_BATCH_SYNC_MODULE)) as {
    syncDueLouisianaCandidateFinance: (input: {
      db: Pool;
      now: Date;
      dryRun: boolean;
      maxCandidates?: number;
      staleAfterDays?: number;
      electionLookbackDays?: number;
      electionLookaheadDays?: number;
      rawDataCacheDir?: string;
    }) => Promise<LouisianaCandidateFinanceBatchSyncResult>;
  };
}

export function createLouisianaCandidateFinanceSyncSchedulerQueue(): Queue<LouisianaCandidateFinanceSyncJobData> {
  return new Queue<LouisianaCandidateFinanceSyncJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export function buildLouisianaCandidateFinanceLinkedElectionSyncJobId(now = new Date()): string {
  if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
    throw new Error("Invalid Louisiana finance linked-election sync job date");
  }
  return `louisiana-candidate-finance-linked-election-sync-${now.toISOString().slice(0, 10)}`;
}

export async function upsertRecurringLouisianaCandidateFinanceSyncJobs(
  jobData: LouisianaCandidateFinanceSyncJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isLouisianaCampaignFinanceSyncEnabled()) {
    const queue = createLouisianaCandidateFinanceSyncSchedulerQueue();
    try {
      await queue.removeJobScheduler(LOUISIANA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const queue = createLouisianaCandidateFinanceSyncSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      LOUISIANA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: LOUISIANA_CANDIDATE_FINANCE_SYNC_JOB_NAME,
        data: {
          dryRun: Boolean(jobData.dryRun),
          force: false,
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

export async function enqueueManualLouisianaCandidateFinanceSyncJob(
  jobData: LouisianaCandidateFinanceSyncJobData = {},
  options: LouisianaCandidateFinanceSyncEnqueueOptions = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isLouisianaCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const queue = createLouisianaCandidateFinanceSyncSchedulerQueue();

  try {
    const job = await queue.add(
      LOUISIANA_CANDIDATE_FINANCE_SYNC_JOB_NAME,
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

export async function runLouisianaCandidateFinanceSyncJob(
  data: LouisianaCandidateFinanceSyncJobData = {}
): Promise<LouisianaCandidateFinanceSyncJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const dryRun = Boolean(data.dryRun);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const now = new Date();
  const enabled = isLouisianaCampaignFinanceSyncEnabled(force);

  if (!data.triggeredBy) {
    console.warn("Louisiana finance sync job missing triggeredBy; recording as unknown");
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
  const { syncDueLouisianaCandidateFinance } = await loadLouisianaCandidateFinanceBatchSyncModule();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  try {
    const result = await syncDueLouisianaCandidateFinance({
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

export function createLouisianaCandidateFinanceSyncSchedulerWorker(): Worker<
  LouisianaCandidateFinanceSyncJobData,
  LouisianaCandidateFinanceSyncJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<
    LouisianaCandidateFinanceSyncJobData,
    LouisianaCandidateFinanceSyncJobResult
  > = async (job) => {
    return runLouisianaCandidateFinanceSyncJob(job.data ?? {});
  };

  return new Worker<LouisianaCandidateFinanceSyncJobData, LouisianaCandidateFinanceSyncJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
