import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { Pool } from "pg";

import { createFinanceIndustryClassifierFromEnv } from "../ai/classifyFinanceIndustry.js";
import { getPipelineEnv } from "../config/env.js";
import {
  isFloridaCampaignFinanceEnabled,
  isFloridaCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  syncDueFloridaCandidateFinance,
  syncFloridaCandidateFinanceBatch,
  type FloridaCandidateFinanceBatchSyncItemInput,
  type FloridaCandidateFinanceBatchSyncResult,
  type FloridaCandidateFinanceDueSyncResult,
} from "../pipeline/floridaFinance/floridaCandidateFinanceBatchSync.js";

export const FLORIDA_CANDIDATE_FINANCE_SYNC_JOB_NAME = "florida_candidate_finance_sync_due";
export const FLORIDA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID =
  "florida_candidate_finance_sync_daily";

export type FloridaCandidateFinanceSyncJobData = {
  dryRun?: boolean;
  force?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  syncInputs?: readonly FloridaCandidateFinanceBatchSyncItemInput[];
  defaultArtifactCacheDir?: string | null;
  refreshExportArtifacts?: boolean;
  exportMinIntervalMs?: number;
  exportRowLimit?: number;
  aiClassifyIndustries?: boolean;
  aiClassificationMinAmount?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type FloridaCandidateFinanceSyncJobResult = (FloridaCandidateFinanceBatchSyncResult | FloridaCandidateFinanceDueSyncResult) & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<FloridaCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type FloridaCandidateFinanceSyncEnqueueOptions = {
  jobId?: string;
};

type FloridaCandidateFinanceSyncSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

const DISABLED_RESULT_DEFAULT_MAX_CANDIDATES = 0;

function readSchedulerRuntimeConfig(): FloridaCandidateFinanceSyncSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.FLORIDA_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() ||
      "florida_candidate_finance_sync_maintenance",
    dailyCron: process.env.FLORIDA_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() || "25 9 * * *",
    dailyTz: process.env.FLORIDA_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  if (value !== undefined && (!Number.isInteger(value) || value <= 0)) {
    throw new Error(`Invalid Florida finance sync scheduler ${label}: ${value}`);
  }
}

function assertNonnegativeInteger(value: number | undefined, label: string): void {
  if (value !== undefined && (!Number.isInteger(value) || value < 0)) {
    throw new Error(`Invalid Florida finance sync scheduler ${label}: ${value}`);
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
    throw new Error("Florida finance sync scheduler jobId must not contain ':'");
  }
  return trimmed;
}

function jobOptionsWithId(jobId: string | undefined): JobsOptions {
  const normalizedJobId = normalizeOptionalJobId(jobId);
  return normalizedJobId ? { ...defaultJobOptions(), jobId: normalizedJobId } : defaultJobOptions();
}

function assertValidJobOptions(data: FloridaCandidateFinanceSyncJobData): void {
  assertPositiveInteger(data.maxCandidates, "maxCandidates");
  assertPositiveInteger(data.staleAfterDays, "staleAfterDays");
  assertPositiveInteger(data.electionLookbackDays, "electionLookbackDays");
  assertPositiveInteger(data.electionLookaheadDays, "electionLookaheadDays");
  assertPositiveInteger(data.exportRowLimit, "exportRowLimit");
  assertNonnegativeInteger(data.exportMinIntervalMs, "exportMinIntervalMs");
  assertPositiveInteger(data.aiClassificationMinAmount, "aiClassificationMinAmount");
}

export function createFloridaCandidateFinanceSyncSchedulerQueue(): Queue<FloridaCandidateFinanceSyncJobData> {
  return new Queue<FloridaCandidateFinanceSyncJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export function buildFloridaCandidateFinanceLinkedElectionSyncJobId(now = new Date()): string {
  if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
    throw new Error("Invalid Florida finance linked-election sync job date");
  }
  return `florida-candidate-finance-linked-election-sync-${now.toISOString().slice(0, 10)}`;
}

export async function upsertRecurringFloridaCandidateFinanceSyncJobs(
  jobData: FloridaCandidateFinanceSyncJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isFloridaCampaignFinanceEnabled()) {
    const queue = createFloridaCandidateFinanceSyncSchedulerQueue();
    try {
      await queue.removeJobScheduler(FLORIDA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const queue = createFloridaCandidateFinanceSyncSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      FLORIDA_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: FLORIDA_CANDIDATE_FINANCE_SYNC_JOB_NAME,
        data: {
          dryRun: Boolean(jobData.dryRun),
          force: Boolean(jobData.force),
          maxCandidates: jobData.maxCandidates,
          staleAfterDays: jobData.staleAfterDays,
          electionLookbackDays: jobData.electionLookbackDays,
          electionLookaheadDays: jobData.electionLookaheadDays,
          syncInputs: jobData.syncInputs,
          defaultArtifactCacheDir: jobData.defaultArtifactCacheDir,
          refreshExportArtifacts: Boolean(jobData.refreshExportArtifacts),
          exportMinIntervalMs: jobData.exportMinIntervalMs,
          exportRowLimit: jobData.exportRowLimit,
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

export async function enqueueManualFloridaCandidateFinanceSyncJob(
  jobData: FloridaCandidateFinanceSyncJobData = {},
  options: FloridaCandidateFinanceSyncEnqueueOptions = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isFloridaCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const queue = createFloridaCandidateFinanceSyncSchedulerQueue();

  try {
    const job = await queue.add(
      FLORIDA_CANDIDATE_FINANCE_SYNC_JOB_NAME,
      {
        dryRun: Boolean(jobData.dryRun),
        force: Boolean(jobData.force),
        maxCandidates: jobData.maxCandidates,
        staleAfterDays: jobData.staleAfterDays,
        electionLookbackDays: jobData.electionLookbackDays,
        electionLookaheadDays: jobData.electionLookaheadDays,
        syncInputs: jobData.syncInputs,
        defaultArtifactCacheDir: jobData.defaultArtifactCacheDir,
        refreshExportArtifacts: Boolean(jobData.refreshExportArtifacts),
        exportMinIntervalMs: jobData.exportMinIntervalMs,
        exportRowLimit: jobData.exportRowLimit,
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

export async function runFloridaCandidateFinanceSyncJob(
  data: FloridaCandidateFinanceSyncJobData = {}
): Promise<FloridaCandidateFinanceSyncJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const dryRun = Boolean(data.dryRun);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const now = new Date();
  const enabled = isFloridaCampaignFinanceSyncEnabled(force);

  if (!data.triggeredBy) {
    console.warn("Florida finance sync job missing triggeredBy; recording as unknown");
  }

  if (!enabled) {
    return {
      enabled: false,
      force,
      triggeredBy,
      dryRun,
      now: now.toISOString(),
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
    const financeIndustryClassifier =
      data.aiClassifyIndustries && !dryRun ? createFinanceIndustryClassifierFromEnv() : undefined;
    const result =
      data.syncInputs !== undefined
        ? await syncFloridaCandidateFinanceBatch({
            db: pool,
            now,
            dryRun,
            maxCandidates: data.maxCandidates,
            syncInputs: data.syncInputs,
            defaultArtifactCacheDir: data.defaultArtifactCacheDir,
            financeIndustryClassifier,
            aiClassificationMinAmount: data.aiClassificationMinAmount,
          })
        : await syncDueFloridaCandidateFinance({
            db: pool,
            now,
            dryRun,
            maxCandidates: data.maxCandidates,
            staleAfterDays: data.staleAfterDays,
            electionLookbackDays: data.electionLookbackDays,
            electionLookaheadDays: data.electionLookaheadDays,
            defaultArtifactCacheDir: data.defaultArtifactCacheDir,
            exportMinIntervalMs: data.exportMinIntervalMs,
            exportRowLimit: data.exportRowLimit,
            exportForce: force,
            refreshExportArtifacts: data.refreshExportArtifacts === true,
            financeIndustryClassifier,
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

export function createFloridaCandidateFinanceSyncSchedulerWorker(): Worker<
  FloridaCandidateFinanceSyncJobData,
  FloridaCandidateFinanceSyncJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<
    FloridaCandidateFinanceSyncJobData,
    FloridaCandidateFinanceSyncJobResult
  > = async (job) => {
    return runFloridaCandidateFinanceSyncJob(job.data ?? {});
  };

  return new Worker<FloridaCandidateFinanceSyncJobData, FloridaCandidateFinanceSyncJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
