import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import {
  isOregonCampaignFinanceEnabled,
  isOregonCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  syncDueOregonCandidateFinance,
  type OregonCandidateFinanceBatchSyncResult,
} from "../pipeline/oregonFinance/oregonCandidateFinanceBatchSync.js";

export const OREGON_CANDIDATE_FINANCE_SYNC_JOB_NAME = "oregon_candidate_finance_sync_due";
export const OREGON_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID = "oregon_candidate_finance_sync_daily";

export type OregonCandidateFinanceSyncJobData = {
  dryRun?: boolean;
  force?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  directMaxBreakdownsPerCategory?: number;
  outsideMaxGroups?: number;
  outsideMaxBreakdownsPerCategory?: number;
  minIndustryAmount?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type OregonCandidateFinanceSyncJobResult = OregonCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<OregonCandidateFinanceSyncJobData["triggeredBy"]>;
};

export type OregonCandidateFinanceSyncEnqueueOptions = {
  jobId?: string;
};

type OregonCandidateFinanceSyncSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

const DISABLED_RESULT_DEFAULT_MAX_CANDIDATES = 0;
const DISABLED_RESULT_DEFAULT_STALE_AFTER_DAYS = 0;

function readSchedulerRuntimeConfig(): OregonCandidateFinanceSyncSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.OREGON_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() ||
      "oregon_candidate_finance_sync_maintenance",
    dailyCron: process.env.OREGON_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() || "45 9 * * *",
    dailyTz: process.env.OREGON_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  if (value !== undefined && (!Number.isInteger(value) || value <= 0)) {
    throw new Error(`Invalid Oregon finance sync scheduler ${label}: ${value}`);
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
    throw new Error("Oregon finance sync scheduler jobId must not contain ':'");
  }
  return trimmed;
}

function jobOptionsWithId(jobId: string | undefined): JobsOptions {
  const normalizedJobId = normalizeOptionalJobId(jobId);
  return normalizedJobId ? { ...defaultJobOptions(), jobId: normalizedJobId } : defaultJobOptions();
}

function assertValidJobOptions(data: OregonCandidateFinanceSyncJobData): void {
  assertPositiveInteger(data.maxCandidates, "maxCandidates");
  assertPositiveInteger(data.staleAfterDays, "staleAfterDays");
  assertPositiveInteger(data.electionLookbackDays, "electionLookbackDays");
  assertPositiveInteger(data.electionLookaheadDays, "electionLookaheadDays");
  assertPositiveInteger(data.directMaxBreakdownsPerCategory, "directMaxBreakdownsPerCategory");
  assertPositiveInteger(data.outsideMaxGroups, "outsideMaxGroups");
  assertPositiveInteger(data.outsideMaxBreakdownsPerCategory, "outsideMaxBreakdownsPerCategory");
  assertPositiveInteger(data.minIndustryAmount, "minIndustryAmount");
}

export function createOregonCandidateFinanceSyncSchedulerQueue(): Queue<OregonCandidateFinanceSyncJobData> {
  return new Queue<OregonCandidateFinanceSyncJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export function buildOregonCandidateFinanceLinkedElectionSyncJobId(now = new Date()): string {
  if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
    throw new Error("Invalid Oregon finance linked-election sync job date");
  }
  return `oregon-candidate-finance-linked-election-sync-${now.toISOString().slice(0, 10)}`;
}

export async function upsertRecurringOregonCandidateFinanceSyncJobs(
  jobData: OregonCandidateFinanceSyncJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isOregonCampaignFinanceEnabled()) {
    const queue = createOregonCandidateFinanceSyncSchedulerQueue();
    try {
      await queue.removeJobScheduler(OREGON_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const queue = createOregonCandidateFinanceSyncSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      OREGON_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: OREGON_CANDIDATE_FINANCE_SYNC_JOB_NAME,
        data: {
          dryRun: Boolean(jobData.dryRun),
          force: Boolean(jobData.force),
          maxCandidates: jobData.maxCandidates,
          staleAfterDays: jobData.staleAfterDays,
          electionLookbackDays: jobData.electionLookbackDays,
          electionLookaheadDays: jobData.electionLookaheadDays,
          directMaxBreakdownsPerCategory: jobData.directMaxBreakdownsPerCategory,
          outsideMaxGroups: jobData.outsideMaxGroups,
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

export async function enqueueManualOregonCandidateFinanceSyncJob(
  jobData: OregonCandidateFinanceSyncJobData = {},
  options: OregonCandidateFinanceSyncEnqueueOptions = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isOregonCampaignFinanceSyncEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const queue = createOregonCandidateFinanceSyncSchedulerQueue();

  try {
    const job = await queue.add(
      OREGON_CANDIDATE_FINANCE_SYNC_JOB_NAME,
      {
        dryRun: Boolean(jobData.dryRun),
        force: Boolean(jobData.force),
        maxCandidates: jobData.maxCandidates,
        staleAfterDays: jobData.staleAfterDays,
        electionLookbackDays: jobData.electionLookbackDays,
        electionLookaheadDays: jobData.electionLookaheadDays,
        directMaxBreakdownsPerCategory: jobData.directMaxBreakdownsPerCategory,
        outsideMaxGroups: jobData.outsideMaxGroups,
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

export async function runOregonCandidateFinanceSyncJob(
  data: OregonCandidateFinanceSyncJobData = {}
): Promise<OregonCandidateFinanceSyncJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const dryRun = Boolean(data.dryRun);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const now = new Date();
  const enabled = isOregonCampaignFinanceSyncEnabled(force);

  if (!data.triggeredBy) {
    console.warn("Oregon finance sync job missing triggeredBy; recording as unknown");
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
    const result = await syncDueOregonCandidateFinance({
      db: pool,
      now,
      dryRun,
      maxCandidates: data.maxCandidates,
      staleAfterDays: data.staleAfterDays,
      electionLookbackDays: data.electionLookbackDays,
      electionLookaheadDays: data.electionLookaheadDays,
      directMaxBreakdownsPerCategory: data.directMaxBreakdownsPerCategory,
      outsideMaxGroups: data.outsideMaxGroups,
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

export function createOregonCandidateFinanceSyncSchedulerWorker(): Worker<
  OregonCandidateFinanceSyncJobData,
  OregonCandidateFinanceSyncJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<OregonCandidateFinanceSyncJobData, OregonCandidateFinanceSyncJobResult> = async (job) => {
    return runOregonCandidateFinanceSyncJob(job.data ?? {});
  };

  return new Worker<OregonCandidateFinanceSyncJobData, OregonCandidateFinanceSyncJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
