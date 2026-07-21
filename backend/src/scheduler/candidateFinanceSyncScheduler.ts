import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";
import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import { isCandidateFinanceEnabled, isCandidateFinanceSyncEnabled } from "../config/featureFlags.js";
import {
  syncDueCandidateFinance,
  type CandidateFinanceBatchSyncItemResult,
  type CandidateFinanceBatchSyncResult,
  type CandidateFinanceDueRow,
} from "../pipeline/finance/candidateFinanceBatchSync.js";
import { syncCandidateFinance } from "../pipeline/finance/candidateFinanceSync.js";
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
  candidateId?: string;
  fecCandidateId?: string;
  electionYear?: number;
  source?: CandidateFinanceDueRow["source"];
  triggeredBy?: "daily" | "manual" | "candidate_link" | "unknown";
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

function normalizeTargetFecCandidateId(value: string | undefined): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  const normalized = value.trim().toUpperCase();
  if (!/^[HPS][0-9A-Z]{8}$/.test(normalized)) {
    throw new Error(`Invalid candidate finance sync scheduler fecCandidateId: ${value}`);
  }
  return normalized;
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
  if (
    data.electionYear !== undefined &&
    (!Number.isInteger(data.electionYear) || data.electionYear < 1970 || data.electionYear > 2100)
  ) {
    throw new Error(`Invalid candidate finance sync scheduler electionYear: ${data.electionYear}`);
  }
  normalizeTargetFecCandidateId(data.fecCandidateId);
  if ((data.fecCandidateId === undefined) !== (data.electionYear === undefined)) {
    throw new Error("candidate finance sync scheduler targeted jobs require both fecCandidateId and electionYear");
  }
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
        candidateId: jobData.candidateId,
        fecCandidateId: jobData.fecCandidateId?.trim().toUpperCase(),
        electionYear: jobData.electionYear,
        source: jobData.source,
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

export async function enqueueCandidateLinkCandidateFinanceSyncJob(input: {
  candidateId: string;
  fecCandidateId: string;
  electionYear: number;
  source?: CandidateFinanceDueRow["source"];
  includeOutside?: boolean;
  perPage?: number;
  outsideGroupLimit?: number;
  timeoutMs?: number;
}): Promise<string> {
  const jobData: CandidateFinanceSyncJobData = {
    candidateId: input.candidateId,
    fecCandidateId: input.fecCandidateId,
    electionYear: input.electionYear,
    source: input.source ?? "candidate_election",
    force: false,
    includeOutside: input.includeOutside,
    perPage: input.perPage,
    outsideGroupLimit: input.outsideGroupLimit,
    timeoutMs: input.timeoutMs,
  };
  assertValidJobOptions(jobData);
  if (!isCandidateFinanceSyncEnabled(false)) {
    return "disabled";
  }

  const queue = createCandidateFinanceSyncSchedulerQueue();

  try {
    const job = await queue.add(
      CANDIDATE_FINANCE_SYNC_JOB_NAME,
      {
        ...jobData,
        fecCandidateId: jobData.fecCandidateId?.trim().toUpperCase(),
        triggeredBy: "candidate_link",
        requestedAt: new Date().toISOString(),
      },
      defaultJobOptions()
    );
    return job.id ?? "unknown";
  } finally {
    await queue.close();
  }
}

function buildTargetedSyncResult(input: {
  dryRun: boolean;
  includeOutside: boolean;
  now: Date;
  candidateId: string | undefined;
  fecCandidateId: string;
  electionYear: number;
  source: CandidateFinanceDueRow["source"];
  result: Awaited<ReturnType<typeof syncCandidateFinance>>;
}): CandidateFinanceBatchSyncResult {
  const item: CandidateFinanceBatchSyncItemResult = {
    candidateId: input.candidateId ?? "",
    fecCandidateId: input.fecCandidateId,
    electionYear: input.electionYear,
    source: input.source,
    ok: true,
    result: input.result,
  };

  return {
    dryRun: input.dryRun,
    includeOutside: input.includeOutside,
    now: input.now.toISOString(),
    staleAfterDays: 0,
    maxCandidates: 1,
    dueCandidateCount: 1,
    selectedCandidateCount: 1,
    syncedCandidateCount: 1,
    failedCandidateCount: 0,
    results: [item],
  };
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
    const targetedFecCandidateId = normalizeTargetFecCandidateId(data.fecCandidateId);
    if (targetedFecCandidateId && data.electionYear !== undefined) {
      const result = await syncCandidateFinance({
        db: pool,
        fecCandidateId: targetedFecCandidateId,
        electionYear: data.electionYear,
        openFecOptions: { apiKeys, timeoutMs: data.timeoutMs ?? DEFAULT_OPEN_FEC_TIMEOUT_MS },
        now,
        dryRun,
        includeOutside,
        perPage: data.perPage,
        outsideGroupLimit: data.outsideGroupLimit,
      });

      return {
        enabled: true,
        force,
        triggeredBy,
        ...buildTargetedSyncResult({
          dryRun,
          includeOutside,
          now,
          candidateId: data.candidateId,
          fecCandidateId: targetedFecCandidateId,
          electionYear: data.electionYear,
          source: data.source ?? "candidate_election",
          result,
        }),
      };
    }

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
