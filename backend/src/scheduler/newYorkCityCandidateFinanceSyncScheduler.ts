import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import {
  isNewYorkCityCampaignFinanceEnabled,
  isNewYorkCityCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  syncDueNewYorkCityCandidateFinance,
  type NewYorkCityCandidateFinanceBatchResult,
} from "../pipeline/newYorkCityFinance/newYorkCityCandidateFinanceBatchSync.js";
import { toConnectionOptions } from "../utils/redisConnection.js";

export const NEW_YORK_CITY_FINANCE_SYNC_JOB_NAME = "new_york_city_candidate_finance_sync_due";
export const NEW_YORK_CITY_FINANCE_SYNC_SCHEDULER_ID = "new_york_city_candidate_finance_sync_daily";

export type NewYorkCityFinanceSyncJobData = {
  dryRun?: boolean;
  force?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  triggeredBy?: "daily" | "manual";
};

export type NewYorkCityFinanceSyncJobResult = NewYorkCityCandidateFinanceBatchResult & {
  enabled: boolean;
  triggeredBy: "daily" | "manual";
};

function queueName(): string {
  return process.env.NEW_YORK_CITY_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE?.trim()
    || "new_york_city_candidate_finance_sync_maintenance";
}
function connection(): ConnectionOptions {
  return toConnectionOptions(getPipelineEnv().REDIS_URL);
}

function defaultJobOptions(): JobsOptions {
  return { removeOnComplete: 1000, removeOnFail: 1000 };
}

function validate(data: NewYorkCityFinanceSyncJobData): void {
  for (const [name, value] of Object.entries({
    maxCandidates: data.maxCandidates,
    staleAfterDays: data.staleAfterDays,
    electionLookbackDays: data.electionLookbackDays,
    electionLookaheadDays: data.electionLookaheadDays,
  })) {
    // isSafeInteger, not isInteger: Number("9007199254740993") silently rounds
    // to 2^53 and still passes isInteger.
    if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
      throw new Error(`Invalid NYC finance scheduler ${name}: ${value}`);
    }
  }
}

export function createNewYorkCityFinanceSyncQueue(): Queue<NewYorkCityFinanceSyncJobData> {
  return new Queue(queueName(), { connection: connection(), defaultJobOptions: defaultJobOptions() });
}

export async function upsertRecurringNewYorkCityFinanceSyncJob(data: NewYorkCityFinanceSyncJobData = {}): Promise<void> {
  validate(data);
  const queue = createNewYorkCityFinanceSyncQueue();
  try {
    if (!isNewYorkCityCampaignFinanceEnabled()) {
      await queue.removeJobScheduler(NEW_YORK_CITY_FINANCE_SYNC_SCHEDULER_ID);
      return;
    }
    await queue.upsertJobScheduler(
      NEW_YORK_CITY_FINANCE_SYNC_SCHEDULER_ID,
      {
        pattern: process.env.NEW_YORK_CITY_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() || "50 9 * * *",
        tz: process.env.NEW_YORK_CITY_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
      },
      {
        name: NEW_YORK_CITY_FINANCE_SYNC_JOB_NAME,
        data: { ...data, triggeredBy: "daily" },
        opts: defaultJobOptions(),
      }
    );
  } finally {
    await queue.close();
  }
}

export async function enqueueNewYorkCityFinanceSyncJob(data: NewYorkCityFinanceSyncJobData = {}): Promise<string> {
  validate(data);
  if (!isNewYorkCityCampaignFinanceSyncEnabled(Boolean(data.force))) return "disabled";
  const queue = createNewYorkCityFinanceSyncQueue();
  try {
    const job = await queue.add(NEW_YORK_CITY_FINANCE_SYNC_JOB_NAME, { ...data, triggeredBy: "manual" }, defaultJobOptions());
    return job.id ?? "queued";
  } finally {
    await queue.close();
  }
}

export async function runNewYorkCityFinanceSyncJob(
  data: NewYorkCityFinanceSyncJobData = {}
): Promise<NewYorkCityFinanceSyncJobResult> {
  validate(data);
  const triggeredBy = data.triggeredBy ?? "manual";
  if (!isNewYorkCityCampaignFinanceSyncEnabled(Boolean(data.force))) {
    return {
      enabled: false,
      triggeredBy,
      dryRun: Boolean(data.dryRun),
      dueCandidateCount: 0,
      selectedCandidateCount: 0,
      syncedCandidateCount: 0,
      deferredCandidateCount: 0,
      failedCandidateCount: 0,
      results: [],
    };
  }
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  try {
    const result = await syncDueNewYorkCityCandidateFinance({
      db: pool,
      dryRun: data.dryRun,
      maxCandidates: data.maxCandidates,
      staleAfterDays: data.staleAfterDays,
      electionLookbackDays: data.electionLookbackDays,
      electionLookaheadDays: data.electionLookaheadDays,
    });
    return { enabled: true, triggeredBy, ...result };
  } finally {
    await pool.end();
  }
}

export function createNewYorkCityFinanceSyncWorker(): Worker<NewYorkCityFinanceSyncJobData, NewYorkCityFinanceSyncJobResult> {
  const processor: Processor<NewYorkCityFinanceSyncJobData, NewYorkCityFinanceSyncJobResult> = async (job) =>
    runNewYorkCityFinanceSyncJob(job.data ?? {});
  return new Worker(queueName(), processor, { connection: connection(), concurrency: 1 });
}
