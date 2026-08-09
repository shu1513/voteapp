import { Queue, Worker, type JobsOptions } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { Pool } from "pg";
import { getPipelineEnv } from "../config/env.js";
import {
  isLosAngelesCityCampaignFinanceEnabled,
  isLosAngelesCityCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  syncDueLosAngelesCandidateFinance,
  type LosAngelesCandidateFinanceBatchSyncResult,
} from "../pipeline/losAngelesCityFinance/losAngelesCandidateFinanceBatchSync.js";
import { toConnectionOptions } from "../utils/redisConnection.js";

export const LOS_ANGELES_CITY_FINANCE_JOB_NAME =
  "los_angeles_city_candidate_finance_sync_due";
export const LOS_ANGELES_CITY_FINANCE_SCHEDULER_ID =
  "los_angeles_city_candidate_finance_sync_daily";
export type LosAngelesCityFinanceJobData = {
  dryRun?: boolean;
  force?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};
export type LosAngelesCityFinanceJobResult =
  LosAngelesCandidateFinanceBatchSyncResult & {
    enabled: boolean;
    force: boolean;
    triggeredBy: string;
  };
const config = () => ({
  queue:
    process.env.LOS_ANGELES_CITY_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() ||
    "los_angeles_city_candidate_finance_maintenance",
  cron:
    process.env.LOS_ANGELES_CITY_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() ||
    "50 9 * * *",
  tz:
    process.env.LOS_ANGELES_CITY_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() ||
    "UTC",
});
const connection = (): ConnectionOptions =>
  toConnectionOptions(getPipelineEnv().REDIS_URL);
const opts = (): JobsOptions => ({
  removeOnComplete: 1000,
  removeOnFail: 1000,
  attempts: 3,
  backoff: { type: "exponential", delay: 5_000 },
});
// isSafeInteger, not isInteger: Number("9007199254740993") silently rounds
// to 2^53 and still passes isInteger.
const validate = (data: LosAngelesCityFinanceJobData): void => {
  for (const [key, value] of Object.entries(data))
    if (
      [
        "maxCandidates",
        "staleAfterDays",
        "electionLookbackDays",
        "electionLookaheadDays",
      ].includes(key) &&
      value !== undefined &&
      (!Number.isSafeInteger(value) || Number(value) <= 0)
    )
      throw new Error(`Invalid Los Angeles finance scheduler ${key}: ${value}`);
};
export const createLosAngelesCityFinanceQueue = () =>
  new Queue<LosAngelesCityFinanceJobData>(config().queue, {
    connection: connection(),
    defaultJobOptions: opts(),
  });
export function buildLosAngelesCityFinanceLinkedElectionSyncJobId(
  now = new Date(),
): string {
  if (Number.isNaN(now.getTime()))
    throw new Error("Invalid Los Angeles finance job date");
  return `los-angeles-city-finance-linked-${now.toISOString().slice(0, 10)}`;
}
export async function upsertRecurringLosAngelesCityFinanceJobs(
  data: LosAngelesCityFinanceJobData = {},
): Promise<void> {
  validate(data);
  const queue = createLosAngelesCityFinanceQueue();
  try {
    if (!isLosAngelesCityCampaignFinanceEnabled()) {
      await queue.removeJobScheduler(LOS_ANGELES_CITY_FINANCE_SCHEDULER_ID);
      return;
    }
    await queue.upsertJobScheduler(
      LOS_ANGELES_CITY_FINANCE_SCHEDULER_ID,
      { pattern: config().cron, tz: config().tz },
      {
        name: LOS_ANGELES_CITY_FINANCE_JOB_NAME,
        data: { ...data, triggeredBy: "daily" },
        opts: opts(),
      },
    );
  } finally {
    await queue.close();
  }
}
export async function enqueueManualLosAngelesCityFinanceSyncJob(
  data: LosAngelesCityFinanceJobData = {},
  options: { jobId?: string } = {},
): Promise<string> {
  validate(data);
  if (!isLosAngelesCityCampaignFinanceSyncEnabled(Boolean(data.force)))
    return "disabled";
  const queue = createLosAngelesCityFinanceQueue();
  try {
    const job = await queue.add(
      LOS_ANGELES_CITY_FINANCE_JOB_NAME,
      { ...data, triggeredBy: "manual", requestedAt: new Date().toISOString() },
      { ...opts(), ...(options.jobId ? { jobId: options.jobId } : {}) },
    );
    return job.id ?? "unknown";
  } finally {
    await queue.close();
  }
}
export async function runLosAngelesCityFinanceJob(
  data: LosAngelesCityFinanceJobData = {},
): Promise<LosAngelesCityFinanceJobResult> {
  validate(data);
  const force = Boolean(data.force),
    triggeredBy = data.triggeredBy ?? "unknown";
  if (!isLosAngelesCityCampaignFinanceSyncEnabled(force))
    return {
      enabled: false,
      force,
      triggeredBy,
      dryRun: Boolean(data.dryRun),
      dueCandidateCount: 0,
      selectedCandidateCount: 0,
      syncedCandidateCount: 0,
      failedCandidateCount: 0,
      autoLinkAttemptedCount: 0,
      autoLinkLinkedCount: 0,
      results: [],
    };
  const pool = new Pool({ connectionString: getPipelineEnv().DATABASE_URL });
  try {
    return {
      enabled: true,
      force,
      triggeredBy,
      ...(await syncDueLosAngelesCandidateFinance({
        db: pool,
        dryRun: data.dryRun,
        maxCandidates: data.maxCandidates,
        staleAfterDays: data.staleAfterDays,
        electionLookbackDays: data.electionLookbackDays,
        electionLookaheadDays: data.electionLookaheadDays,
      })),
    };
  } finally {
    await pool.end();
  }
}
export function createLosAngelesCityFinanceWorker(): Worker<
  LosAngelesCityFinanceJobData,
  LosAngelesCityFinanceJobResult
> {
  return new Worker(
    config().queue,
    async (job) => {
      if (job.name !== LOS_ANGELES_CITY_FINANCE_JOB_NAME)
        throw new Error(`Unexpected Los Angeles finance job: ${job.name}`);
      return runLosAngelesCityFinanceJob(job.data);
    },
    { connection: connection(), concurrency: 1 },
  );
}
