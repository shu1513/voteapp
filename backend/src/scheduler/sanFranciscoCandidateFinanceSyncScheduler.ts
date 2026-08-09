import { Queue, Worker, type JobsOptions } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { Pool } from "pg";
import { getPipelineEnv } from "../config/env.js";
import {
  isSanFranciscoCampaignFinanceEnabled,
  isSanFranciscoCampaignFinanceSyncEnabled,
} from "../config/featureFlags.js";
import {
  syncDueSanFranciscoCandidateFinance,
  type SanFranciscoCandidateFinanceBatchSyncResult,
} from "../pipeline/sanFranciscoFinance/sanFranciscoCandidateFinanceBatchSync.js";
import { toConnectionOptions } from "../utils/redisConnection.js";

export const SAN_FRANCISCO_FINANCE_JOB_NAME =
  "san_francisco_candidate_finance_sync_due";
export const SAN_FRANCISCO_FINANCE_SCHEDULER_ID =
  "san_francisco_candidate_finance_sync_daily";
export type SanFranciscoFinanceJobData = {
  dryRun?: boolean;
  force?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};
export type SanFranciscoFinanceJobResult =
  SanFranciscoCandidateFinanceBatchSyncResult & {
    enabled: boolean;
    force: boolean;
    triggeredBy: string;
  };
const config = () => ({
  queue:
    process.env.SAN_FRANCISCO_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() ||
    "san_francisco_candidate_finance_maintenance",
  // 16:30 UTC = 09:30 PDT / 08:30 PST — after DataSF's nightly refresh
  // year-round, and offset from LA's 09:50 so the city syncs never stack
  // on one worker host.
  cron:
    process.env.SAN_FRANCISCO_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() ||
    "30 16 * * *",
  tz:
    process.env.SAN_FRANCISCO_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
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
const validate = (data: SanFranciscoFinanceJobData): void => {
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
      throw new Error(
        `Invalid San Francisco finance scheduler ${key}: ${value}`,
      );
};
export const createSanFranciscoFinanceQueue = () =>
  new Queue<SanFranciscoFinanceJobData>(config().queue, {
    connection: connection(),
    defaultJobOptions: opts(),
  });
export async function upsertRecurringSanFranciscoFinanceJobs(
  data: SanFranciscoFinanceJobData = {},
): Promise<void> {
  validate(data);
  const queue = createSanFranciscoFinanceQueue();
  try {
    if (!isSanFranciscoCampaignFinanceEnabled()) {
      await queue.removeJobScheduler(SAN_FRANCISCO_FINANCE_SCHEDULER_ID);
      return;
    }
    await queue.upsertJobScheduler(
      SAN_FRANCISCO_FINANCE_SCHEDULER_ID,
      { pattern: config().cron, tz: config().tz },
      {
        name: SAN_FRANCISCO_FINANCE_JOB_NAME,
        data: { ...data, triggeredBy: "daily" },
        opts: opts(),
      },
    );
  } finally {
    await queue.close();
  }
}
export async function enqueueManualSanFranciscoFinanceSyncJob(
  data: SanFranciscoFinanceJobData = {},
  options: { jobId?: string } = {},
): Promise<string> {
  validate(data);
  if (!isSanFranciscoCampaignFinanceSyncEnabled(Boolean(data.force)))
    return "disabled";
  const queue = createSanFranciscoFinanceQueue();
  try {
    const job = await queue.add(
      SAN_FRANCISCO_FINANCE_JOB_NAME,
      { ...data, triggeredBy: "manual", requestedAt: new Date().toISOString() },
      { ...opts(), ...(options.jobId ? { jobId: options.jobId } : {}) },
    );
    return job.id ?? "unknown";
  } finally {
    await queue.close();
  }
}
export async function runSanFranciscoFinanceJob(
  data: SanFranciscoFinanceJobData = {},
): Promise<SanFranciscoFinanceJobResult> {
  validate(data);
  const force = Boolean(data.force),
    triggeredBy = data.triggeredBy ?? "unknown";
  if (!isSanFranciscoCampaignFinanceSyncEnabled(force))
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
      staleElectionRefreshCount: 0,
      results: [],
    };
  const pool = new Pool({ connectionString: getPipelineEnv().DATABASE_URL });
  try {
    return {
      enabled: true,
      force,
      triggeredBy,
      ...(await syncDueSanFranciscoCandidateFinance({
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
export function createSanFranciscoFinanceWorker(): Worker<
  SanFranciscoFinanceJobData,
  SanFranciscoFinanceJobResult
> {
  return new Worker(
    config().queue,
    async (job) => {
      if (job.name !== SAN_FRANCISCO_FINANCE_JOB_NAME)
        throw new Error(`Unexpected San Francisco finance job: ${job.name}`);
      return runSanFranciscoFinanceJob(job.data);
    },
    { connection: connection(), concurrency: 1 },
  );
}
