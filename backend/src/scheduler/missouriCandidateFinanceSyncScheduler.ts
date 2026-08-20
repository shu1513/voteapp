import { Queue, Worker, type ConnectionOptions, type JobsOptions, type Processor } from "bullmq";
import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import { isMissouriCampaignFinanceEnabled, isMissouriCampaignFinanceSyncEnabled } from "../config/featureFlags.js";
import { syncDueMissouriCandidateFinance, type MissouriCandidateFinanceBatchSyncResult } from "../pipeline/missouriFinance/missouriCandidateFinanceBatchSync.js";
import { toConnectionOptions } from "../utils/redisConnection.js";

export const MISSOURI_CANDIDATE_FINANCE_SYNC_JOB_NAME = "missouri_candidate_finance_sync_due";
export const MISSOURI_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID = "missouri_candidate_finance_sync_daily";

export type MissouriCandidateFinanceSyncJobData = {
  dryRun?: boolean;
  force?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  cacheDir?: string;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type MissouriCandidateFinanceSyncJobResult = MissouriCandidateFinanceBatchSyncResult & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<MissouriCandidateFinanceSyncJobData["triggeredBy"]>;
};

const DEFAULT_JOB_OPTIONS: JobsOptions = { removeOnComplete: 1000, removeOnFail: 1000 };

function runtimeConfig(): { queueName: string; cron: string; timezone: string } {
  return {
    queueName: process.env.MISSOURI_CAMPAIGN_FINANCE_SYNC_SCHEDULER_QUEUE?.trim() || "missouri_candidate_finance_sync_maintenance",
    cron: process.env.MISSOURI_CAMPAIGN_FINANCE_SYNC_DAILY_CRON?.trim() || "25 11 * * *",
    timezone: process.env.MISSOURI_CAMPAIGN_FINANCE_SYNC_DAILY_TZ?.trim() || "UTC",
  };
}

function connection(): ConnectionOptions {
  return toConnectionOptions(getPipelineEnv().REDIS_URL);
}

function validate(data: MissouriCandidateFinanceSyncJobData): void {
  for (const [label, value] of [
    ["maxCandidates", data.maxCandidates],
    ["staleAfterDays", data.staleAfterDays],
    ["electionLookbackDays", data.electionLookbackDays],
    ["electionLookaheadDays", data.electionLookaheadDays],
  ] as const) {
    if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
      throw new Error(`Invalid Missouri finance sync scheduler ${label}: ${value}`);
    }
  }
}

function normalizeJobId(value: string | undefined): string | undefined {
  const id = value?.trim();
  if (!id) return undefined;
  if (id.includes(":")) throw new Error("Missouri finance sync scheduler jobId must not contain ':'");
  if (id === "disabled" || id === "unknown") throw new Error("Missouri finance sync scheduler jobId uses a reserved value");
  return id;
}

export function createMissouriCandidateFinanceSyncSchedulerQueue(): Queue<MissouriCandidateFinanceSyncJobData> {
  return new Queue(runtimeConfig().queueName, { connection: connection(), defaultJobOptions: DEFAULT_JOB_OPTIONS });
}

export async function upsertRecurringMissouriCandidateFinanceSyncJobs(
  data: MissouriCandidateFinanceSyncJobData = {}
): Promise<void> {
  validate(data);
  const queue = createMissouriCandidateFinanceSyncSchedulerQueue();
  try {
    if (!isMissouriCampaignFinanceEnabled()) {
      await queue.removeJobScheduler(MISSOURI_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID);
      return;
    }
    const config = runtimeConfig();
    await queue.upsertJobScheduler(
      MISSOURI_CANDIDATE_FINANCE_SYNC_DAILY_SCHEDULER_ID,
      { pattern: config.cron, tz: config.timezone },
      {
        name: MISSOURI_CANDIDATE_FINANCE_SYNC_JOB_NAME,
        data: {
          dryRun: Boolean(data.dryRun), force: Boolean(data.force), maxCandidates: data.maxCandidates,
          staleAfterDays: data.staleAfterDays, electionLookbackDays: data.electionLookbackDays,
          electionLookaheadDays: data.electionLookaheadDays, cacheDir: data.cacheDir, triggeredBy: "daily",
        },
        opts: DEFAULT_JOB_OPTIONS,
      }
    );
  } finally {
    await queue.close();
  }
}

export async function enqueueManualMissouriCandidateFinanceSyncJob(
  data: MissouriCandidateFinanceSyncJobData = {},
  options: { jobId?: string } = {}
): Promise<string> {
  validate(data);
  if (!isMissouriCampaignFinanceSyncEnabled(Boolean(data.force))) return "disabled";
  const queue = createMissouriCandidateFinanceSyncSchedulerQueue();
  try {
    const jobId = normalizeJobId(options.jobId);
    const job = await queue.add(MISSOURI_CANDIDATE_FINANCE_SYNC_JOB_NAME, {
      ...data,
      dryRun: Boolean(data.dryRun),
      force: Boolean(data.force),
      triggeredBy: "manual",
      requestedAt: new Date().toISOString(),
    }, jobId ? { ...DEFAULT_JOB_OPTIONS, jobId } : DEFAULT_JOB_OPTIONS);
    return job.id ?? "unknown";
  } finally {
    await queue.close();
  }
}

export async function runMissouriCandidateFinanceSyncJob(
  data: MissouriCandidateFinanceSyncJobData = {}
): Promise<MissouriCandidateFinanceSyncJobResult> {
  validate(data);
  const force = Boolean(data.force);
  const dryRun = Boolean(data.dryRun);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const now = new Date();
  if (!isMissouriCampaignFinanceSyncEnabled(force)) {
    return {
      enabled: false, force, triggeredBy, dryRun, rawDataRefreshEnabled: false, now: now.toISOString(),
      staleAfterDays: data.staleAfterDays ?? 0, maxCandidates: data.maxCandidates ?? 0,
      dueCandidateCount: 0, selectedCandidateCount: 0, syncedCandidateCount: 0, failedCandidateCount: 0,
      autoLinkAttemptedCount: 0, autoLinkLinkedCount: 0,
      outsideArtifactYearCount: 0, failedOutsideArtifactYearCount: 0, results: [],
    };
  }
  const pool = new Pool({ connectionString: getPipelineEnv().DATABASE_URL });
  try {
    const result = await syncDueMissouriCandidateFinance({
      db: pool,
      now,
      dryRun,
      forceRawDataRefresh: force,
      maxCandidates: data.maxCandidates,
      staleAfterDays: data.staleAfterDays,
      electionLookbackDays: data.electionLookbackDays,
      electionLookaheadDays: data.electionLookaheadDays,
      cacheDir: data.cacheDir,
    });
    return { enabled: true, force, triggeredBy, ...result };
  } finally {
    await pool.end();
  }
}

export function createMissouriCandidateFinanceSyncSchedulerWorker(): Worker<
  MissouriCandidateFinanceSyncJobData,
  MissouriCandidateFinanceSyncJobResult
> {
  const processor: Processor<MissouriCandidateFinanceSyncJobData, MissouriCandidateFinanceSyncJobResult> = async (job) =>
    runMissouriCandidateFinanceSyncJob(job.data ?? {});
  return new Worker(runtimeConfig().queueName, processor, { connection: connection(), concurrency: 1 });
}
