import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import { runStateResourcesProducer } from "../pipeline/producers/stateResourcesProducer.js";
import { runStateResourcesEnricher } from "../pipeline/enrichers/stateResourcesEnricher.js";
import { runStateResourcesValidator } from "../pipeline/validators/stateResourcesValidator.js";
import { runStateResourcesWriter } from "../pipeline/writers/stateResourcesWriter.js";
import { runStateResourcesRetrySweeper } from "../pipeline/retries/stateResourcesRetry.js";

export const STATE_RESOURCES_REFRESH_JOB_NAME = "state_resources_refresh";
export const STATE_RESOURCES_ANNUAL_SCHEDULER_ID = "state_resources_annual_refresh";
export const STATE_RESOURCES_PRE_ELECTION_WEEKLY_SCHEDULER_ID = "state_resources_pre_election_weekly_refresh";

export type StateResourcesRefreshJobData = {
  dryRun?: boolean;
  force?: boolean;
  triggeredBy?: "annual" | "monthly" | "manual";
  requestedAt?: string;
};

export type StateResourcesRefreshJobResult = {
  dryRun: boolean;
  force: boolean;
  triggeredBy: NonNullable<StateResourcesRefreshJobData["triggeredBy"]>;
  skipped: boolean;
  skipReason: string | null;
  passes: number;
  retrySweeps: number;
  requeuedToDraft: number;
  requeuedToPending: number;
  startedAt: string;
  finishedAt: string;
};

type SchedulerRuntimeConfig = {
  queueName: string;
  monthlyCron: string;
  monthlyTz: string;
  maxPasses: number;
  batchSize: number;
  blockMs: number;
};

type StateResourceStageCounts = {
  total: number;
  draftPending: number;
  enrichedPending: number;
  validated: number;
  failed: number;
  rejected: number;
  written: number;
};

function readPositiveIntegerEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw || raw.trim().length === 0) {
    return fallback;
  }

  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Invalid ${name}: ${raw}. Expected a positive integer.`);
  }
  return parsed;
}

function readSchedulerRuntimeConfig(): SchedulerRuntimeConfig {
  return {
    queueName: process.env.STATE_RESOURCES_SCHEDULER_QUEUE?.trim() || "state_resources_maintenance",
    monthlyCron: process.env.STATE_RESOURCES_MONTHLY_CRON?.trim() || "0 3 1 * *",
    monthlyTz: process.env.STATE_RESOURCES_MONTHLY_TZ?.trim() || "UTC",
    maxPasses: readPositiveIntegerEnv("STATE_RESOURCES_SCHEDULER_MAX_PASSES", 3),
    batchSize: readPositiveIntegerEnv("STATE_RESOURCES_SCHEDULER_BATCH_SIZE", 200),
    blockMs: readPositiveIntegerEnv("STATE_RESOURCES_SCHEDULER_BLOCK_MS", 250),
  };
}

function toConnectionOptions(redisUrl: string): ConnectionOptions {
  const parsed = new URL(redisUrl);

  const opts: ConnectionOptions = {
    host: parsed.hostname,
    port: parsed.port ? Number.parseInt(parsed.port, 10) : 6379,
    db: parsed.pathname.length > 1 ? Number.parseInt(parsed.pathname.slice(1), 10) : 0,
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

function hasInFlightItems(counts: StateResourceStageCounts): boolean {
  return counts.draftPending > 0 || counts.enrichedPending > 0 || counts.validated > 0;
}

function getDrainRoundLimit(totalItems: number, batchSize: number): number {
  const safeBatchSize = Math.max(1, batchSize);
  const minRounds = 5;
  const estimatedRounds = Math.ceil(Math.max(1, totalItems) / safeBatchSize);
  return Math.max(minRounds, estimatedRounds * 4);
}

async function loadStateResourceStageCounts(pool: Pool): Promise<StateResourceStageCounts> {
  const result = await pool.query<{
    total: string;
    draft_pending: string;
    enriched_pending: string;
    validated: string;
    failed: string;
    rejected: string;
    written: string;
  }>(
    `
      SELECT
        count(*)::text AS total,
        count(*) FILTER (
          WHERE status = 'pending' AND schema_version = 'state_resources_draft_v1'
        )::text AS draft_pending,
        count(*) FILTER (
          WHERE status = 'pending' AND schema_version = 'state_resources_enrichment_v1'
        )::text AS enriched_pending,
        count(*) FILTER (WHERE status = 'validated')::text AS validated,
        count(*) FILTER (WHERE status = 'failed')::text AS failed,
        count(*) FILTER (WHERE status = 'rejected')::text AS rejected,
        count(*) FILTER (WHERE status = 'written')::text AS written
      FROM staging_items
      WHERE item_type = 'state_resources'
    `
  );

  const row = result.rows[0];
  return {
    total: Number.parseInt(row?.total ?? "0", 10),
    draftPending: Number.parseInt(row?.draft_pending ?? "0", 10),
    enrichedPending: Number.parseInt(row?.enriched_pending ?? "0", 10),
    validated: Number.parseInt(row?.validated ?? "0", 10),
    failed: Number.parseInt(row?.failed ?? "0", 10),
    rejected: Number.parseInt(row?.rejected ?? "0", 10),
    written: Number.parseInt(row?.written ?? "0", 10),
  };
}

async function runOneDrainRound(config: SchedulerRuntimeConfig): Promise<void> {
  await runStateResourcesEnricher({
    once: true,
    batchSize: config.batchSize,
    blockMs: config.blockMs,
  });
  await runStateResourcesValidator({
    once: true,
    batchSize: config.batchSize,
    blockMs: config.blockMs,
  });
  await runStateResourcesWriter({
    once: true,
    batchSize: config.batchSize,
    blockMs: config.blockMs,
  });
}

/**
 * Drains all currently in-flight rows:
 * - pending draft rows
 * - pending enriched rows
 * - validated rows waiting for writer
 */
async function drainCurrentWork(
  pool: Pool,
  config: SchedulerRuntimeConfig,
  label: string
): Promise<void> {
  const initial = await loadStateResourceStageCounts(pool);
  if (!hasInFlightItems(initial)) {
    return;
  }

  const maxRounds = getDrainRoundLimit(initial.total, config.batchSize);
  let rounds = 0;
  let stagnantRounds = 0;
  let lastSignature: string | null = null;

  while (rounds < maxRounds) {
    await runOneDrainRound(config);
    rounds += 1;

    const after = await loadStateResourceStageCounts(pool);
    if (!hasInFlightItems(after)) {
      return;
    }

    const signature = `${after.draftPending}:${after.enrichedPending}:${after.validated}:${after.failed}:${after.rejected}:${after.written}`;
    if (signature === lastSignature) {
      stagnantRounds += 1;
    } else {
      stagnantRounds = 0;
      lastSignature = signature;
    }

    if (stagnantRounds >= 2) {
      throw new Error(
        `state_resources ${label} made no forward progress after ${rounds} rounds (inflight: draft=${after.draftPending}, enriched=${after.enrichedPending}, validated=${after.validated})`
      );
    }
  }

  const remaining = await loadStateResourceStageCounts(pool);
  throw new Error(
    `state_resources ${label} exceeded drain round limit (${maxRounds}) with inflight rows remaining (draft=${remaining.draftPending}, enriched=${remaining.enrichedPending}, validated=${remaining.validated})`
  );
}

/**
 * Creates the BullMQ queue used for recurring and manual state_resources refresh jobs.
 */
export function createStateResourcesSchedulerQueue(): Queue<StateResourcesRefreshJobData> {
  return new Queue<StateResourcesRefreshJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

/**
 * Upserts recurring scheduler:
 * - monthly full refresh
 */
export async function upsertRecurringStateResourcesRefreshJobs(
  jobData: StateResourcesRefreshJobData = {}
): Promise<void> {
  const config = readSchedulerRuntimeConfig();
  const queue = createStateResourcesSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      STATE_RESOURCES_ANNUAL_SCHEDULER_ID,
      {
        pattern: config.monthlyCron,
        tz: config.monthlyTz,
      },
      {
        name: STATE_RESOURCES_REFRESH_JOB_NAME,
        data: {
          dryRun: Boolean(jobData.dryRun),
          force: Boolean(jobData.force),
          triggeredBy: "monthly",
        },
        opts: defaultJobOptions(),
      }
    );

    await queue.removeJobScheduler(STATE_RESOURCES_PRE_ELECTION_WEEKLY_SCHEDULER_ID);
  } finally {
    await queue.close();
  }
}

/**
 * Backward-compatible alias (historical name).
 */
export async function upsertAnnualStateResourcesRefreshJob(
  jobData: StateResourcesRefreshJobData = {}
): Promise<void> {
  await upsertRecurringStateResourcesRefreshJobs(jobData);
}

/**
 * Enqueues a one-off manual refresh job.
 */
export async function enqueueManualStateResourcesRefreshJob(
  jobData: StateResourcesRefreshJobData = {}
): Promise<string> {
  const queue = createStateResourcesSchedulerQueue();

  try {
    const job = await queue.add(
      STATE_RESOURCES_REFRESH_JOB_NAME,
      {
        dryRun: Boolean(jobData.dryRun),
        force: Boolean(jobData.force),
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

/**
 * Executes one refresh pipeline run.
 * dryRun=true only runs producer preflight.
 */
export async function runStateResourcesRefreshJob(
  data: StateResourcesRefreshJobData = {}
): Promise<StateResourcesRefreshJobResult> {
  const config = readSchedulerRuntimeConfig();
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const dryRun = Boolean(data.dryRun);
  const force = Boolean(data.force);
  const triggeredBy = data.triggeredBy ?? "manual";
  const startedAt = new Date().toISOString();

  try {
    await runStateResourcesProducer({ dryRun, force });

    if (dryRun) {
      return {
        dryRun,
        force,
        triggeredBy,
        skipped: false,
        skipReason: null,
        passes: 0,
        retrySweeps: 0,
        requeuedToDraft: 0,
        requeuedToPending: 0,
        startedAt,
        finishedAt: new Date().toISOString(),
      };
    }

    let passes = 0;
    let retrySweeps = 0;
    let requeuedToDraft = 0;
    let requeuedToPending = 0;

    // Pass 1: drain all current rows, regardless of count.
    passes += 1;
    await drainCurrentWork(pool, config, "pass_1");

    // Pass 2..N: retry only failed/rejected rows; stop early when nothing is requeued.
    const maxRetryPasses = Math.max(0, config.maxPasses - 1);
    for (let retryPass = 0; retryPass < maxRetryPasses; retryPass += 1) {
      const countsBeforeRetry = await loadStateResourceStageCounts(pool);
      const retry = await runStateResourcesRetrySweeper({
        maxItems: Math.max(config.batchSize, countsBeforeRetry.total),
      });

      retrySweeps += 1;
      requeuedToDraft += retry.requeuedToDraft;
      requeuedToPending += retry.requeuedToPending;

      if (retry.requeuedToDraft + retry.requeuedToPending === 0) {
        break;
      }

      passes += 1;
      await drainCurrentWork(pool, config, `pass_${passes}`);
    }

    return {
      dryRun,
      force,
      triggeredBy,
      skipped: false,
      skipReason: null,
      passes,
      retrySweeps,
      requeuedToDraft,
      requeuedToPending,
      startedAt,
      finishedAt: new Date().toISOString(),
    };
  } finally {
    await pool.end();
  }
}

/**
 * Creates a BullMQ worker for scheduled/manual refresh jobs.
 */
export function createStateResourcesSchedulerWorker(): Worker<StateResourcesRefreshJobData, StateResourcesRefreshJobResult> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<StateResourcesRefreshJobData, StateResourcesRefreshJobResult> = async (job) => {
    return runStateResourcesRefreshJob(job.data ?? {});
  };

  return new Worker<StateResourcesRefreshJobData, StateResourcesRefreshJobResult>(queueName, processor, {
    connection,
    concurrency: 1,
  });
}
