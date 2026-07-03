import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import {
  buildDigestMailerFromEnv,
  sendCandidateFollowDigests,
  withDigestRunLock,
  DEFAULT_DIGEST_MAX_ITEMS_PER_EMAIL,
  DEFAULT_DIGEST_MAX_USERS,
  type SendCandidateFollowDigestsResult,
} from "../scripts/sendCandidateFollowDigests.js";

export const CANDIDATE_FOLLOW_DIGEST_JOB_NAME = "candidate_follow_digest";
export const CANDIDATE_FOLLOW_DIGEST_DAILY_SCHEDULER_ID = "candidate_follow_digest_daily";

export type CandidateFollowDigestJobData = {
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
  maxUsers?: number;
  maxItemsPerEmail?: number;
};

export type CandidateFollowDigestJobResult = SendCandidateFollowDigestsResult & {
  triggeredBy: NonNullable<CandidateFollowDigestJobData["triggeredBy"]>;
  /** True when another live run held the advisory lock and this job did nothing. */
  lockSkipped?: true;
};

type CandidateFollowDigestSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

function readSchedulerRuntimeConfig(): CandidateFollowDigestSchedulerRuntimeConfig {
  return {
    queueName: process.env.CANDIDATE_FOLLOW_DIGEST_SCHEDULER_QUEUE?.trim() || "candidate_follow_digest",
    dailyCron: process.env.CANDIDATE_FOLLOW_DIGEST_DAILY_CRON?.trim() || "0 14 * * *",
    dailyTz: process.env.CANDIDATE_FOLLOW_DIGEST_DAILY_TZ?.trim() || "UTC",
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
    // Retry whole-job transients (DB down at cron time) instead of skipping a
    // digest day. Safe to re-run: delivered events are already marked, so a
    // retry only processes users whose events are still unsent.
    attempts: 3,
    backoff: { type: "exponential", delay: 30_000 },
  };
}

export function createCandidateFollowDigestSchedulerQueue(): Queue<CandidateFollowDigestJobData> {
  return new Queue<CandidateFollowDigestJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringCandidateFollowDigestJobs(): Promise<void> {
  const config = readSchedulerRuntimeConfig();
  const queue = createCandidateFollowDigestSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      CANDIDATE_FOLLOW_DIGEST_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: CANDIDATE_FOLLOW_DIGEST_JOB_NAME,
        data: {
          triggeredBy: "daily",
        },
        opts: defaultJobOptions(),
      }
    );
  } finally {
    await queue.close();
  }
}

// No manual enqueue helper on purpose: ad-hoc runs go through
// `npm run notifications:digest -- --live` directly, without the queue.

export async function runCandidateFollowDigestJob(
  data: CandidateFollowDigestJobData = {}
): Promise<CandidateFollowDigestJobResult> {
  const triggeredBy = data.triggeredBy ?? "unknown";
  if (!data.triggeredBy) {
    console.warn("candidate_follow_digest job missing triggeredBy; recording as unknown");
  }

  const env = getPipelineEnv();
  const mailer = buildDigestMailerFromEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  try {
    // Failures stay in the result instead of failing the job: delivered events
    // are already marked notified_at, so the next scheduled run touches only
    // events from failed sends (stage "send", retried) or failed marks
    // (stage "mark_after_send", re-sent — the at-least-once duplicate).
    const result = await withDigestRunLock(pool, () =>
      sendCandidateFollowDigests(pool, mailer, {
        live: true,
        maxUsers: data.maxUsers ?? DEFAULT_DIGEST_MAX_USERS,
        maxItemsPerEmail: data.maxItemsPerEmail ?? DEFAULT_DIGEST_MAX_ITEMS_PER_EMAIL,
      })
    );
    if (result === null) {
      console.warn("candidate_follow_digest job skipped: another live digest run holds the lock");
      return {
        dryRun: false,
        resolvedWithoutEmailCount: 0,
        eligibleUserCount: 0,
        eventsPendingCount: 0,
        usersEmailedCount: 0,
        eventsDeliveredCount: 0,
        failures: [],
        triggeredBy,
        lockSkipped: true,
      };
    }
    return {
      ...result,
      triggeredBy,
    };
  } finally {
    await pool.end();
  }
}

export function createCandidateFollowDigestSchedulerWorker(): Worker<
  CandidateFollowDigestJobData,
  CandidateFollowDigestJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<CandidateFollowDigestJobData, CandidateFollowDigestJobResult> = async (job) => {
    return runCandidateFollowDigestJob(job.data ?? {});
  };

  return new Worker<CandidateFollowDigestJobData, CandidateFollowDigestJobResult>(queueName, processor, {
    connection,
    concurrency: 1,
  });
}
