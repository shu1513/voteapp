import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import { buildUnsubscribeUrlBuilderFromEnv } from "../scripts/sendCandidateFollowDigests.js";
import {
  buildAlertMailerFromEnv,
  sendNewElectionAlerts,
  withNewElectionAlertRunLock,
  DEFAULT_ALERT_MAX_ITEMS_PER_EMAIL,
  DEFAULT_ALERT_MAX_USERS,
  type SendNewElectionAlertsResult,
} from "../scripts/sendNewElectionAlerts.js";

export const NEW_ELECTION_ALERT_JOB_NAME = "new_election_alert";
export const NEW_ELECTION_ALERT_DAILY_SCHEDULER_ID = "new_election_alert_daily";

export type NewElectionAlertJobData = {
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
  maxUsers?: number;
  maxItemsPerEmail?: number;
};

export type NewElectionAlertJobResult = SendNewElectionAlertsResult & {
  triggeredBy: NonNullable<NewElectionAlertJobData["triggeredBy"]>;
  /** True when another live run held the advisory lock and this job did nothing. */
  lockSkipped?: true;
};

type NewElectionAlertSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

function readSchedulerRuntimeConfig(): NewElectionAlertSchedulerRuntimeConfig {
  return {
    queueName: process.env.NEW_ELECTION_ALERT_SCHEDULER_QUEUE?.trim() || "new_election_alert",
    // Offset from the digest's 14:00 so the two daily sends do not compete.
    dailyCron: process.env.NEW_ELECTION_ALERT_DAILY_CRON?.trim() || "30 14 * * *",
    dailyTz: process.env.NEW_ELECTION_ALERT_DAILY_TZ?.trim() || "UTC",
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
    // Retry whole-job transients (DB down at cron time) instead of skipping an
    // alert day. Safe to re-run: delivered events are already marked, so a
    // retry only processes users whose events are still unsent.
    attempts: 3,
    backoff: { type: "exponential", delay: 30_000 },
  };
}

export function createNewElectionAlertSchedulerQueue(): Queue<NewElectionAlertJobData> {
  return new Queue<NewElectionAlertJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringNewElectionAlertJobs(): Promise<void> {
  const config = readSchedulerRuntimeConfig();
  const queue = createNewElectionAlertSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      NEW_ELECTION_ALERT_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: NEW_ELECTION_ALERT_JOB_NAME,
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
// `npm run notifications:new-elections -- --live` directly, without the queue.

export async function runNewElectionAlertJob(
  data: NewElectionAlertJobData = {}
): Promise<NewElectionAlertJobResult> {
  const triggeredBy = data.triggeredBy ?? "unknown";
  if (!data.triggeredBy) {
    console.warn("new_election_alert job missing triggeredBy; recording as unknown");
  }

  const env = getPipelineEnv();
  const mailer = buildAlertMailerFromEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  try {
    // Failures stay in the result instead of failing the job: delivered events
    // are already marked notified_at, so the next scheduled run touches only
    // events from failed sends (stage "send", retried) or failed marks
    // (stage "mark_after_send", re-sent — the at-least-once duplicate).
    const buildUnsubscribeUrl = buildUnsubscribeUrlBuilderFromEnv("new_election_alerts");
    const result = await withNewElectionAlertRunLock(pool, () =>
      sendNewElectionAlerts(pool, mailer, {
        live: true,
        maxUsers: data.maxUsers ?? DEFAULT_ALERT_MAX_USERS,
        maxItemsPerEmail: data.maxItemsPerEmail ?? DEFAULT_ALERT_MAX_ITEMS_PER_EMAIL,
        ...(buildUnsubscribeUrl ? { buildUnsubscribeUrl } : {}),
      })
    );
    if (result === null) {
      console.warn("new_election_alert job skipped: another live alert run holds the lock");
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

export function createNewElectionAlertSchedulerWorker(): Worker<NewElectionAlertJobData, NewElectionAlertJobResult> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<NewElectionAlertJobData, NewElectionAlertJobResult> = async (job) => {
    return runNewElectionAlertJob(job.data ?? {});
  };

  return new Worker<NewElectionAlertJobData, NewElectionAlertJobResult>(queueName, processor, {
    connection,
    concurrency: 1,
  });
}
