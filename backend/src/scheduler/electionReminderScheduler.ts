import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import { buildUnsubscribeUrlBuilderFromEnv } from "../scripts/sendCandidateFollowDigests.js";
import {
  buildReminderMailerFromEnv,
  sendElectionReminders,
  withElectionReminderRunLock,
  DEFAULT_REMINDER_MAX_ITEMS_PER_EMAIL,
  DEFAULT_REMINDER_MAX_USERS,
  type SendElectionRemindersResult,
} from "../scripts/sendElectionReminders.js";

export const ELECTION_REMINDER_JOB_NAME = "election_reminder";
export const ELECTION_REMINDER_DAILY_SCHEDULER_ID = "election_reminder_daily";

export type ElectionReminderJobData = {
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
  maxUsers?: number;
  maxItemsPerEmail?: number;
};

export type ElectionReminderJobResult = SendElectionRemindersResult & {
  triggeredBy: NonNullable<ElectionReminderJobData["triggeredBy"]>;
  /** True when another live run held the advisory lock and this job did nothing. */
  lockSkipped?: true;
};

type ElectionReminderSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

function readSchedulerRuntimeConfig(): ElectionReminderSchedulerRuntimeConfig {
  return {
    queueName: process.env.ELECTION_REMINDER_SCHEDULER_QUEUE?.trim() || "election_reminder",
    // 15:00 UTC sits after the digest (14:00) and new-election alerts (14:30)
    // so the daily sends do not compete, lands in the morning across the
    // continental US (11am ET / 8am PT), and falls inside the window where
    // every US timezone is on the same calendar date — so "tomorrow"
    // (US-latest local date + 1) is unambiguous nationwide.
    dailyCron: process.env.ELECTION_REMINDER_DAILY_CRON?.trim() || "0 15 * * *",
    dailyTz: process.env.ELECTION_REMINDER_DAILY_TZ?.trim() || "UTC",
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
    // reminder day. Safe to re-run: reminded users already have a dedupe row,
    // so a retry only emails users who have not been reminded for the date.
    attempts: 3,
    backoff: { type: "exponential", delay: 30_000 },
  };
}

export function createElectionReminderSchedulerQueue(): Queue<ElectionReminderJobData> {
  return new Queue<ElectionReminderJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringElectionReminderJobs(): Promise<void> {
  const config = readSchedulerRuntimeConfig();
  const queue = createElectionReminderSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      ELECTION_REMINDER_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: ELECTION_REMINDER_JOB_NAME,
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
// `npm run notifications:reminders -- --live` directly, without the queue.

export async function runElectionReminderJob(
  data: ElectionReminderJobData = {}
): Promise<ElectionReminderJobResult> {
  const triggeredBy = data.triggeredBy ?? "unknown";
  if (!data.triggeredBy) {
    console.warn("election_reminder job missing triggeredBy; recording as unknown");
  }

  const env = getPipelineEnv();
  const mailer = buildReminderMailerFromEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  try {
    // Failures stay in the result instead of failing the job: reminded users
    // already carry a dedupe row, so a retried job touches only users whose
    // send failed (stage "send") or whose dedupe insert failed (stage
    // "mark_after_send", re-sent — the at-least-once duplicate).
    const buildUnsubscribeUrl = buildUnsubscribeUrlBuilderFromEnv("election_reminders");
    const result = await withElectionReminderRunLock(pool, () =>
      sendElectionReminders(pool, mailer, {
        live: true,
        maxUsers: data.maxUsers ?? DEFAULT_REMINDER_MAX_USERS,
        maxItemsPerEmail: data.maxItemsPerEmail ?? DEFAULT_REMINDER_MAX_ITEMS_PER_EMAIL,
        ...(buildUnsubscribeUrl ? { buildUnsubscribeUrl } : {}),
      })
    );
    if (result === null) {
      console.warn("election_reminder job skipped: another live reminder run holds the lock");
      return {
        dryRun: false,
        // Unknown: the run never started, so the date was never computed.
        targetElectionDate: "",
        eligibleUserCount: 0,
        electionsPendingCount: 0,
        usersEmailedCount: 0,
        usersMarkedCount: 0,
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

export function createElectionReminderSchedulerWorker(): Worker<ElectionReminderJobData, ElectionReminderJobResult> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<ElectionReminderJobData, ElectionReminderJobResult> = async (job) => {
    return runElectionReminderJob(job.data ?? {});
  };

  return new Worker<ElectionReminderJobData, ElectionReminderJobResult>(queueName, processor, {
    connection,
    concurrency: 1,
  });
}
