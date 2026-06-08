import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";

import { getPipelineEnv } from "../config/env.js";
import {
  runElectionResultScheduleProducer,
  type ElectionResultScheduleProducerResult,
} from "../pipeline/producers/electionResultScheduleProducer.js";

export const ELECTION_RESULT_SCHEDULE_ROLLOVER_JOB_NAME = "election_result_schedule_rollover";
export const ELECTION_RESULT_DAILY_SCHEDULER_ID = "election_result_daily_schedule_rollover";

export type ElectionResultScheduleRolloverJobData = {
  dryRun?: boolean;
  force?: boolean;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type ElectionResultScheduleRolloverJobResult = ElectionResultScheduleProducerResult & {
  force: boolean;
  triggeredBy: NonNullable<ElectionResultScheduleRolloverJobData["triggeredBy"]>;
};

type ElectionResultSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

function readSchedulerRuntimeConfig(): ElectionResultSchedulerRuntimeConfig {
  return {
    queueName: process.env.ELECTION_RESULT_SCHEDULER_QUEUE?.trim() || "election_result_maintenance",
    dailyCron: process.env.ELECTION_RESULT_DAILY_CRON?.trim() || "15 0 * * *",
    dailyTz: process.env.ELECTION_RESULT_DAILY_TZ?.trim() || "UTC",
  };
}

function toConnectionOptions(redisUrl: string): ConnectionOptions {
  const parsed = new URL(redisUrl);

  const parsedPort = parsed.port ? Number.parseInt(parsed.port, 10) : 6379;
  const parsedDb = parsed.pathname.length > 1 ? Number.parseInt(parsed.pathname.slice(1), 10) : 0;
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

export function createElectionResultSchedulerQueue(): Queue<ElectionResultScheduleRolloverJobData> {
  return new Queue<ElectionResultScheduleRolloverJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringElectionResultScheduleJobs(
  jobData: ElectionResultScheduleRolloverJobData = {}
): Promise<void> {
  const config = readSchedulerRuntimeConfig();
  const queue = createElectionResultSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      ELECTION_RESULT_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: ELECTION_RESULT_SCHEDULE_ROLLOVER_JOB_NAME,
        data: {
          dryRun: Boolean(jobData.dryRun),
          force: Boolean(jobData.force),
          triggeredBy: "daily",
        },
        opts: defaultJobOptions(),
      }
    );
  } finally {
    await queue.close();
  }
}

export async function enqueueManualElectionResultScheduleJob(
  jobData: ElectionResultScheduleRolloverJobData = {}
): Promise<string> {
  const queue = createElectionResultSchedulerQueue();

  try {
    const job = await queue.add(
      ELECTION_RESULT_SCHEDULE_ROLLOVER_JOB_NAME,
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

export async function runElectionResultScheduleRolloverJob(
  data: ElectionResultScheduleRolloverJobData = {}
): Promise<ElectionResultScheduleRolloverJobResult> {
  const force = Boolean(data.force);
  const triggeredBy = data.triggeredBy ?? "unknown";

  if (!data.triggeredBy) {
    console.warn("election_result schedule rollover job missing triggeredBy; recording as unknown");
  }

  const result = await runElectionResultScheduleProducer({
    dryRun: Boolean(data.dryRun),
    force,
  });

  return {
    ...result,
    force,
    triggeredBy,
  };
}

export function createElectionResultSchedulerWorker(): Worker<
  ElectionResultScheduleRolloverJobData,
  ElectionResultScheduleRolloverJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<
    ElectionResultScheduleRolloverJobData,
    ElectionResultScheduleRolloverJobResult
  > = async (job) => {
    return runElectionResultScheduleRolloverJob(job.data ?? {});
  };

  return new Worker<ElectionResultScheduleRolloverJobData, ElectionResultScheduleRolloverJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
