import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";

import { getPipelineEnv } from "../config/env.js";
import {
  runSourceUrlHealthProducer,
  type SourceUrlHealthProducerResult,
} from "../pipeline/elections/sourceUrlHealthProducer.js";

export const SOURCE_URL_HEALTH_JOB_NAME = "source_url_health_check";
export const SOURCE_URL_HEALTH_DAILY_SCHEDULER_ID = "source_url_health_daily";

export type SourceUrlHealthJobData = {
  dryRun?: boolean;
  force?: boolean;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type SourceUrlHealthJobResult = SourceUrlHealthProducerResult & {
  triggeredBy: NonNullable<SourceUrlHealthJobData["triggeredBy"]>;
};

type SourceUrlHealthSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

function readSchedulerRuntimeConfig(): SourceUrlHealthSchedulerRuntimeConfig {
  return {
    queueName: process.env.SOURCE_URL_HEALTH_SCHEDULER_QUEUE?.trim() || "source_url_health_maintenance",
    dailyCron: process.env.SOURCE_URL_HEALTH_DAILY_CRON?.trim() || "15 3 * * *",
    dailyTz: process.env.SOURCE_URL_HEALTH_DAILY_TZ?.trim() || "UTC",
  };
}

function toConnectionOptions(redisUrl: string): ConnectionOptions {
  const parsed = new URL(redisUrl);
  const parsedPort = parsed.port ? Number.parseInt(parsed.port, 10) : 6379;
  const parsedDb =
    parsed.pathname.length > 1 ? Number.parseInt(parsed.pathname.slice(1), 10) : 0;
  if (!Number.isInteger(parsedPort) || parsedPort <= 0) {
    throw new Error(`Invalid REDIS_URL port in source_url_health scheduler: ${redisUrl}`);
  }
  if (!Number.isInteger(parsedDb) || parsedDb < 0) {
    throw new Error(`Invalid REDIS_URL db index in source_url_health scheduler: ${redisUrl}`);
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

export function createSourceUrlHealthSchedulerQueue(): Queue<SourceUrlHealthJobData> {
  return new Queue<SourceUrlHealthJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringSourceUrlHealthJobs(
  jobData: SourceUrlHealthJobData = {}
): Promise<void> {
  const config = readSchedulerRuntimeConfig();
  const queue = createSourceUrlHealthSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      SOURCE_URL_HEALTH_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: SOURCE_URL_HEALTH_JOB_NAME,
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

export async function enqueueManualSourceUrlHealthJob(
  jobData: SourceUrlHealthJobData = {}
): Promise<string> {
  const queue = createSourceUrlHealthSchedulerQueue();

  try {
    const job = await queue.add(
      SOURCE_URL_HEALTH_JOB_NAME,
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

export async function runSourceUrlHealthJob(
  data: SourceUrlHealthJobData = {}
): Promise<SourceUrlHealthJobResult> {
  const triggeredBy = data.triggeredBy ?? "unknown";
  const result = await runSourceUrlHealthProducer({
    dryRun: Boolean(data.dryRun),
    force: Boolean(data.force),
  });

  return {
    ...result,
    triggeredBy,
  };
}

export function createSourceUrlHealthSchedulerWorker(): Worker<
  SourceUrlHealthJobData,
  SourceUrlHealthJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<SourceUrlHealthJobData, SourceUrlHealthJobResult> = async (job) => {
    return runSourceUrlHealthJob(job.data ?? {});
  };

  return new Worker<SourceUrlHealthJobData, SourceUrlHealthJobResult>(queueName, processor, {
    connection,
    concurrency: 1,
  });
}
