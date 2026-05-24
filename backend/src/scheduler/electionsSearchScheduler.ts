import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";

import { getPipelineEnv } from "../config/env.js";
import {
  runElectionsSearchRolloverProducer,
  type ElectionsSearchRolloverProducerResult,
} from "../pipeline/producers/electionsSearchRolloverProducer.js";

export const ELECTIONS_SEARCH_ROLLOVER_JOB_NAME = "elections_search_rollover";
export const ELECTIONS_SEARCH_DAILY_SCHEDULER_ID = "elections_search_daily_rollover";

export type ElectionsSearchRolloverJobData = {
  dryRun?: boolean;
  force?: boolean;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type ElectionsSearchRolloverJobResult = ElectionsSearchRolloverProducerResult & {
  triggeredBy: NonNullable<ElectionsSearchRolloverJobData["triggeredBy"]>;
};

type ElectionsSearchSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

function readSchedulerRuntimeConfig(): ElectionsSearchSchedulerRuntimeConfig {
  return {
    queueName: process.env.ELECTIONS_SEARCH_SCHEDULER_QUEUE?.trim() || "elections_search_maintenance",
    dailyCron: process.env.ELECTIONS_SEARCH_DAILY_CRON?.trim() || "0 3 * * *",
    dailyTz: process.env.ELECTIONS_SEARCH_DAILY_TZ?.trim() || "UTC",
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

export function createElectionsSearchSchedulerQueue(): Queue<ElectionsSearchRolloverJobData> {
  return new Queue<ElectionsSearchRolloverJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringElectionsSearchJobs(
  jobData: ElectionsSearchRolloverJobData = {}
): Promise<void> {
  const config = readSchedulerRuntimeConfig();
  const queue = createElectionsSearchSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      ELECTIONS_SEARCH_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: ELECTIONS_SEARCH_ROLLOVER_JOB_NAME,
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

export async function enqueueManualElectionsSearchJob(
  jobData: ElectionsSearchRolloverJobData = {}
): Promise<string> {
  const queue = createElectionsSearchSchedulerQueue();

  try {
    const job = await queue.add(
      ELECTIONS_SEARCH_ROLLOVER_JOB_NAME,
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

export async function runElectionsSearchRolloverJob(
  data: ElectionsSearchRolloverJobData = {}
): Promise<ElectionsSearchRolloverJobResult> {
  const triggeredBy = data.triggeredBy ?? "unknown";
  const result = await runElectionsSearchRolloverProducer({
    dryRun: Boolean(data.dryRun),
    force: Boolean(data.force),
  });

  return {
    ...result,
    triggeredBy,
  };
}

export function createElectionsSearchSchedulerWorker(): Worker<
  ElectionsSearchRolloverJobData,
  ElectionsSearchRolloverJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<ElectionsSearchRolloverJobData, ElectionsSearchRolloverJobResult> = async (job) => {
    return runElectionsSearchRolloverJob(job.data ?? {});
  };

  return new Worker<ElectionsSearchRolloverJobData, ElectionsSearchRolloverJobResult>(queueName, processor, {
    connection,
    concurrency: 1,
  });
}

