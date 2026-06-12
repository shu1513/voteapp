import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";

import { getPipelineEnv } from "../config/env.js";
import {
  runPresidentialPrimaryDateResearchProducer,
  type PresidentialPrimaryDateResearchProducerResult,
} from "../pipeline/producers/presidentialPrimaryDateResearchProducer.js";

export const PRESIDENTIAL_PRIMARY_DATE_RESEARCH_ROLLOVER_JOB_NAME =
  "presidential_primary_date_research_rollover";
export const PRESIDENTIAL_PRIMARY_DATE_RESEARCH_DAILY_SCHEDULER_ID =
  "presidential_primary_date_research_daily_rollover";

export type PresidentialPrimaryDateResearchRolloverJobData = {
  dryRun?: boolean;
  force?: boolean;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type PresidentialPrimaryDateResearchRolloverJobResult =
  PresidentialPrimaryDateResearchProducerResult & {
    force: boolean;
    triggeredBy: NonNullable<PresidentialPrimaryDateResearchRolloverJobData["triggeredBy"]>;
  };

type SchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

function readSchedulerRuntimeConfig(): SchedulerRuntimeConfig {
  return {
    queueName:
      process.env.PRESIDENTIAL_PRIMARY_DATE_RESEARCH_SCHEDULER_QUEUE?.trim() ||
      "presidential_primary_date_research_maintenance",
    dailyCron:
      process.env.PRESIDENTIAL_PRIMARY_DATE_RESEARCH_DAILY_CRON?.trim() ||
      "45 8 * * *",
    dailyTz:
      process.env.PRESIDENTIAL_PRIMARY_DATE_RESEARCH_DAILY_TZ?.trim() ||
      "UTC",
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

export function createPresidentialPrimaryDateResearchSchedulerQueue(): Queue<PresidentialPrimaryDateResearchRolloverJobData> {
  return new Queue<PresidentialPrimaryDateResearchRolloverJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringPresidentialPrimaryDateResearchJobs(
  jobData: PresidentialPrimaryDateResearchRolloverJobData = {}
): Promise<void> {
  const config = readSchedulerRuntimeConfig();
  const queue = createPresidentialPrimaryDateResearchSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      PRESIDENTIAL_PRIMARY_DATE_RESEARCH_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: PRESIDENTIAL_PRIMARY_DATE_RESEARCH_ROLLOVER_JOB_NAME,
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

export async function enqueueManualPresidentialPrimaryDateResearchJob(
  jobData: PresidentialPrimaryDateResearchRolloverJobData = {}
): Promise<string> {
  const queue = createPresidentialPrimaryDateResearchSchedulerQueue();

  try {
    const job = await queue.add(
      PRESIDENTIAL_PRIMARY_DATE_RESEARCH_ROLLOVER_JOB_NAME,
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

export async function runPresidentialPrimaryDateResearchRolloverJob(
  data: PresidentialPrimaryDateResearchRolloverJobData = {}
): Promise<PresidentialPrimaryDateResearchRolloverJobResult> {
  const force = Boolean(data.force);
  const triggeredBy = data.triggeredBy ?? "unknown";

  if (!data.triggeredBy) {
    console.warn(
      "presidential primary date research rollover job missing triggeredBy; recording as unknown"
    );
  }

  const result = await runPresidentialPrimaryDateResearchProducer({
    dryRun: Boolean(data.dryRun),
    force,
  });

  return {
    ...result,
    force,
    triggeredBy,
  };
}

export function createPresidentialPrimaryDateResearchSchedulerWorker(): Worker<
  PresidentialPrimaryDateResearchRolloverJobData,
  PresidentialPrimaryDateResearchRolloverJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<
    PresidentialPrimaryDateResearchRolloverJobData,
    PresidentialPrimaryDateResearchRolloverJobResult
  > = async (job) => {
    return runPresidentialPrimaryDateResearchRolloverJob(job.data ?? {});
  };

  return new Worker<
    PresidentialPrimaryDateResearchRolloverJobData,
    PresidentialPrimaryDateResearchRolloverJobResult
  >(queueName, processor, {
    connection,
    concurrency: 1,
  });
}
