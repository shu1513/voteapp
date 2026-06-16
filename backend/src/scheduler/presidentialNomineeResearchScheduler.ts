import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";

import { getPipelineEnv } from "../config/env.js";
import { isPresidentialElectionsEnabled } from "../config/featureFlags.js";
import {
  runPresidentialNomineeResearchProducer,
  type PresidentialNomineeResearchProducerResult,
} from "../pipeline/producers/presidentialNomineeResearchProducer.js";

export const PRESIDENTIAL_NOMINEE_RESEARCH_ROLLOVER_JOB_NAME = "presidential_nominee_research_rollover";
export const PRESIDENTIAL_NOMINEE_RESEARCH_DAILY_SCHEDULER_ID =
  "presidential_nominee_research_daily_rollover";

export type PresidentialNomineeResearchRolloverJobData = {
  dryRun?: boolean;
  force?: boolean;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type PresidentialNomineeResearchRolloverJobResult =
  PresidentialNomineeResearchProducerResult & {
    triggeredBy: NonNullable<PresidentialNomineeResearchRolloverJobData["triggeredBy"]>;
  };

type PresidentialNomineeResearchSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

function readSchedulerRuntimeConfig(): PresidentialNomineeResearchSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.PRESIDENTIAL_NOMINEE_RESEARCH_SCHEDULER_QUEUE?.trim() ||
      "presidential_nominee_research_scheduler",
    dailyCron: process.env.PRESIDENTIAL_NOMINEE_RESEARCH_DAILY_CRON?.trim() || "15 8 * * *",
    dailyTz: process.env.PRESIDENTIAL_NOMINEE_RESEARCH_DAILY_TZ?.trim() || "UTC",
  };
}

function toConnectionOptions(redisUrl: string): ConnectionOptions {
  const parsed = new URL(redisUrl);
  if (parsed.protocol !== "redis:" && parsed.protocol !== "rediss:") {
    throw new Error(`Unsupported REDIS_URL protocol: ${parsed.protocol}`);
  }
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

export function createPresidentialNomineeResearchSchedulerQueue(): Queue<PresidentialNomineeResearchRolloverJobData> {
  return new Queue<PresidentialNomineeResearchRolloverJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringPresidentialNomineeResearchJobs(
  jobData: PresidentialNomineeResearchRolloverJobData = {}
): Promise<void> {
  if (!isPresidentialElectionsEnabled()) {
    const queue = createPresidentialNomineeResearchSchedulerQueue();
    try {
      await queue.removeJobScheduler(PRESIDENTIAL_NOMINEE_RESEARCH_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const queue = createPresidentialNomineeResearchSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      PRESIDENTIAL_NOMINEE_RESEARCH_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: PRESIDENTIAL_NOMINEE_RESEARCH_ROLLOVER_JOB_NAME,
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

export async function enqueueManualPresidentialNomineeResearchJob(
  jobData: PresidentialNomineeResearchRolloverJobData = {}
): Promise<string> {
  if (!isPresidentialElectionsEnabled()) {
    return "disabled";
  }

  const queue = createPresidentialNomineeResearchSchedulerQueue();

  try {
    const job = await queue.add(
      PRESIDENTIAL_NOMINEE_RESEARCH_ROLLOVER_JOB_NAME,
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

export async function runPresidentialNomineeResearchRolloverJob(
  data: PresidentialNomineeResearchRolloverJobData = {}
): Promise<PresidentialNomineeResearchRolloverJobResult> {
  const triggeredBy = data.triggeredBy ?? "unknown";
  if (!data.triggeredBy) {
    console.warn("presidential_nominee_research rollover job missing triggeredBy; recording as unknown");
  }

  const result = await runPresidentialNomineeResearchProducer({
    dryRun: Boolean(data.dryRun),
    force: Boolean(data.force),
  });
  return {
    ...result,
    triggeredBy,
  };
}

export function createPresidentialNomineeResearchSchedulerWorker(): Worker<
  PresidentialNomineeResearchRolloverJobData,
  PresidentialNomineeResearchRolloverJobResult
> {
  const processor: Processor<
    PresidentialNomineeResearchRolloverJobData,
    PresidentialNomineeResearchRolloverJobResult
  > = async (job) => {
    if (job.name !== PRESIDENTIAL_NOMINEE_RESEARCH_ROLLOVER_JOB_NAME) {
      throw new Error(`Unsupported presidential nominee research rollover job name: ${job.name}`);
    }
    return runPresidentialNomineeResearchRolloverJob(job.data ?? {});
  };

  return new Worker<PresidentialNomineeResearchRolloverJobData, PresidentialNomineeResearchRolloverJobResult>(
    getQueueName(),
    processor,
    {
      connection: getQueueConnection(),
      concurrency: 1,
    }
  );
}
