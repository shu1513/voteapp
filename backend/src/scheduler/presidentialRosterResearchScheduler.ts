import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";

import { getPipelineEnv } from "../config/env.js";
import { isPresidentialElectionsEnabled } from "../config/featureFlags.js";
import {
  runPresidentialRosterResearchProducer,
  type PresidentialRosterResearchProducerResult,
} from "../pipeline/producers/presidentialRosterResearchProducer.js";

export const PRESIDENTIAL_ROSTER_RESEARCH_ROLLOVER_JOB_NAME = "presidential_roster_research_rollover";
export const PRESIDENTIAL_ROSTER_RESEARCH_DAILY_SCHEDULER_ID = "presidential_roster_research_daily_rollover";

export type PresidentialRosterResearchRolloverJobData = {
  dryRun?: boolean;
  force?: boolean;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type PresidentialRosterResearchRolloverJobResult = PresidentialRosterResearchProducerResult & {
  triggeredBy: NonNullable<PresidentialRosterResearchRolloverJobData["triggeredBy"]>;
};

type PresidentialRosterResearchSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

function readSchedulerRuntimeConfig(): PresidentialRosterResearchSchedulerRuntimeConfig {
  return {
    queueName: process.env.PRESIDENTIAL_ROSTER_RESEARCH_SCHEDULER_QUEUE?.trim() || "presidential_roster_research_scheduler",
    dailyCron: process.env.PRESIDENTIAL_ROSTER_RESEARCH_DAILY_CRON?.trim() || "0 7 * * *",
    dailyTz: process.env.PRESIDENTIAL_ROSTER_RESEARCH_DAILY_TZ?.trim() || "UTC",
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

export function createPresidentialRosterResearchSchedulerQueue(): Queue<PresidentialRosterResearchRolloverJobData> {
  return new Queue<PresidentialRosterResearchRolloverJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringPresidentialRosterResearchJobs(
  jobData: PresidentialRosterResearchRolloverJobData = {}
): Promise<void> {
  if (!isPresidentialElectionsEnabled()) {
    const queue = createPresidentialRosterResearchSchedulerQueue();
    try {
      await queue.removeJobScheduler(PRESIDENTIAL_ROSTER_RESEARCH_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const queue = createPresidentialRosterResearchSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      PRESIDENTIAL_ROSTER_RESEARCH_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: PRESIDENTIAL_ROSTER_RESEARCH_ROLLOVER_JOB_NAME,
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

export async function enqueueManualPresidentialRosterResearchJob(
  jobData: PresidentialRosterResearchRolloverJobData = {}
): Promise<string> {
  if (!isPresidentialElectionsEnabled()) {
    return "disabled";
  }

  const queue = createPresidentialRosterResearchSchedulerQueue();

  try {
    const job = await queue.add(
      PRESIDENTIAL_ROSTER_RESEARCH_ROLLOVER_JOB_NAME,
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

export async function runPresidentialRosterResearchRolloverJob(
  data: PresidentialRosterResearchRolloverJobData = {}
): Promise<PresidentialRosterResearchRolloverJobResult> {
  const triggeredBy = data.triggeredBy ?? "unknown";
  if (!data.triggeredBy) {
    console.warn("presidential_roster_research rollover job missing triggeredBy; recording as unknown");
  }

  const result = await runPresidentialRosterResearchProducer({
    dryRun: Boolean(data.dryRun),
    force: Boolean(data.force),
  });
  return {
    ...result,
    triggeredBy,
  };
}

export function createPresidentialRosterResearchSchedulerWorker(): Worker<
  PresidentialRosterResearchRolloverJobData,
  PresidentialRosterResearchRolloverJobResult
> {
  const processor: Processor<
    PresidentialRosterResearchRolloverJobData,
    PresidentialRosterResearchRolloverJobResult
  > = async (job) => {
    if (job.name !== PRESIDENTIAL_ROSTER_RESEARCH_ROLLOVER_JOB_NAME) {
      throw new Error(`Unsupported presidential roster research rollover job name: ${job.name}`);
    }
    return runPresidentialRosterResearchRolloverJob(job.data ?? {});
  };

  return new Worker<PresidentialRosterResearchRolloverJobData, PresidentialRosterResearchRolloverJobResult>(
    getQueueName(),
    processor,
    {
      connection: getQueueConnection(),
      concurrency: 1,
    }
  );
}
