import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";

import { getPipelineEnv } from "../config/env.js";
import {
  runCandidateRosterRolloverProducer,
  type CandidateRosterRolloverProducerResult,
} from "../pipeline/producers/candidateRosterRolloverProducer.js";

export const CANDIDATE_ROSTER_ROLLOVER_JOB_NAME = "candidate_roster_rollover";
export const CANDIDATE_ROSTER_ROLLOVER_DAILY_SCHEDULER_ID = "candidate_roster_daily_rollover";

export type CandidateRosterRolloverJobData = {
  force?: boolean;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type CandidateRosterRolloverJobResult = CandidateRosterRolloverProducerResult & {
  force: boolean;
  triggeredBy: NonNullable<CandidateRosterRolloverJobData["triggeredBy"]>;
};

type CandidateRosterSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

function readSchedulerRuntimeConfig(): CandidateRosterSchedulerRuntimeConfig {
  return {
    queueName: process.env.CANDIDATE_ROSTER_SCHEDULER_QUEUE?.trim() || "candidate_roster_maintenance",
    dailyCron: process.env.CANDIDATE_ROSTER_ROLLOVER_DAILY_CRON?.trim() || "0 7 * * *",
    dailyTz: process.env.CANDIDATE_ROSTER_ROLLOVER_DAILY_TZ?.trim() || "UTC",
  };
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

export function createCandidateRosterSchedulerQueue(): Queue<CandidateRosterRolloverJobData> {
  return new Queue<CandidateRosterRolloverJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringCandidateRosterRolloverJobs(
  jobData: CandidateRosterRolloverJobData = {}
): Promise<void> {
  const config = readSchedulerRuntimeConfig();
  const queue = createCandidateRosterSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      CANDIDATE_ROSTER_ROLLOVER_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: CANDIDATE_ROSTER_ROLLOVER_JOB_NAME,
        data: {
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

export async function enqueueManualCandidateRosterRolloverJob(
  jobData: CandidateRosterRolloverJobData = {}
): Promise<string> {
  const queue = createCandidateRosterSchedulerQueue();

  try {
    const job = await queue.add(
      CANDIDATE_ROSTER_ROLLOVER_JOB_NAME,
      {
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

export async function runCandidateRosterRolloverJob(
  data: CandidateRosterRolloverJobData = {}
): Promise<CandidateRosterRolloverJobResult> {
  const force = Boolean(data.force);
  const triggeredBy = data.triggeredBy ?? "unknown";

  if (!data.triggeredBy) {
    console.warn("candidate_roster rollover job missing triggeredBy; recording as unknown");
  }

  const result = await runCandidateRosterRolloverProducer({ force });
  return {
    ...result,
    force,
    triggeredBy,
  };
}

export function createCandidateRosterSchedulerWorker(): Worker<
  CandidateRosterRolloverJobData,
  CandidateRosterRolloverJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<CandidateRosterRolloverJobData, CandidateRosterRolloverJobResult> = async (job) => {
    return runCandidateRosterRolloverJob(job.data ?? {});
  };

  return new Worker<CandidateRosterRolloverJobData, CandidateRosterRolloverJobResult>(queueName, processor, {
    connection,
    concurrency: 1,
  });
}

