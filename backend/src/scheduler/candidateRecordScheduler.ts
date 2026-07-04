import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";

import { getPipelineEnv } from "../config/env.js";
import {
  runCandidateRecordRolloverProducer,
  type CandidateRecordRolloverProducerResult,
} from "../pipeline/producers/candidateRecordRolloverProducer.js";

export const CANDIDATE_RECORD_ROLLOVER_JOB_NAME = "candidate_record_rollover";
export const CANDIDATE_RECORD_ROLLOVER_DAILY_SCHEDULER_ID = "candidate_record_daily_rollover";

export type CandidateRecordRolloverJobData = {
  force?: boolean;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type CandidateRecordRolloverJobResult = CandidateRecordRolloverProducerResult & {
  force: boolean;
  triggeredBy: NonNullable<CandidateRecordRolloverJobData["triggeredBy"]>;
};

type CandidateRecordSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

function readSchedulerRuntimeConfig(): CandidateRecordSchedulerRuntimeConfig {
  return {
    queueName: process.env.CANDIDATE_RECORD_SCHEDULER_QUEUE?.trim() || "candidate_record_maintenance",
    dailyCron: process.env.CANDIDATE_RECORD_ROLLOVER_DAILY_CRON?.trim() || "30 7 * * *",
    dailyTz: process.env.CANDIDATE_RECORD_ROLLOVER_DAILY_TZ?.trim() || "UTC",
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

export function createCandidateRecordSchedulerQueue(): Queue<CandidateRecordRolloverJobData> {
  return new Queue<CandidateRecordRolloverJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringCandidateRecordRolloverJobs(
  jobData: CandidateRecordRolloverJobData = {}
): Promise<void> {
  const config = readSchedulerRuntimeConfig();
  const queue = createCandidateRecordSchedulerQueue();

  try {
    await queue.upsertJobScheduler(
      CANDIDATE_RECORD_ROLLOVER_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: CANDIDATE_RECORD_ROLLOVER_JOB_NAME,
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

export async function enqueueManualCandidateRecordRolloverJob(
  jobData: CandidateRecordRolloverJobData = {}
): Promise<string> {
  const queue = createCandidateRecordSchedulerQueue();

  try {
    const job = await queue.add(
      CANDIDATE_RECORD_ROLLOVER_JOB_NAME,
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

export async function runCandidateRecordRolloverJob(
  data: CandidateRecordRolloverJobData = {}
): Promise<CandidateRecordRolloverJobResult> {
  const force = Boolean(data.force);
  const triggeredBy = data.triggeredBy ?? "unknown";

  if (!data.triggeredBy) {
    console.warn("candidate_record rollover job missing triggeredBy; recording as unknown");
  }

  const result = await runCandidateRecordRolloverProducer({ force });
  return {
    ...result,
    force,
    triggeredBy,
  };
}

export function createCandidateRecordSchedulerWorker(): Worker<
  CandidateRecordRolloverJobData,
  CandidateRecordRolloverJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<CandidateRecordRolloverJobData, CandidateRecordRolloverJobResult> = async (job) => {
    return runCandidateRecordRolloverJob(job.data ?? {});
  };

  return new Worker<CandidateRecordRolloverJobData, CandidateRecordRolloverJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
