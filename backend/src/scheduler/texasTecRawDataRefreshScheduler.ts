import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";

import { getPipelineEnv } from "../config/env.js";
import {
  isTexasCampaignFinanceEnabled,
  isTexasTecRawDataRefreshEnabled,
} from "../config/featureFlags.js";
import {
  DEFAULT_TEXAS_TEC_CSV_DATABASE_CACHE_DIR,
  TEXAS_TEC_CSV_DATABASE_FETCH_TIMEOUT_MS,
  TEXAS_TEC_CSV_DATABASE_URL,
  parseTexasTecHttpsUrl,
  refreshTexasTecCsvDatabaseArtifactCache,
  type TexasTecCsvDatabaseArtifactRefreshResult,
} from "../pipeline/texasFinance/texasTecCsvDatabaseArtifactCache.js";

export const TEXAS_TEC_RAW_DATA_REFRESH_JOB_NAME = "texas_tec_raw_data_refresh";
export const TEXAS_TEC_RAW_DATA_REFRESH_DAILY_SCHEDULER_ID = "texas_tec_raw_data_refresh_daily";

export type TexasTecRawDataRefreshJobData = {
  force?: boolean;
  url?: string;
  cacheDir?: string;
  timeoutMs?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type TexasTecRawDataRefreshJobResult = {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<TexasTecRawDataRefreshJobData["triggeredBy"]>;
  status: "disabled" | TexasTecCsvDatabaseArtifactRefreshResult["status"];
  refresh: TexasTecCsvDatabaseArtifactRefreshResult | null;
};

type RawDataRefreshSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

type NormalizedRefreshJobData = Required<Pick<TexasTecRawDataRefreshJobData, "url" | "cacheDir" | "timeoutMs">>;

function readSchedulerRuntimeConfig(): RawDataRefreshSchedulerRuntimeConfig {
  return {
    queueName: process.env.TEXAS_TEC_RAW_DATA_REFRESH_QUEUE?.trim() || "texas_tec_raw_data_refresh_maintenance",
    dailyCron: process.env.TEXAS_TEC_RAW_DATA_REFRESH_DAILY_CRON?.trim() || "25 8 * * *",
    dailyTz: process.env.TEXAS_TEC_RAW_DATA_REFRESH_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  if (value !== undefined && (!Number.isInteger(value) || value <= 0)) {
    throw new Error(`Invalid Texas TEC raw data refresh scheduler ${label}: ${value}`);
  }
}

function normalizeRefreshJobData(data: TexasTecRawDataRefreshJobData): NormalizedRefreshJobData {
  return {
    url: parseTexasTecHttpsUrl(data.url?.trim() || TEXAS_TEC_CSV_DATABASE_URL, "url"),
    cacheDir: data.cacheDir?.trim() || DEFAULT_TEXAS_TEC_CSV_DATABASE_CACHE_DIR,
    timeoutMs: data.timeoutMs ?? TEXAS_TEC_CSV_DATABASE_FETCH_TIMEOUT_MS,
  };
}

function assertValidJobOptions(data: TexasTecRawDataRefreshJobData): void {
  assertPositiveInteger(data.timeoutMs, "timeoutMs");
  normalizeRefreshJobData(data);
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

export function createTexasTecRawDataRefreshQueue(): Queue<TexasTecRawDataRefreshJobData> {
  return new Queue<TexasTecRawDataRefreshJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringTexasTecRawDataRefreshJobs(
  jobData: TexasTecRawDataRefreshJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  const queue = createTexasTecRawDataRefreshQueue();

  try {
    if (!isTexasCampaignFinanceEnabled()) {
      await queue.removeJobScheduler(TEXAS_TEC_RAW_DATA_REFRESH_DAILY_SCHEDULER_ID);
      return;
    }

    const config = readSchedulerRuntimeConfig();
    const normalized = normalizeRefreshJobData(jobData);
    await queue.upsertJobScheduler(
      TEXAS_TEC_RAW_DATA_REFRESH_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: TEXAS_TEC_RAW_DATA_REFRESH_JOB_NAME,
        data: {
          force: Boolean(jobData.force),
          url: normalized.url,
          cacheDir: normalized.cacheDir,
          timeoutMs: normalized.timeoutMs,
          triggeredBy: "daily",
        },
        opts: defaultJobOptions(),
      }
    );
  } finally {
    await queue.close();
  }
}

export async function enqueueManualTexasTecRawDataRefreshJob(
  jobData: TexasTecRawDataRefreshJobData = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isTexasTecRawDataRefreshEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const normalized = normalizeRefreshJobData(jobData);
  const queue = createTexasTecRawDataRefreshQueue();

  try {
    const job = await queue.add(
      TEXAS_TEC_RAW_DATA_REFRESH_JOB_NAME,
      {
        force: Boolean(jobData.force),
        url: normalized.url,
        cacheDir: normalized.cacheDir,
        timeoutMs: normalized.timeoutMs,
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

export async function runTexasTecRawDataRefreshJob(
  data: TexasTecRawDataRefreshJobData = {}
): Promise<TexasTecRawDataRefreshJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const enabled = isTexasTecRawDataRefreshEnabled(force);

  if (!data.triggeredBy) {
    console.warn("Texas TEC raw data refresh job missing triggeredBy; recording as unknown");
  }

  if (!enabled) {
    return {
      enabled: false,
      force,
      triggeredBy,
      status: "disabled",
      refresh: null,
    };
  }

  const normalized = normalizeRefreshJobData(data);
  const refresh = await refreshTexasTecCsvDatabaseArtifactCache({
    cacheDir: normalized.cacheDir,
    url: normalized.url,
    force,
    timeoutMs: normalized.timeoutMs,
  });

  return {
    enabled: true,
    force,
    triggeredBy,
    status: refresh.status,
    refresh,
  };
}

export function createTexasTecRawDataRefreshWorker(): Worker<
  TexasTecRawDataRefreshJobData,
  TexasTecRawDataRefreshJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<TexasTecRawDataRefreshJobData, TexasTecRawDataRefreshJobResult> = async (job) => {
    return runTexasTecRawDataRefreshJob(job.data ?? {});
  };

  return new Worker<TexasTecRawDataRefreshJobData, TexasTecRawDataRefreshJobResult>(queueName, processor, {
    connection,
    concurrency: 1,
  });
}
