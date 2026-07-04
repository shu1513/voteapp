import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";

import { getPipelineEnv } from "../config/env.js";
import {
  isCaliforniaCampaignFinanceEnabled,
  isCaliforniaCampaignFinanceRawDataRefreshEnabled,
} from "../config/featureFlags.js";
import {
  CAL_ACCESS_RAW_DATA_FETCH_TIMEOUT_MS,
  CAL_ACCESS_RAW_DATA_ZIP_URL,
  DEFAULT_CAL_ACCESS_RAW_DATA_CACHE_DIR,
  parseCalAccessHttpsUrl,
  refreshCalAccessRawDataArtifactCache,
  type CalAccessRawDataArtifactRefreshResult,
} from "../pipeline/californiaFinance/calAccessRawDataArtifactCache.js";

export const CALIFORNIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_JOB_NAME =
  "california_campaign_finance_raw_data_refresh";
export const CALIFORNIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_DAILY_SCHEDULER_ID =
  "california_campaign_finance_raw_data_refresh_daily";

export type CaliforniaCampaignFinanceRawDataRefreshJobData = {
  force?: boolean;
  url?: string;
  cacheDir?: string;
  timeoutMs?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type CaliforniaCampaignFinanceRawDataRefreshJobResult = {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<CaliforniaCampaignFinanceRawDataRefreshJobData["triggeredBy"]>;
  status: "disabled" | CalAccessRawDataArtifactRefreshResult["status"];
  refresh: CalAccessRawDataArtifactRefreshResult | null;
};

type RawDataRefreshSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

function readSchedulerRuntimeConfig(): RawDataRefreshSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.CALIFORNIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_QUEUE?.trim() ||
      "california_campaign_finance_raw_data_refresh_maintenance",
    dailyCron: process.env.CALIFORNIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_DAILY_CRON?.trim() || "15 8 * * *",
    dailyTz: process.env.CALIFORNIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  if (value !== undefined && (!Number.isInteger(value) || value <= 0)) {
    throw new Error(`Invalid California raw data refresh scheduler ${label}: ${value}`);
  }
}

function normalizeRefreshJobData(data: CaliforniaCampaignFinanceRawDataRefreshJobData): Required<
  Pick<CaliforniaCampaignFinanceRawDataRefreshJobData, "url" | "cacheDir" | "timeoutMs">
> {
  return {
    url: parseCalAccessHttpsUrl(data.url?.trim() || CAL_ACCESS_RAW_DATA_ZIP_URL, "url"),
    cacheDir: data.cacheDir?.trim() || DEFAULT_CAL_ACCESS_RAW_DATA_CACHE_DIR,
    timeoutMs: data.timeoutMs ?? CAL_ACCESS_RAW_DATA_FETCH_TIMEOUT_MS,
  };
}

function assertValidJobOptions(data: CaliforniaCampaignFinanceRawDataRefreshJobData): void {
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

export function createCaliforniaCampaignFinanceRawDataRefreshQueue(): Queue<CaliforniaCampaignFinanceRawDataRefreshJobData> {
  return new Queue<CaliforniaCampaignFinanceRawDataRefreshJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringCaliforniaCampaignFinanceRawDataRefreshJobs(
  jobData: CaliforniaCampaignFinanceRawDataRefreshJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isCaliforniaCampaignFinanceEnabled()) {
    const queue = createCaliforniaCampaignFinanceRawDataRefreshQueue();
    try {
      await queue.removeJobScheduler(CALIFORNIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const normalized = normalizeRefreshJobData(jobData);
  const queue = createCaliforniaCampaignFinanceRawDataRefreshQueue();

  try {
    await queue.upsertJobScheduler(
      CALIFORNIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: CALIFORNIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_JOB_NAME,
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

export async function enqueueManualCaliforniaCampaignFinanceRawDataRefreshJob(
  jobData: CaliforniaCampaignFinanceRawDataRefreshJobData = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isCaliforniaCampaignFinanceRawDataRefreshEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const normalized = normalizeRefreshJobData(jobData);
  const queue = createCaliforniaCampaignFinanceRawDataRefreshQueue();

  try {
    const job = await queue.add(
      CALIFORNIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_JOB_NAME,
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

export async function runCaliforniaCampaignFinanceRawDataRefreshJob(
  data: CaliforniaCampaignFinanceRawDataRefreshJobData = {}
): Promise<CaliforniaCampaignFinanceRawDataRefreshJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const enabled = isCaliforniaCampaignFinanceRawDataRefreshEnabled(force);

  if (!data.triggeredBy) {
    console.warn("California raw data refresh job missing triggeredBy; recording as unknown");
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
  const refresh = await refreshCalAccessRawDataArtifactCache({
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

export function createCaliforniaCampaignFinanceRawDataRefreshWorker(): Worker<
  CaliforniaCampaignFinanceRawDataRefreshJobData,
  CaliforniaCampaignFinanceRawDataRefreshJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<
    CaliforniaCampaignFinanceRawDataRefreshJobData,
    CaliforniaCampaignFinanceRawDataRefreshJobResult
  > = async (job) => {
    return runCaliforniaCampaignFinanceRawDataRefreshJob(job.data ?? {});
  };

  return new Worker<CaliforniaCampaignFinanceRawDataRefreshJobData, CaliforniaCampaignFinanceRawDataRefreshJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
