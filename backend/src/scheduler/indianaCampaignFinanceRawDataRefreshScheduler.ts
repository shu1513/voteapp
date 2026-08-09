import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";

import { getPipelineEnv } from "../config/env.js";
import {
  isIndianaCampaignFinanceEnabled,
  isIndianaCampaignFinanceRawDataRefreshEnabled,
} from "../config/featureFlags.js";
import {
  DEFAULT_INDIANA_CAMPAIGN_FINANCE_CACHE_DIR,
  INDIANA_CAMPAIGN_FINANCE_DOWNLOAD_TIMEOUT_MS,
  buildIndianaCampaignFinanceArtifactUrl,
  normalizeIndianaCampaignFinanceArtifactKind,
  normalizeIndianaCampaignFinanceYear,
  parseIndianaCampaignFinanceHttpsUrl,
  refreshIndianaCampaignFinanceArtifactCache,
  type IndianaCampaignFinanceArtifactKind,
  type IndianaCampaignFinanceArtifactRefreshResult,
} from "../pipeline/indianaFinance/indianaCampaignFinanceArtifactCache.js";

export const INDIANA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_JOB_NAME =
  "indiana_campaign_finance_raw_data_refresh";
export const INDIANA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_DAILY_SCHEDULER_ID =
  "indiana_campaign_finance_raw_data_refresh_daily";

export type IndianaCampaignFinanceRawDataRefreshJobData = {
  force?: boolean;
  year?: number;
  artifactKind?: IndianaCampaignFinanceArtifactKind;
  url?: string;
  cacheDir?: string;
  timeoutMs?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type IndianaCampaignFinanceRawDataRefreshJobResult = {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<IndianaCampaignFinanceRawDataRefreshJobData["triggeredBy"]>;
  status: "disabled" | IndianaCampaignFinanceArtifactRefreshResult["status"];
  refresh: IndianaCampaignFinanceArtifactRefreshResult | null;
};

type IndianaCampaignFinanceRawDataRefreshSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

type NormalizedRefreshJobData = Required<
  Pick<
    IndianaCampaignFinanceRawDataRefreshJobData,
    "year" | "artifactKind" | "url" | "cacheDir" | "timeoutMs"
  >
>;

function readSchedulerRuntimeConfig(): IndianaCampaignFinanceRawDataRefreshSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.INDIANA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_QUEUE?.trim() ||
      "indiana_campaign_finance_raw_data_refresh_maintenance",
    dailyCron: process.env.INDIANA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_DAILY_CRON?.trim() || "10 10 * * *",
    dailyTz: process.env.INDIANA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  // isSafeInteger, not isInteger: Number("9007199254740993") silently rounds
  // to 2^53 and still passes isInteger.
  if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
    throw new Error(`Invalid Indiana raw data refresh scheduler ${label}: ${value}`);
  }
}

function normalizeRefreshJobData(data: IndianaCampaignFinanceRawDataRefreshJobData): NormalizedRefreshJobData {
  const year = normalizeIndianaCampaignFinanceYear(data.year ?? new Date().getUTCFullYear());
  const artifactKind = normalizeIndianaCampaignFinanceArtifactKind(data.artifactKind ?? "contribution");
  return {
    year,
    artifactKind,
    url: parseIndianaCampaignFinanceHttpsUrl(
      data.url?.trim() || buildIndianaCampaignFinanceArtifactUrl({ year, artifactKind }),
      "url"
    ),
    cacheDir: data.cacheDir?.trim() || DEFAULT_INDIANA_CAMPAIGN_FINANCE_CACHE_DIR,
    timeoutMs: data.timeoutMs ?? INDIANA_CAMPAIGN_FINANCE_DOWNLOAD_TIMEOUT_MS,
  };
}

function assertValidJobOptions(data: IndianaCampaignFinanceRawDataRefreshJobData): void {
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

export function createIndianaCampaignFinanceRawDataRefreshQueue(): Queue<IndianaCampaignFinanceRawDataRefreshJobData> {
  return new Queue<IndianaCampaignFinanceRawDataRefreshJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringIndianaCampaignFinanceRawDataRefreshJobs(
  jobData: IndianaCampaignFinanceRawDataRefreshJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  const queue = createIndianaCampaignFinanceRawDataRefreshQueue();

  try {
    if (!isIndianaCampaignFinanceEnabled()) {
      await queue.removeJobScheduler(INDIANA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_DAILY_SCHEDULER_ID);
      return;
    }

    const config = readSchedulerRuntimeConfig();
    const normalized = normalizeRefreshJobData(jobData);
    await queue.upsertJobScheduler(
      INDIANA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: INDIANA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_JOB_NAME,
        data: {
          force: Boolean(jobData.force),
          year: normalized.year,
          artifactKind: normalized.artifactKind,
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

export async function enqueueManualIndianaCampaignFinanceRawDataRefreshJob(
  jobData: IndianaCampaignFinanceRawDataRefreshJobData = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isIndianaCampaignFinanceRawDataRefreshEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const normalized = normalizeRefreshJobData(jobData);
  const queue = createIndianaCampaignFinanceRawDataRefreshQueue();

  try {
    const job = await queue.add(
      INDIANA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_JOB_NAME,
      {
        force: Boolean(jobData.force),
        year: normalized.year,
        artifactKind: normalized.artifactKind,
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

export async function runIndianaCampaignFinanceRawDataRefreshJob(
  data: IndianaCampaignFinanceRawDataRefreshJobData = {}
): Promise<IndianaCampaignFinanceRawDataRefreshJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const enabled = isIndianaCampaignFinanceRawDataRefreshEnabled(force);

  if (!data.triggeredBy) {
    console.warn("Indiana raw data refresh job missing triggeredBy; recording as unknown");
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
  const refresh = await refreshIndianaCampaignFinanceArtifactCache({
    year: normalized.year,
    artifactKind: normalized.artifactKind,
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

export function createIndianaCampaignFinanceRawDataRefreshWorker(): Worker<
  IndianaCampaignFinanceRawDataRefreshJobData,
  IndianaCampaignFinanceRawDataRefreshJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<
    IndianaCampaignFinanceRawDataRefreshJobData,
    IndianaCampaignFinanceRawDataRefreshJobResult
  > = async (job) => {
    return runIndianaCampaignFinanceRawDataRefreshJob(job.data ?? {});
  };

  return new Worker<IndianaCampaignFinanceRawDataRefreshJobData, IndianaCampaignFinanceRawDataRefreshJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
