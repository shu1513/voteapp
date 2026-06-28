import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";

import { getPipelineEnv } from "../config/env.js";
import {
  isPennsylvaniaCampaignFinanceEnabled,
  isPennsylvaniaCampaignFinanceRawDataRefreshEnabled,
} from "../config/featureFlags.js";
import {
  DEFAULT_PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_CACHE_DIR,
  PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_FETCH_TIMEOUT_MS,
  buildPennsylvaniaCampaignFinanceExportUrl,
  normalizePennsylvaniaCampaignFinanceExportYear,
  parsePennsylvaniaCampaignFinanceHttpsUrl,
  refreshPennsylvaniaCampaignFinanceExportCache,
  type PennsylvaniaCampaignFinanceExportRefreshResult,
} from "../pipeline/pennsylvaniaFinance/pennsylvaniaCampaignFinanceArtifactCache.js";

export const PENNSYLVANIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_JOB_NAME =
  "pennsylvania_campaign_finance_raw_data_refresh";
export const PENNSYLVANIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_DAILY_SCHEDULER_ID =
  "pennsylvania_campaign_finance_raw_data_refresh_daily";

export type PennsylvaniaCampaignFinanceRawDataRefreshJobData = {
  year?: number;
  force?: boolean;
  url?: string;
  cacheDir?: string;
  timeoutMs?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type PennsylvaniaCampaignFinanceRawDataRefreshJobResult = {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<PennsylvaniaCampaignFinanceRawDataRefreshJobData["triggeredBy"]>;
  status: "disabled" | PennsylvaniaCampaignFinanceExportRefreshResult["status"];
  refresh: PennsylvaniaCampaignFinanceExportRefreshResult | null;
};

type RawDataRefreshSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

function readSchedulerRuntimeConfig(): RawDataRefreshSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.PENNSYLVANIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_QUEUE?.trim() ||
      "pennsylvania_campaign_finance_raw_data_refresh_maintenance",
    dailyCron: process.env.PENNSYLVANIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_DAILY_CRON?.trim() || "10 8 * * *",
    dailyTz: process.env.PENNSYLVANIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_DAILY_TZ?.trim() || "UTC",
  };
}

function defaultPennsylvaniaCampaignFinanceRawDataRefreshYear(now = new Date()): number {
  if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
    throw new Error("Invalid Pennsylvania campaign finance raw data refresh date");
  }
  return now.getUTCFullYear();
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  if (value !== undefined && (!Number.isInteger(value) || value <= 0)) {
    throw new Error(`Invalid Pennsylvania raw data refresh scheduler ${label}: ${value}`);
  }
}

function normalizeCacheDir(value: string | undefined): string {
  const normalized = value?.trim() || DEFAULT_PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_CACHE_DIR;
  if (!normalized) {
    throw new Error("Pennsylvania raw data refresh cacheDir is required");
  }
  return normalized;
}

function normalizeRefreshJobData(data: PennsylvaniaCampaignFinanceRawDataRefreshJobData): Required<
  Pick<PennsylvaniaCampaignFinanceRawDataRefreshJobData, "year" | "url" | "cacheDir" | "timeoutMs">
> {
  const year = normalizePennsylvaniaCampaignFinanceExportYear(
    data.year ?? defaultPennsylvaniaCampaignFinanceRawDataRefreshYear()
  );
  return {
    year,
    url: parsePennsylvaniaCampaignFinanceHttpsUrl(
      data.url?.trim() || buildPennsylvaniaCampaignFinanceExportUrl({ year }),
      "url"
    ),
    cacheDir: normalizeCacheDir(data.cacheDir),
    timeoutMs: data.timeoutMs ?? PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_FETCH_TIMEOUT_MS,
  };
}

function assertValidJobOptions(data: PennsylvaniaCampaignFinanceRawDataRefreshJobData): void {
  assertPositiveInteger(data.timeoutMs, "timeoutMs");
  normalizeRefreshJobData(data);
}

function toConnectionOptions(redisUrl: string): ConnectionOptions {
  const parsed = new URL(redisUrl);
  if (parsed.protocol !== "redis:" && parsed.protocol !== "rediss:") {
    throw new Error(`Unsupported REDIS_URL protocol: ${parsed.protocol}`);
  }
  const parsedPort = parsed.port ? Number.parseInt(parsed.port, 10) : 6379;
  const dbSegment = parsed.pathname.length > 1 ? parsed.pathname.slice(1) : "0";
  if (!/^\d+$/.test(dbSegment)) {
    throw new Error(`Invalid REDIS_URL db index: ${parsed.pathname}`);
  }
  const parsedDb = Number(dbSegment);

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

export function createPennsylvaniaCampaignFinanceRawDataRefreshQueue(): Queue<PennsylvaniaCampaignFinanceRawDataRefreshJobData> {
  return new Queue<PennsylvaniaCampaignFinanceRawDataRefreshJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringPennsylvaniaCampaignFinanceRawDataRefreshJobs(
  jobData: PennsylvaniaCampaignFinanceRawDataRefreshJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isPennsylvaniaCampaignFinanceEnabled()) {
    const queue = createPennsylvaniaCampaignFinanceRawDataRefreshQueue();
    try {
      await queue.removeJobScheduler(PENNSYLVANIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const normalized = normalizeRefreshJobData(jobData);
  const queue = createPennsylvaniaCampaignFinanceRawDataRefreshQueue();

  try {
    await queue.upsertJobScheduler(
      PENNSYLVANIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: PENNSYLVANIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_JOB_NAME,
        data: {
          year: normalized.year,
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

export async function enqueueManualPennsylvaniaCampaignFinanceRawDataRefreshJob(
  jobData: PennsylvaniaCampaignFinanceRawDataRefreshJobData = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isPennsylvaniaCampaignFinanceRawDataRefreshEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const normalized = normalizeRefreshJobData(jobData);
  const queue = createPennsylvaniaCampaignFinanceRawDataRefreshQueue();

  try {
    const job = await queue.add(
      PENNSYLVANIA_CAMPAIGN_FINANCE_RAW_DATA_REFRESH_JOB_NAME,
      {
        year: normalized.year,
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

export async function runPennsylvaniaCampaignFinanceRawDataRefreshJob(
  data: PennsylvaniaCampaignFinanceRawDataRefreshJobData = {}
): Promise<PennsylvaniaCampaignFinanceRawDataRefreshJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const enabled = isPennsylvaniaCampaignFinanceRawDataRefreshEnabled(force);

  if (!data.triggeredBy) {
    console.warn("Pennsylvania raw data refresh job missing triggeredBy; recording as unknown");
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
  const refresh = await refreshPennsylvaniaCampaignFinanceExportCache({
    year: normalized.year,
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

export function createPennsylvaniaCampaignFinanceRawDataRefreshWorker(): Worker<
  PennsylvaniaCampaignFinanceRawDataRefreshJobData,
  PennsylvaniaCampaignFinanceRawDataRefreshJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<
    PennsylvaniaCampaignFinanceRawDataRefreshJobData,
    PennsylvaniaCampaignFinanceRawDataRefreshJobResult
  > = async (job) => {
    return runPennsylvaniaCampaignFinanceRawDataRefreshJob(job.data ?? {});
  };

  return new Worker<PennsylvaniaCampaignFinanceRawDataRefreshJobData, PennsylvaniaCampaignFinanceRawDataRefreshJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
