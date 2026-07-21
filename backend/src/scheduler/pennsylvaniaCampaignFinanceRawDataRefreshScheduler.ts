import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";

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

export type PennsylvaniaCampaignFinanceRawDataRefreshOutcome = {
  year: number;
  status: PennsylvaniaCampaignFinanceExportRefreshResult["status"];
  refresh: PennsylvaniaCampaignFinanceExportRefreshResult;
};

export type PennsylvaniaCampaignFinanceRawDataRefreshJobResult = {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<PennsylvaniaCampaignFinanceRawDataRefreshJobData["triggeredBy"]>;
  years: number[];
  status: "disabled" | PennsylvaniaCampaignFinanceExportRefreshResult["status"];
  refreshes: PennsylvaniaCampaignFinanceRawDataRefreshOutcome[];
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

// Pennsylvania's batch sync loads one export ZIP per cycle year
// ({electionYear - 1, electionYear}), so an unpinned refresh must cover both.
// Years are resolved at RUN time (not bake time) so recurring jobs stay
// correct across year boundaries without re-upserting the scheduler.
function resolveRefreshYears(value: number | undefined, now = new Date()): number[] {
  if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
    throw new Error("Invalid Pennsylvania campaign finance raw data refresh date");
  }
  if (value !== undefined) {
    return [normalizePennsylvaniaCampaignFinanceExportYear(value)];
  }
  const currentYear = now.getUTCFullYear();
  return [currentYear - 1, currentYear];
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

type NormalizedRefreshJobData = {
  years: number[];
  urlOverride: string | null;
  cacheDir: string;
  timeoutMs: number;
};

function normalizeRefreshJobData(data: PennsylvaniaCampaignFinanceRawDataRefreshJobData): NormalizedRefreshJobData {
  // The export URL embeds the year, so an explicit url only makes sense for a
  // single explicitly pinned year — never for a resolved multi-year cycle.
  const urlOverride = data.url?.trim() || null;
  if (urlOverride && data.year === undefined) {
    throw new Error("Pennsylvania raw data refresh url requires an explicit year");
  }
  return {
    years: resolveRefreshYears(data.year),
    urlOverride: urlOverride ? parsePennsylvaniaCampaignFinanceHttpsUrl(urlOverride, "url") : null,
    cacheDir: normalizeCacheDir(data.cacheDir),
    timeoutMs: data.timeoutMs ?? PENNSYLVANIA_CAMPAIGN_FINANCE_EXPORT_FETCH_TIMEOUT_MS,
  };
}

function refreshUrlForYear(input: { year: number; urlOverride: string | null }): string {
  return input.urlOverride ?? buildPennsylvaniaCampaignFinanceExportUrl({ year: input.year });
}

// Bake year/url into job data only when explicitly pinned; otherwise each run
// resolves both cycle years (and their per-year URLs) at run time.
function bakedYearAndUrlFields(
  jobData: PennsylvaniaCampaignFinanceRawDataRefreshJobData,
  normalized: NormalizedRefreshJobData
): Partial<Pick<PennsylvaniaCampaignFinanceRawDataRefreshJobData, "year" | "url">> {
  return {
    ...(jobData.year === undefined ? {} : { year: normalized.years[0] }),
    ...(normalized.urlOverride ? { url: normalized.urlOverride } : {}),
  };
}

function assertValidJobOptions(data: PennsylvaniaCampaignFinanceRawDataRefreshJobData): void {
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
          ...bakedYearAndUrlFields(jobData, normalized),
          force: Boolean(jobData.force),
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
        ...bakedYearAndUrlFields(jobData, normalized),
        force: Boolean(jobData.force),
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

  const normalized = normalizeRefreshJobData(data);

  if (!enabled) {
    return {
      enabled: false,
      force,
      triggeredBy,
      years: normalized.years,
      status: "disabled",
      refreshes: [],
    };
  }

  const refreshes: PennsylvaniaCampaignFinanceRawDataRefreshOutcome[] = [];
  for (const year of normalized.years) {
    const refresh = await refreshPennsylvaniaCampaignFinanceExportCache({
      year,
      cacheDir: normalized.cacheDir,
      url: refreshUrlForYear({ year, urlOverride: normalized.urlOverride }),
      force,
      timeoutMs: normalized.timeoutMs,
    });
    refreshes.push({ year, status: refresh.status, refresh });
  }

  const status = refreshes.some((outcome) => outcome.status === "downloaded")
    ? "downloaded"
    : refreshes.some((outcome) => outcome.status === "extracted")
      ? "extracted"
      : "unchanged";

  return {
    enabled: true,
    force,
    triggeredBy,
    years: normalized.years,
    status,
    refreshes,
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
