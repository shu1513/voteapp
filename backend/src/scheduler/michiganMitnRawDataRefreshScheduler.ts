import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";

import { getPipelineEnv } from "../config/env.js";
import {
  isMichiganCampaignFinanceEnabled,
  isMichiganMitnRawDataRefreshEnabled,
} from "../config/featureFlags.js";
import {
  DEFAULT_MICHIGAN_MITN_LEGACY_ARCHIVE_CACHE_DIR,
  MICHIGAN_MITN_LEGACY_ARCHIVE_FETCH_TIMEOUT_MS,
  MICHIGAN_MITN_LEGACY_FINAL_ARCHIVE_YEAR,
  MICHIGAN_MITN_LEGACY_FIRST_ARCHIVE_YEAR,
  buildMichiganMitnLegacyArchiveUrl,
  normalizeMichiganMitnLegacyArchiveYear,
  parseMichiganMitnHttpsUrl,
  refreshMichiganMitnLegacyArchiveCache,
  type MichiganMitnLegacyArchiveRefreshResult,
} from "../pipeline/michiganFinance/michiganMitnLegacyArtifactCache.js";

export const MICHIGAN_MITN_RAW_DATA_REFRESH_JOB_NAME = "michigan_mitn_raw_data_refresh";
export const MICHIGAN_MITN_RAW_DATA_REFRESH_DAILY_SCHEDULER_ID =
  "michigan_mitn_raw_data_refresh_daily";

function clampToAvailableArchiveYear(year: number): number {
  return Math.min(
    Math.max(year, MICHIGAN_MITN_LEGACY_FIRST_ARCHIVE_YEAR),
    MICHIGAN_MITN_LEGACY_FINAL_ARCHIVE_YEAR
  );
}

export function defaultMichiganMitnRawDataRefreshYear(now = new Date()): number {
  return clampToAvailableArchiveYear(now.getUTCFullYear());
}

// The finance sync's cycle loader reads filing-year archives across
// [electionYear - 1, electionYear] (and the following year for completed
// legacy cycles), so an unpinned refresh must cover both current cycle years
// — clamped to the archives that exist upstream, because the legacy export is
// frozen at MICHIGAN_MITN_LEGACY_FINAL_ARCHIVE_YEAR. Resolved at RUN time
// (not bake time) so recurring jobs stay correct across year boundaries
// without re-upserting the scheduler.
function resolveRefreshYears(value: number | undefined, now = new Date()): number[] {
  if (value !== undefined) {
    return [normalizeMichiganMitnLegacyArchiveYear(value)];
  }
  const currentYear = now.getUTCFullYear();
  return [...new Set([clampToAvailableArchiveYear(currentYear - 1), clampToAvailableArchiveYear(currentYear)])];
}

export type MichiganMitnRawDataRefreshJobData = {
  year?: number;
  force?: boolean;
  url?: string;
  cacheDir?: string;
  timeoutMs?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type MichiganMitnRawDataRefreshArchiveOutcome = {
  year: number;
  status: MichiganMitnLegacyArchiveRefreshResult["status"];
  refresh: MichiganMitnLegacyArchiveRefreshResult;
};

export type MichiganMitnRawDataRefreshJobResult = {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<MichiganMitnRawDataRefreshJobData["triggeredBy"]>;
  years: number[];
  status: "disabled" | MichiganMitnLegacyArchiveRefreshResult["status"];
  refreshes: MichiganMitnRawDataRefreshArchiveOutcome[];
};

type RawDataRefreshSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

type NormalizedRefreshJobData = {
  years: number[];
  url?: string;
  cacheDir: string;
  timeoutMs: number;
};

function readSchedulerRuntimeConfig(): RawDataRefreshSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.MICHIGAN_MITN_RAW_DATA_REFRESH_QUEUE?.trim() ||
      "michigan_mitn_raw_data_refresh_maintenance",
    dailyCron: process.env.MICHIGAN_MITN_RAW_DATA_REFRESH_DAILY_CRON?.trim() || "40 8 * * *",
    dailyTz: process.env.MICHIGAN_MITN_RAW_DATA_REFRESH_DAILY_TZ?.trim() || "UTC",
  };
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  if (value !== undefined && (!Number.isInteger(value) || value <= 0)) {
    throw new Error(`Invalid Michigan MiTN raw data refresh scheduler ${label}: ${value}`);
  }
}

function normalizeRefreshJobData(data: MichiganMitnRawDataRefreshJobData): NormalizedRefreshJobData {
  const url = data.url?.trim();
  if (url && data.year === undefined) {
    // A URL points at exactly one filing year's archive, so an unpinned
    // multi-year refresh cannot honor it.
    throw new Error("Michigan MiTN raw data refresh url requires an explicit year");
  }
  return {
    years: resolveRefreshYears(data.year),
    url: url ? parseMichiganMitnHttpsUrl(url, "url") : undefined,
    cacheDir: data.cacheDir?.trim() || DEFAULT_MICHIGAN_MITN_LEGACY_ARCHIVE_CACHE_DIR,
    timeoutMs: data.timeoutMs ?? MICHIGAN_MITN_LEGACY_ARCHIVE_FETCH_TIMEOUT_MS,
  };
}

function assertValidJobOptions(data: MichiganMitnRawDataRefreshJobData): void {
  assertPositiveInteger(data.year, "year");
  assertPositiveInteger(data.timeoutMs, "timeoutMs");
  normalizeRefreshJobData(data);
}

// Bake the year (and url) only when explicitly pinned; otherwise each run
// resolves the current cycle years at run time, so recurring jobs keep
// covering the right archives across year boundaries without re-upserting
// the scheduler.
function bakedYearAndUrlFields(
  jobData: MichiganMitnRawDataRefreshJobData,
  normalized: NormalizedRefreshJobData
): Pick<MichiganMitnRawDataRefreshJobData, "year" | "url"> {
  if (jobData.year === undefined) {
    return {};
  }
  return {
    year: normalizeMichiganMitnLegacyArchiveYear(jobData.year),
    ...(normalized.url ? { url: normalized.url } : {}),
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

export function createMichiganMitnRawDataRefreshQueue(): Queue<MichiganMitnRawDataRefreshJobData> {
  return new Queue<MichiganMitnRawDataRefreshJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringMichiganMitnRawDataRefreshJobs(
  jobData: MichiganMitnRawDataRefreshJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  const queue = createMichiganMitnRawDataRefreshQueue();

  try {
    if (!isMichiganCampaignFinanceEnabled()) {
      await queue.removeJobScheduler(MICHIGAN_MITN_RAW_DATA_REFRESH_DAILY_SCHEDULER_ID);
      return;
    }

    const config = readSchedulerRuntimeConfig();
    const normalized = normalizeRefreshJobData(jobData);
    await queue.upsertJobScheduler(
      MICHIGAN_MITN_RAW_DATA_REFRESH_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: MICHIGAN_MITN_RAW_DATA_REFRESH_JOB_NAME,
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

export async function enqueueManualMichiganMitnRawDataRefreshJob(
  jobData: MichiganMitnRawDataRefreshJobData = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isMichiganMitnRawDataRefreshEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const normalized = normalizeRefreshJobData(jobData);
  const queue = createMichiganMitnRawDataRefreshQueue();

  try {
    const job = await queue.add(
      MICHIGAN_MITN_RAW_DATA_REFRESH_JOB_NAME,
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

export async function runMichiganMitnRawDataRefreshJob(
  data: MichiganMitnRawDataRefreshJobData = {}
): Promise<MichiganMitnRawDataRefreshJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const enabled = isMichiganMitnRawDataRefreshEnabled(force);
  const normalized = normalizeRefreshJobData(data);

  if (!data.triggeredBy) {
    console.warn("Michigan MiTN raw data refresh job missing triggeredBy; recording as unknown");
  }

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

  const refreshes: MichiganMitnRawDataRefreshArchiveOutcome[] = [];
  for (const year of normalized.years) {
    const refresh = await refreshMichiganMitnLegacyArchiveCache({
      year,
      cacheDir: normalized.cacheDir,
      url: normalized.url ?? buildMichiganMitnLegacyArchiveUrl({ year }),
      force,
      timeoutMs: normalized.timeoutMs,
    });
    refreshes.push({ year, status: refresh.status, refresh });
  }

  const aggregateStatus = refreshes.some((outcome) => outcome.status === "downloaded")
    ? "downloaded"
    : refreshes.some((outcome) => outcome.status === "extracted")
      ? "extracted"
      : "unchanged";
  return {
    enabled: true,
    force,
    triggeredBy,
    years: normalized.years,
    status: aggregateStatus,
    refreshes,
  };
}

export function createMichiganMitnRawDataRefreshWorker(): Worker<
  MichiganMitnRawDataRefreshJobData,
  MichiganMitnRawDataRefreshJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<MichiganMitnRawDataRefreshJobData, MichiganMitnRawDataRefreshJobResult> = async (job) => {
    return runMichiganMitnRawDataRefreshJob(job.data ?? {});
  };

  return new Worker<MichiganMitnRawDataRefreshJobData, MichiganMitnRawDataRefreshJobResult>(queueName, processor, {
    connection,
    concurrency: 1,
  });
}
