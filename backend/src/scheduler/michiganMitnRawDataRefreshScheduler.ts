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
  buildMichiganMitnLegacyArchiveUrl,
  normalizeMichiganMitnLegacyArchiveYear,
  parseMichiganMitnHttpsUrl,
  refreshMichiganMitnLegacyArchiveCache,
  type MichiganMitnLegacyArchiveRefreshResult,
} from "../pipeline/michiganFinance/michiganMitnLegacyArtifactCache.js";

export const MICHIGAN_MITN_RAW_DATA_REFRESH_JOB_NAME = "michigan_mitn_raw_data_refresh";
export const MICHIGAN_MITN_RAW_DATA_REFRESH_DAILY_SCHEDULER_ID =
  "michigan_mitn_raw_data_refresh_daily";

export function defaultMichiganMitnRawDataRefreshYear(now = new Date()): number {
  return now.getUTCFullYear();
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

export type MichiganMitnRawDataRefreshJobResult = {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<MichiganMitnRawDataRefreshJobData["triggeredBy"]>;
  status: "disabled" | MichiganMitnLegacyArchiveRefreshResult["status"];
  refresh: MichiganMitnLegacyArchiveRefreshResult | null;
};

type RawDataRefreshSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

type NormalizedRefreshJobData = Required<
  Pick<MichiganMitnRawDataRefreshJobData, "year" | "url" | "cacheDir" | "timeoutMs">
>;

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
  const year = normalizeMichiganMitnLegacyArchiveYear(
    data.year ?? defaultMichiganMitnRawDataRefreshYear()
  );
  return {
    year,
    url: parseMichiganMitnHttpsUrl(data.url?.trim() || buildMichiganMitnLegacyArchiveUrl({ year }), "url"),
    cacheDir: data.cacheDir?.trim() || DEFAULT_MICHIGAN_MITN_LEGACY_ARCHIVE_CACHE_DIR,
    timeoutMs: data.timeoutMs ?? MICHIGAN_MITN_LEGACY_ARCHIVE_FETCH_TIMEOUT_MS,
  };
}

function assertValidJobOptions(data: MichiganMitnRawDataRefreshJobData): void {
  assertPositiveInteger(data.year, "year");
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

export async function runMichiganMitnRawDataRefreshJob(
  data: MichiganMitnRawDataRefreshJobData = {}
): Promise<MichiganMitnRawDataRefreshJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const enabled = isMichiganMitnRawDataRefreshEnabled(force);

  if (!data.triggeredBy) {
    console.warn("Michigan MiTN raw data refresh job missing triggeredBy; recording as unknown");
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
  const refresh = await refreshMichiganMitnLegacyArchiveCache({
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
