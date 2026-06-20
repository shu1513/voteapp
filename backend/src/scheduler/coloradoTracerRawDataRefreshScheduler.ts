import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";

import { getPipelineEnv } from "../config/env.js";
import {
  isColoradoCampaignFinanceEnabled,
  isColoradoTracerRawDataRefreshEnabled,
} from "../config/featureFlags.js";
import {
  COLORADO_TRACER_CONTRIBUTION_FETCH_TIMEOUT_MS,
  DEFAULT_COLORADO_TRACER_CONTRIBUTION_CACHE_DIR,
  buildColoradoTracerContributionZipUrl,
  parseColoradoTracerHttpsUrl,
  refreshColoradoTracerContributionArtifactCache,
  type ColoradoTracerContributionArtifactRefreshResult,
} from "../pipeline/coloradoFinance/coloradoTracerContributionArtifactCache.js";

export const COLORADO_TRACER_RAW_DATA_REFRESH_JOB_NAME = "colorado_tracer_raw_data_refresh";
export const COLORADO_TRACER_RAW_DATA_REFRESH_DAILY_SCHEDULER_ID =
  "colorado_tracer_raw_data_refresh_daily";

export type ColoradoTracerRawDataRefreshJobData = {
  force?: boolean;
  year?: number;
  url?: string;
  cacheDir?: string;
  timeoutMs?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type ColoradoTracerRawDataRefreshJobResult = {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<ColoradoTracerRawDataRefreshJobData["triggeredBy"]>;
  year: number;
  status: "disabled" | ColoradoTracerContributionArtifactRefreshResult["status"];
  refresh: ColoradoTracerContributionArtifactRefreshResult | null;
};

type RawDataRefreshSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

function readSchedulerRuntimeConfig(): RawDataRefreshSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.COLORADO_TRACER_RAW_DATA_REFRESH_QUEUE?.trim() ||
      "colorado_tracer_raw_data_refresh_maintenance",
    dailyCron: process.env.COLORADO_TRACER_RAW_DATA_REFRESH_DAILY_CRON?.trim() || "5 8 * * *",
    dailyTz: process.env.COLORADO_TRACER_RAW_DATA_REFRESH_DAILY_TZ?.trim() || "UTC",
  };
}

function defaultRefreshYear(): number {
  return new Date().getUTCFullYear();
}

function normalizeYear(value: number | undefined): number {
  const year = value ?? defaultRefreshYear();
  if (!Number.isInteger(year) || year < 2001 || year > 2100) {
    throw new Error(`Invalid Colorado TRACER raw data refresh year: ${value}`);
  }
  return year;
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  if (value !== undefined && (!Number.isInteger(value) || value <= 0)) {
    throw new Error(`Invalid Colorado TRACER raw data refresh scheduler ${label}: ${value}`);
  }
}

function normalizeRefreshJobData(data: ColoradoTracerRawDataRefreshJobData): Required<
  Pick<ColoradoTracerRawDataRefreshJobData, "year" | "url" | "cacheDir" | "timeoutMs">
> {
  const year = normalizeYear(data.year);
  return {
    year,
    url: parseColoradoTracerHttpsUrl(
      data.url?.trim() || buildColoradoTracerContributionZipUrl({ year }),
      "url"
    ),
    cacheDir: data.cacheDir?.trim() || DEFAULT_COLORADO_TRACER_CONTRIBUTION_CACHE_DIR,
    timeoutMs: data.timeoutMs ?? COLORADO_TRACER_CONTRIBUTION_FETCH_TIMEOUT_MS,
  };
}

function assertValidJobOptions(data: ColoradoTracerRawDataRefreshJobData): void {
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

export function createColoradoTracerRawDataRefreshQueue(): Queue<ColoradoTracerRawDataRefreshJobData> {
  return new Queue<ColoradoTracerRawDataRefreshJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringColoradoTracerRawDataRefreshJobs(
  jobData: ColoradoTracerRawDataRefreshJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isColoradoCampaignFinanceEnabled()) {
    const queue = createColoradoTracerRawDataRefreshQueue();
    try {
      await queue.removeJobScheduler(COLORADO_TRACER_RAW_DATA_REFRESH_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const normalized = normalizeRefreshJobData(jobData);
  const queue = createColoradoTracerRawDataRefreshQueue();

  try {
    await queue.upsertJobScheduler(
      COLORADO_TRACER_RAW_DATA_REFRESH_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: COLORADO_TRACER_RAW_DATA_REFRESH_JOB_NAME,
        data: {
          force: Boolean(jobData.force),
          year: normalized.year,
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

export async function enqueueManualColoradoTracerRawDataRefreshJob(
  jobData: ColoradoTracerRawDataRefreshJobData = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isColoradoTracerRawDataRefreshEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const normalized = normalizeRefreshJobData(jobData);
  const queue = createColoradoTracerRawDataRefreshQueue();

  try {
    const job = await queue.add(
      COLORADO_TRACER_RAW_DATA_REFRESH_JOB_NAME,
      {
        force: Boolean(jobData.force),
        year: normalized.year,
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

export async function runColoradoTracerRawDataRefreshJob(
  data: ColoradoTracerRawDataRefreshJobData = {}
): Promise<ColoradoTracerRawDataRefreshJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const enabled = isColoradoTracerRawDataRefreshEnabled(force);
  const normalized = normalizeRefreshJobData(data);

  if (!data.triggeredBy) {
    console.warn("Colorado TRACER raw data refresh job missing triggeredBy; recording as unknown");
  }

  if (!enabled) {
    return {
      enabled: false,
      force,
      triggeredBy,
      year: normalized.year,
      status: "disabled",
      refresh: null,
    };
  }

  const refresh = await refreshColoradoTracerContributionArtifactCache({
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
    year: normalized.year,
    status: refresh.status,
    refresh,
  };
}

export function createColoradoTracerRawDataRefreshWorker(): Worker<
  ColoradoTracerRawDataRefreshJobData,
  ColoradoTracerRawDataRefreshJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<
    ColoradoTracerRawDataRefreshJobData,
    ColoradoTracerRawDataRefreshJobResult
  > = async (job) => {
    return runColoradoTracerRawDataRefreshJob(job.data ?? {});
  };

  return new Worker<ColoradoTracerRawDataRefreshJobData, ColoradoTracerRawDataRefreshJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
