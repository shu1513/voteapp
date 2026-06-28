import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";

import { getPipelineEnv } from "../config/env.js";
import { isMaineCampaignFinanceEnabled, isMaineCfisRawDataRefreshEnabled } from "../config/featureFlags.js";
import {
  DEFAULT_MAINE_CFIS_CACHE_DIR,
  refreshMaineCfisArtifactCache,
  type MaineCfisArtifactRefreshResult,
} from "../pipeline/maineFinance/maineCfisArtifactCache.js";
import {
  MAINE_CFIS_CSV_DOWNLOAD_API_URL,
  MAINE_CFIS_FETCH_TIMEOUT_MS,
  normalizeMaineCfisArtifactKind,
  normalizeMaineCfisFilingYear,
  parseMaineCfisHttpsUrl,
  type MaineCfisArtifactKind,
} from "../pipeline/maineFinance/maineCfisClient.js";

export const MAINE_CFIS_RAW_DATA_REFRESH_JOB_NAME = "maine_cfis_raw_data_refresh";
export const MAINE_CFIS_RAW_DATA_REFRESH_CONTRIBUTIONS_SCHEDULER_ID =
  "maine_cfis_raw_data_refresh_contributions_daily";
export const MAINE_CFIS_RAW_DATA_REFRESH_EXPENDITURES_SCHEDULER_ID =
  "maine_cfis_raw_data_refresh_expenditures_daily";

export type MaineCfisRawDataRefreshJobData = {
  force?: boolean;
  filingYear?: number;
  artifactKind?: MaineCfisArtifactKind;
  url?: string;
  cacheDir?: string;
  timeoutMs?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type MaineCfisRawDataRefreshJobResult = {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<MaineCfisRawDataRefreshJobData["triggeredBy"]>;
  filingYear: number;
  artifactKind: MaineCfisArtifactKind;
  status: "disabled" | MaineCfisArtifactRefreshResult["status"];
  refresh: MaineCfisArtifactRefreshResult | null;
};

type MaineCfisRawDataRefreshSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

type NormalizedRefreshJobData = Required<
  Pick<MaineCfisRawDataRefreshJobData, "filingYear" | "artifactKind" | "url" | "cacheDir" | "timeoutMs">
>;

function readSchedulerRuntimeConfig(): MaineCfisRawDataRefreshSchedulerRuntimeConfig {
  return {
    queueName: process.env.MAINE_CFIS_RAW_DATA_REFRESH_QUEUE?.trim() || "maine_cfis_raw_data_refresh_maintenance",
    dailyCron: process.env.MAINE_CFIS_RAW_DATA_REFRESH_DAILY_CRON?.trim() || "25 8 * * *",
    dailyTz: process.env.MAINE_CFIS_RAW_DATA_REFRESH_DAILY_TZ?.trim() || "UTC",
  };
}

function defaultRefreshFilingYear(): number {
  return new Date().getUTCFullYear();
}

function normalizeFilingYear(value: number | undefined): number {
  return normalizeMaineCfisFilingYear(value ?? defaultRefreshFilingYear());
}

function normalizeArtifactKind(value: MaineCfisRawDataRefreshJobData["artifactKind"]): MaineCfisArtifactKind {
  return normalizeMaineCfisArtifactKind(value ?? "contributions");
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  if (value !== undefined && (!Number.isInteger(value) || value <= 0)) {
    throw new Error(`Invalid Maine CFIS raw data refresh scheduler ${label}: ${value}`);
  }
}

function normalizeRefreshJobData(data: MaineCfisRawDataRefreshJobData): NormalizedRefreshJobData {
  return {
    filingYear: normalizeFilingYear(data.filingYear),
    artifactKind: normalizeArtifactKind(data.artifactKind),
    url: parseMaineCfisHttpsUrl(data.url?.trim() || MAINE_CFIS_CSV_DOWNLOAD_API_URL, "url"),
    cacheDir: data.cacheDir?.trim() || DEFAULT_MAINE_CFIS_CACHE_DIR,
    timeoutMs: data.timeoutMs ?? MAINE_CFIS_FETCH_TIMEOUT_MS,
  };
}

function assertValidJobOptions(data: MaineCfisRawDataRefreshJobData): void {
  assertPositiveInteger(data.filingYear, "filingYear");
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
  return toConnectionOptions(getPipelineEnv().REDIS_URL);
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

export function createMaineCfisRawDataRefreshQueue(): Queue<MaineCfisRawDataRefreshJobData> {
  return new Queue<MaineCfisRawDataRefreshJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringMaineCfisRawDataRefreshJobs(
  jobData: MaineCfisRawDataRefreshJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  const queue = createMaineCfisRawDataRefreshQueue();
  try {
    if (!isMaineCampaignFinanceEnabled()) {
      await queue.removeJobScheduler(MAINE_CFIS_RAW_DATA_REFRESH_CONTRIBUTIONS_SCHEDULER_ID);
      await queue.removeJobScheduler(MAINE_CFIS_RAW_DATA_REFRESH_EXPENDITURES_SCHEDULER_ID);
      return;
    }

    const config = readSchedulerRuntimeConfig();
    const entries: Array<{ schedulerId: string; normalized: NormalizedRefreshJobData }> = jobData.artifactKind
      ? [
          {
            schedulerId:
              jobData.artifactKind === "contributions"
                ? MAINE_CFIS_RAW_DATA_REFRESH_CONTRIBUTIONS_SCHEDULER_ID
                : MAINE_CFIS_RAW_DATA_REFRESH_EXPENDITURES_SCHEDULER_ID,
            normalized: normalizeRefreshJobData(jobData),
          },
        ]
      : [
          {
            schedulerId: MAINE_CFIS_RAW_DATA_REFRESH_CONTRIBUTIONS_SCHEDULER_ID,
            normalized: normalizeRefreshJobData({ ...jobData, url: undefined, artifactKind: "contributions" }),
          },
          {
            schedulerId: MAINE_CFIS_RAW_DATA_REFRESH_EXPENDITURES_SCHEDULER_ID,
            normalized: normalizeRefreshJobData({ ...jobData, url: undefined, artifactKind: "expenditures" }),
          },
        ];

    const configuredIds = new Set(entries.map((entry) => entry.schedulerId));
    for (const schedulerId of [
      MAINE_CFIS_RAW_DATA_REFRESH_CONTRIBUTIONS_SCHEDULER_ID,
      MAINE_CFIS_RAW_DATA_REFRESH_EXPENDITURES_SCHEDULER_ID,
    ]) {
      if (!configuredIds.has(schedulerId)) {
        await queue.removeJobScheduler(schedulerId);
      }
    }

    for (const entry of entries) {
      await queue.upsertJobScheduler(
        entry.schedulerId,
        {
          pattern: config.dailyCron,
          tz: config.dailyTz,
        },
        {
          name: MAINE_CFIS_RAW_DATA_REFRESH_JOB_NAME,
          data: {
            force: Boolean(jobData.force),
            filingYear: entry.normalized.filingYear,
            artifactKind: entry.normalized.artifactKind,
            url: entry.normalized.url,
            cacheDir: entry.normalized.cacheDir,
            timeoutMs: entry.normalized.timeoutMs,
            triggeredBy: "daily",
          },
          opts: defaultJobOptions(),
        }
      );
    }
  } finally {
    await queue.close();
  }
}

export async function enqueueManualMaineCfisRawDataRefreshJob(
  jobData: MaineCfisRawDataRefreshJobData = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isMaineCfisRawDataRefreshEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const normalized = normalizeRefreshJobData(jobData);
  const queue = createMaineCfisRawDataRefreshQueue();
  try {
    const job = await queue.add(
      MAINE_CFIS_RAW_DATA_REFRESH_JOB_NAME,
      {
        force: Boolean(jobData.force),
        filingYear: normalized.filingYear,
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

export async function runMaineCfisRawDataRefreshJob(
  data: MaineCfisRawDataRefreshJobData = {}
): Promise<MaineCfisRawDataRefreshJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const enabled = isMaineCfisRawDataRefreshEnabled(force);
  const normalized = normalizeRefreshJobData(data);

  if (!data.triggeredBy) {
    console.warn("Maine CFIS raw data refresh job missing triggeredBy; recording as unknown");
  }

  if (!enabled) {
    return {
      enabled: false,
      force,
      triggeredBy,
      filingYear: normalized.filingYear,
      artifactKind: normalized.artifactKind,
      status: "disabled",
      refresh: null,
    };
  }

  const refresh = await refreshMaineCfisArtifactCache({
    filingYear: normalized.filingYear,
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
    filingYear: normalized.filingYear,
    artifactKind: normalized.artifactKind,
    status: refresh.status,
    refresh,
  };
}

export function createMaineCfisRawDataRefreshWorker(): Worker<
  MaineCfisRawDataRefreshJobData,
  MaineCfisRawDataRefreshJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<MaineCfisRawDataRefreshJobData, MaineCfisRawDataRefreshJobResult> = async (job) => {
    return runMaineCfisRawDataRefreshJob(job.data ?? {});
  };

  return new Worker<MaineCfisRawDataRefreshJobData, MaineCfisRawDataRefreshJobResult>(queueName, processor, {
    connection,
    concurrency: 1,
  });
}
