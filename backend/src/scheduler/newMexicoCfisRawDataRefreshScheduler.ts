import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";

import { getPipelineEnv } from "../config/env.js";
import {
  isNewMexicoCampaignFinanceEnabled,
  isNewMexicoCfisRawDataRefreshEnabled,
} from "../config/featureFlags.js";
import {
  DEFAULT_NEW_MEXICO_CFIS_CACHE_DIR,
  NEW_MEXICO_CFIS_FETCH_TIMEOUT_MS,
  buildNewMexicoCfisArtifactUrl,
  parseNewMexicoCfisHttpsUrl,
  refreshNewMexicoCfisArtifactCache,
  type NewMexicoCfisArtifactKind,
  type NewMexicoCfisArtifactRefreshResult,
} from "../pipeline/newMexicoFinance/newMexicoCfisArtifactCache.js";

export const NEW_MEXICO_CFIS_RAW_DATA_REFRESH_JOB_NAME = "new_mexico_cfis_raw_data_refresh";
export const NEW_MEXICO_CFIS_RAW_DATA_REFRESH_CONTRIBUTIONS_SCHEDULER_ID =
  "new_mexico_cfis_raw_data_refresh_contributions_daily";
export const NEW_MEXICO_CFIS_RAW_DATA_REFRESH_EXPENDITURES_SCHEDULER_ID =
  "new_mexico_cfis_raw_data_refresh_expenditures_daily";

export type NewMexicoCfisRawDataRefreshJobData = {
  force?: boolean;
  year?: number;
  artifactKind?: NewMexicoCfisArtifactKind;
  url?: string;
  cacheDir?: string;
  timeoutMs?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type NewMexicoCfisRawDataRefreshJobResult = {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<NewMexicoCfisRawDataRefreshJobData["triggeredBy"]>;
  year: number;
  artifactKind: NewMexicoCfisArtifactKind;
  status: "disabled" | NewMexicoCfisArtifactRefreshResult["status"];
  refresh: NewMexicoCfisArtifactRefreshResult | null;
};

type RawDataRefreshSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

type NormalizedRefreshJobData = Required<
  Pick<NewMexicoCfisRawDataRefreshJobData, "year" | "artifactKind" | "url" | "cacheDir" | "timeoutMs">
>;

function readSchedulerRuntimeConfig(): RawDataRefreshSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.NEW_MEXICO_CFIS_RAW_DATA_REFRESH_QUEUE?.trim() ||
      "new_mexico_cfis_raw_data_refresh_maintenance",
    dailyCron: process.env.NEW_MEXICO_CFIS_RAW_DATA_REFRESH_DAILY_CRON?.trim() || "20 8 * * *",
    dailyTz: process.env.NEW_MEXICO_CFIS_RAW_DATA_REFRESH_DAILY_TZ?.trim() || "UTC",
  };
}

function defaultRefreshYear(): number {
  return new Date().getUTCFullYear();
}

function normalizeYear(value: number | undefined): number {
  const year = value ?? defaultRefreshYear();
  if (!Number.isInteger(year) || year < 2020 || year > 2100) {
    throw new Error(`Invalid New Mexico CFIS raw data refresh year: ${value}`);
  }
  return year;
}

function normalizeArtifactKind(value: NewMexicoCfisRawDataRefreshJobData["artifactKind"]): NewMexicoCfisArtifactKind {
  return value ?? "contributions";
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  if (value !== undefined && (!Number.isInteger(value) || value <= 0)) {
    throw new Error(`Invalid New Mexico CFIS raw data refresh scheduler ${label}: ${value}`);
  }
}

function normalizeRefreshJobData(data: NewMexicoCfisRawDataRefreshJobData): NormalizedRefreshJobData {
  const year = normalizeYear(data.year);
  const artifactKind = normalizeArtifactKind(data.artifactKind);
  const defaultUrl = buildNewMexicoCfisArtifactUrl({ year, artifactKind });

  return {
    year,
    artifactKind,
    url: parseNewMexicoCfisHttpsUrl(data.url?.trim() || defaultUrl, "url"),
    cacheDir: data.cacheDir?.trim() || DEFAULT_NEW_MEXICO_CFIS_CACHE_DIR,
    timeoutMs: data.timeoutMs ?? NEW_MEXICO_CFIS_FETCH_TIMEOUT_MS,
  };
}

function assertValidJobOptions(data: NewMexicoCfisRawDataRefreshJobData): void {
  assertPositiveInteger(data.year, "year");
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

export function createNewMexicoCfisRawDataRefreshQueue(): Queue<NewMexicoCfisRawDataRefreshJobData> {
  return new Queue<NewMexicoCfisRawDataRefreshJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringNewMexicoCfisRawDataRefreshJobs(
  jobData: NewMexicoCfisRawDataRefreshJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  const queue = createNewMexicoCfisRawDataRefreshQueue();
  try {
    if (!isNewMexicoCampaignFinanceEnabled()) {
      await queue.removeJobScheduler(NEW_MEXICO_CFIS_RAW_DATA_REFRESH_CONTRIBUTIONS_SCHEDULER_ID);
      await queue.removeJobScheduler(NEW_MEXICO_CFIS_RAW_DATA_REFRESH_EXPENDITURES_SCHEDULER_ID);
      return;
    }

    const config = readSchedulerRuntimeConfig();
    const entries: Array<{ schedulerId: string; normalized: NormalizedRefreshJobData }> = jobData.artifactKind
      ? [
          {
            schedulerId:
              jobData.artifactKind === "contributions"
                ? NEW_MEXICO_CFIS_RAW_DATA_REFRESH_CONTRIBUTIONS_SCHEDULER_ID
                : NEW_MEXICO_CFIS_RAW_DATA_REFRESH_EXPENDITURES_SCHEDULER_ID,
            normalized: normalizeRefreshJobData(jobData),
          },
        ]
      : [
          {
            schedulerId: NEW_MEXICO_CFIS_RAW_DATA_REFRESH_CONTRIBUTIONS_SCHEDULER_ID,
            normalized: normalizeRefreshJobData({ ...jobData, url: undefined, artifactKind: "contributions" }),
          },
          {
            schedulerId: NEW_MEXICO_CFIS_RAW_DATA_REFRESH_EXPENDITURES_SCHEDULER_ID,
            normalized: normalizeRefreshJobData({ ...jobData, url: undefined, artifactKind: "expenditures" }),
          },
        ];

    for (const entry of entries) {
      await queue.upsertJobScheduler(
        entry.schedulerId,
        {
          pattern: config.dailyCron,
          tz: config.dailyTz,
        },
        {
          name: NEW_MEXICO_CFIS_RAW_DATA_REFRESH_JOB_NAME,
          data: {
            force: Boolean(jobData.force),
            year: entry.normalized.year,
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

export async function enqueueManualNewMexicoCfisRawDataRefreshJob(
  jobData: NewMexicoCfisRawDataRefreshJobData = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isNewMexicoCfisRawDataRefreshEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const normalized = normalizeRefreshJobData(jobData);
  const queue = createNewMexicoCfisRawDataRefreshQueue();
  try {
    const job = await queue.add(
      NEW_MEXICO_CFIS_RAW_DATA_REFRESH_JOB_NAME,
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

export async function runNewMexicoCfisRawDataRefreshJob(
  data: NewMexicoCfisRawDataRefreshJobData = {}
): Promise<NewMexicoCfisRawDataRefreshJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const enabled = isNewMexicoCfisRawDataRefreshEnabled(force);
  const normalized = normalizeRefreshJobData(data);

  if (!data.triggeredBy) {
    console.warn("New Mexico CFIS raw data refresh job missing triggeredBy; recording as unknown");
  }

  if (!enabled) {
    return {
      enabled: false,
      force,
      triggeredBy,
      year: normalized.year,
      artifactKind: normalized.artifactKind,
      status: "disabled",
      refresh: null,
    };
  }

  const refresh = await refreshNewMexicoCfisArtifactCache({
    cacheDir: normalized.cacheDir,
    year: normalized.year,
    artifactKind: normalized.artifactKind,
    url: normalized.url,
    force,
    timeoutMs: normalized.timeoutMs,
  });

  return {
    enabled: true,
    force,
    triggeredBy,
    year: normalized.year,
    artifactKind: normalized.artifactKind,
    status: refresh.status,
    refresh,
  };
}

export function createNewMexicoCfisRawDataRefreshWorker(): Worker<
  NewMexicoCfisRawDataRefreshJobData,
  NewMexicoCfisRawDataRefreshJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<NewMexicoCfisRawDataRefreshJobData, NewMexicoCfisRawDataRefreshJobResult> = async (job) => {
    return runNewMexicoCfisRawDataRefreshJob(job.data ?? {});
  };

  return new Worker<NewMexicoCfisRawDataRefreshJobData, NewMexicoCfisRawDataRefreshJobResult>(queueName, processor, {
    connection,
    concurrency: 1,
  });
}
