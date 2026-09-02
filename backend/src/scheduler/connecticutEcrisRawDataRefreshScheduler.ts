import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";

import { getPipelineEnv } from "../config/env.js";
import {
  isConnecticutCampaignFinanceEnabled,
  isConnecticutEcrisRawDataRefreshEnabled,
} from "../config/featureFlags.js";
import {
  CONNECTICUT_ECRIS_FETCH_TIMEOUT_MS,
  DEFAULT_CONNECTICUT_ECRIS_CACHE_DIR,
  buildConnecticutEcrisArtifactUrl,
  parseConnecticutEcrisHttpsUrl,
  refreshConnecticutEcrisArtifactCache,
  type ConnecticutEcrisArtifactCommitteeType,
  type ConnecticutEcrisArtifactFormat,
  type ConnecticutEcrisArtifactPeriod,
  type ConnecticutEcrisArtifactRefreshResult,
  type ConnecticutEcrisArtifactTransactionType,
} from "../pipeline/connecticutFinance/connecticutEcrisArtifactCache.js";
import { writeConnecticutEcrisIndependentExpenditureCache } from "../pipeline/connecticutFinance/connecticutEcrisIndependentExpenditureCache.js";
import { fetchConnecticutEcrisIndependentExpenditures } from "../pipeline/connecticutFinance/connecticutEcrisIndependentExpenditureClient.js";

export const CONNECTICUT_ECRIS_RAW_DATA_REFRESH_JOB_NAME = "connecticut_ecris_raw_data_refresh";
export const CONNECTICUT_ECRIS_RAW_DATA_REFRESH_DAILY_SCHEDULER_ID =
  "connecticut_ecris_raw_data_refresh_daily";

export type ConnecticutEcrisRawDataRefreshJobData = {
  force?: boolean;
  year?: number;
  transactionType?: ConnecticutEcrisArtifactTransactionType;
  committeeType?: ConnecticutEcrisArtifactCommitteeType;
  period?: ConnecticutEcrisArtifactPeriod;
  format?: ConnecticutEcrisArtifactFormat;
  url?: string;
  cacheDir?: string;
  timeoutMs?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type ConnecticutEcrisIndependentExpenditureRefreshJobResult =
  | { status: "refreshed"; filePath: string; rowCount: number; fetchedAt: string }
  | { status: "failed"; error: string };

export type ConnecticutEcrisRawDataRefreshJobResult = {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<ConnecticutEcrisRawDataRefreshJobData["triggeredBy"]>;
  year: number;
  status: "disabled" | ConnecticutEcrisArtifactRefreshResult["status"];
  refresh: ConnecticutEcrisArtifactRefreshResult | null;
  /**
   * The year's SEEC Form 40 independent-expenditure artifact, refreshed after
   * the candidate receipts CSV (null when the job targets another artifact or
   * is disabled). A failed search never undoes the receipts refresh.
   */
  independentExpenditures: ConnecticutEcrisIndependentExpenditureRefreshJobResult | null;
};

type RawDataRefreshSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

type NormalizedRefreshJobData = Required<
  Pick<
    ConnecticutEcrisRawDataRefreshJobData,
    "year" | "transactionType" | "committeeType" | "period" | "format" | "url" | "cacheDir" | "timeoutMs"
  >
>;

function readSchedulerRuntimeConfig(): RawDataRefreshSchedulerRuntimeConfig {
  return {
    queueName:
      process.env.CONNECTICUT_ECRIS_RAW_DATA_REFRESH_QUEUE?.trim() ||
      "connecticut_ecris_raw_data_refresh_maintenance",
    dailyCron: process.env.CONNECTICUT_ECRIS_RAW_DATA_REFRESH_DAILY_CRON?.trim() || "25 8 * * *",
    dailyTz: process.env.CONNECTICUT_ECRIS_RAW_DATA_REFRESH_DAILY_TZ?.trim() || "UTC",
  };
}

function defaultRefreshYear(): number {
  return new Date().getUTCFullYear();
}

function normalizeYear(value: number | undefined): number {
  const year = value ?? defaultRefreshYear();
  if (!Number.isInteger(year) || year < 2008 || year > 2100) {
    throw new Error(`Invalid Connecticut eCRIS raw data refresh year: ${value}`);
  }
  return year;
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  // isSafeInteger, not isInteger: Number("9007199254740993") silently rounds
  // to 2^53 and still passes isInteger.
  if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
    throw new Error(`Invalid Connecticut eCRIS raw data refresh scheduler ${label}: ${value}`);
  }
}

function normalizeRefreshJobData(data: ConnecticutEcrisRawDataRefreshJobData): NormalizedRefreshJobData {
  const year = normalizeYear(data.year);
  const transactionType = data.transactionType ?? "receipts";
  const committeeType = data.committeeType ?? "candidate_exploratory";
  const period = data.period ?? "election";
  const format = data.format ?? "csv";
  const defaultUrl = buildConnecticutEcrisArtifactUrl({ year, transactionType, committeeType, period, format });

  return {
    year,
    transactionType,
    committeeType,
    period,
    format,
    url: parseConnecticutEcrisHttpsUrl(data.url?.trim() || defaultUrl, "url"),
    cacheDir: data.cacheDir?.trim() || DEFAULT_CONNECTICUT_ECRIS_CACHE_DIR,
    timeoutMs: data.timeoutMs ?? CONNECTICUT_ECRIS_FETCH_TIMEOUT_MS,
  };
}

function assertValidJobOptions(data: ConnecticutEcrisRawDataRefreshJobData): void {
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

export function createConnecticutEcrisRawDataRefreshQueue(): Queue<ConnecticutEcrisRawDataRefreshJobData> {
  return new Queue<ConnecticutEcrisRawDataRefreshJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringConnecticutEcrisRawDataRefreshJobs(
  jobData: ConnecticutEcrisRawDataRefreshJobData = {}
): Promise<void> {
  assertValidJobOptions(jobData);
  if (!isConnecticutCampaignFinanceEnabled()) {
    const queue = createConnecticutEcrisRawDataRefreshQueue();
    try {
      await queue.removeJobScheduler(CONNECTICUT_ECRIS_RAW_DATA_REFRESH_DAILY_SCHEDULER_ID);
    } finally {
      await queue.close();
    }
    return;
  }

  const config = readSchedulerRuntimeConfig();
  const normalized = normalizeRefreshJobData(jobData);
  const queue = createConnecticutEcrisRawDataRefreshQueue();

  try {
    await queue.upsertJobScheduler(
      CONNECTICUT_ECRIS_RAW_DATA_REFRESH_DAILY_SCHEDULER_ID,
      {
        pattern: config.dailyCron,
        tz: config.dailyTz,
      },
      {
        name: CONNECTICUT_ECRIS_RAW_DATA_REFRESH_JOB_NAME,
        data: {
          force: Boolean(jobData.force),
          year: normalized.year,
          transactionType: normalized.transactionType,
          committeeType: normalized.committeeType,
          period: normalized.period,
          format: normalized.format,
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

export async function enqueueManualConnecticutEcrisRawDataRefreshJob(
  jobData: ConnecticutEcrisRawDataRefreshJobData = {}
): Promise<string> {
  assertValidJobOptions(jobData);
  if (!isConnecticutEcrisRawDataRefreshEnabled(Boolean(jobData.force))) {
    return "disabled";
  }

  const normalized = normalizeRefreshJobData(jobData);
  const queue = createConnecticutEcrisRawDataRefreshQueue();

  try {
    const job = await queue.add(
      CONNECTICUT_ECRIS_RAW_DATA_REFRESH_JOB_NAME,
      {
        force: Boolean(jobData.force),
        year: normalized.year,
        transactionType: normalized.transactionType,
        committeeType: normalized.committeeType,
        period: normalized.period,
        format: normalized.format,
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

export async function runConnecticutEcrisRawDataRefreshJob(
  data: ConnecticutEcrisRawDataRefreshJobData = {}
): Promise<ConnecticutEcrisRawDataRefreshJobResult> {
  assertValidJobOptions(data);
  const force = Boolean(data.force);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const enabled = isConnecticutEcrisRawDataRefreshEnabled(force);
  const normalized = normalizeRefreshJobData(data);

  if (!data.triggeredBy) {
    console.warn("Connecticut eCRIS raw data refresh job missing triggeredBy; recording as unknown");
  }

  if (!enabled) {
    return {
      enabled: false,
      force,
      triggeredBy,
      year: normalized.year,
      status: "disabled",
      refresh: null,
      independentExpenditures: null,
    };
  }

  const refresh = await refreshConnecticutEcrisArtifactCache({
    cacheDir: normalized.cacheDir,
    year: normalized.year,
    transactionType: normalized.transactionType,
    committeeType: normalized.committeeType,
    period: normalized.period,
    format: normalized.format,
    url: normalized.url,
    force,
    timeoutMs: normalized.timeoutMs,
  });

  const independentExpenditures =
    normalized.transactionType === "receipts" && normalized.committeeType === "candidate_exploratory"
      ? await refreshIndependentExpenditures({ year: normalized.year, cacheDir: normalized.cacheDir, timeoutMs: normalized.timeoutMs })
      : null;

  return {
    enabled: true,
    force,
    triggeredBy,
    year: normalized.year,
    status: refresh.status,
    refresh,
    independentExpenditures,
  };
}

async function refreshIndependentExpenditures(input: {
  year: number;
  cacheDir: string;
  timeoutMs: number;
}): Promise<ConnecticutEcrisIndependentExpenditureRefreshJobResult> {
  try {
    const fetchResult = await fetchConnecticutEcrisIndependentExpenditures({ year: input.year, timeoutMs: input.timeoutMs });
    const written = await writeConnecticutEcrisIndependentExpenditureCache({ cacheDir: input.cacheDir, fetchResult });
    return {
      status: "refreshed",
      filePath: written.filePath,
      rowCount: written.artifact.rowCount,
      fetchedAt: written.artifact.fetchedAt,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(`Connecticut eCRIS independent expenditure refresh failed for ${input.year}:`, message);
    return { status: "failed", error: message };
  }
}

export function createConnecticutEcrisRawDataRefreshWorker(): Worker<
  ConnecticutEcrisRawDataRefreshJobData,
  ConnecticutEcrisRawDataRefreshJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<
    ConnecticutEcrisRawDataRefreshJobData,
    ConnecticutEcrisRawDataRefreshJobResult
  > = async (job) => {
    return runConnecticutEcrisRawDataRefreshJob(job.data ?? {});
  };

  return new Worker<ConnecticutEcrisRawDataRefreshJobData, ConnecticutEcrisRawDataRefreshJobResult>(
    queueName,
    processor,
    {
      connection,
      concurrency: 1,
    }
  );
}
