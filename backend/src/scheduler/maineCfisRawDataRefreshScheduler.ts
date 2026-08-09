import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { toConnectionOptions } from "../utils/redisConnection.js";

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

export type MaineCfisRawDataRefreshArtifactOutcome = {
  filingYear: number;
  status: MaineCfisArtifactRefreshResult["status"];
  refresh: MaineCfisArtifactRefreshResult;
};

export type MaineCfisRawDataRefreshJobResult = {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<MaineCfisRawDataRefreshJobData["triggeredBy"]>;
  filingYears: number[];
  artifactKind: MaineCfisArtifactKind;
  status: "disabled" | MaineCfisArtifactRefreshResult["status"];
  refreshes: MaineCfisRawDataRefreshArtifactOutcome[];
};

type MaineCfisRawDataRefreshSchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

type NormalizedRefreshJobData = {
  filingYears: number[];
  artifactKind: MaineCfisArtifactKind;
  url: string;
  cacheDir: string;
  timeoutMs: number;
};

function readSchedulerRuntimeConfig(): MaineCfisRawDataRefreshSchedulerRuntimeConfig {
  return {
    queueName: process.env.MAINE_CFIS_RAW_DATA_REFRESH_QUEUE?.trim() || "maine_cfis_raw_data_refresh_maintenance",
    dailyCron: process.env.MAINE_CFIS_RAW_DATA_REFRESH_DAILY_CRON?.trim() || "25 8 * * *",
    dailyTz: process.env.MAINE_CFIS_RAW_DATA_REFRESH_DAILY_TZ?.trim() || "UTC",
  };
}

// Maine CFIS bulk files are keyed by receipt year and the finance sync's cycle
// loader requires [electionYear - 1, electionYear], so an unpinned refresh must
// cover both cycle years. Resolved at RUN time (not bake time) so recurring
// jobs stay correct across year boundaries without re-upserting the scheduler.
function resolveRefreshFilingYears(value: number | undefined): number[] {
  if (value !== undefined) {
    return [normalizeMaineCfisFilingYear(value)];
  }
  const currentYear = new Date().getUTCFullYear();
  return [currentYear - 1, currentYear];
}

function normalizeArtifactKind(value: MaineCfisRawDataRefreshJobData["artifactKind"]): MaineCfisArtifactKind {
  return normalizeMaineCfisArtifactKind(value ?? "contributions");
}

function assertPositiveInteger(value: number | undefined, label: string): void {
  // isSafeInteger, not isInteger: Number("9007199254740993") silently rounds
  // to 2^53 and still passes isInteger.
  if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
    throw new Error(`Invalid Maine CFIS raw data refresh scheduler ${label}: ${value}`);
  }
}

function normalizeRefreshJobData(data: MaineCfisRawDataRefreshJobData): NormalizedRefreshJobData {
  return {
    filingYears: resolveRefreshFilingYears(data.filingYear),
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
            // Bake filingYear only when explicitly pinned; otherwise each
            // daily run resolves both cycle years at run time, so recurring
            // jobs keep covering [currentYear - 1, currentYear] across
            // year boundaries without re-upserting the scheduler.
            ...(jobData.filingYear === undefined
              ? {}
              : { filingYear: normalizeMaineCfisFilingYear(jobData.filingYear) }),
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
        ...(jobData.filingYear === undefined
          ? {}
          : { filingYear: normalizeMaineCfisFilingYear(jobData.filingYear) }),
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
      filingYears: normalized.filingYears,
      artifactKind: normalized.artifactKind,
      status: "disabled",
      refreshes: [],
    };
  }

  const refreshes: MaineCfisRawDataRefreshArtifactOutcome[] = [];
  for (const filingYear of normalized.filingYears) {
    const refresh = await refreshMaineCfisArtifactCache({
      filingYear,
      artifactKind: normalized.artifactKind,
      cacheDir: normalized.cacheDir,
      url: normalized.url,
      force,
      timeoutMs: normalized.timeoutMs,
    });
    refreshes.push({ filingYear, status: refresh.status, refresh });
  }

  return {
    enabled: true,
    force,
    triggeredBy,
    filingYears: normalized.filingYears,
    artifactKind: normalized.artifactKind,
    status: refreshes.some((outcome) => outcome.status === "downloaded") ? "downloaded" : "unchanged",
    refreshes,
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
