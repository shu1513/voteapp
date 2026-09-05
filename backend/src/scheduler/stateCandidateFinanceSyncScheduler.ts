// Shared BullMQ scheduler for a state's link-gated candidate finance sync.
// One config produces the queue factory, the daily job-scheduler upsert (or
// removal when the master flag is off), the manual enqueue, the job runner
// and the worker — the shape washington/hawaii/wisconsin/virginia/
// massachusetts carried as five identical files. States keep exporting their
// own names, constants and types; anything that deviates from this shape
// (sync-gated registration, pass caps) stays in that state's own file.

import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { Pool } from "pg";

import { getPipelineEnv } from "../config/env.js";
import { toConnectionOptions } from "../utils/redisConnection.js";

export type StateCandidateFinanceSyncJobData = {
  dryRun?: boolean;
  force?: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
  triggeredBy?: "daily" | "manual" | "unknown";
  requestedAt?: string;
};

export type StateCandidateFinanceSyncEnqueueOptions = {
  jobId?: string;
};

/** The batch-sync result shape every migrated state's syncDue returns. */
export type StateCandidateFinanceBatchSyncResult<TItem> = {
  dryRun: boolean;
  now: string;
  staleAfterDays: number;
  maxCandidates: number;
  dueCandidateCount: number;
  selectedCandidateCount: number;
  syncedCandidateCount: number;
  failedCandidateCount: number;
  autoLinkAttemptedCount: number;
  autoLinkLinkedCount: number;
  results: TItem[];
};

export type StateCandidateFinanceSyncJobResult<TItem> = StateCandidateFinanceBatchSyncResult<TItem> & {
  enabled: boolean;
  force: boolean;
  triggeredBy: NonNullable<StateCandidateFinanceSyncJobData["triggeredBy"]>;
};

/** What the job runner hands to the state's syncDue. */
export type StateCandidateFinanceBatchSyncCall = {
  db: Pool;
  now: Date;
  dryRun: boolean;
  maxCandidates?: number;
  staleAfterDays?: number;
  electionLookbackDays?: number;
  electionLookaheadDays?: number;
};

export type StateCandidateFinanceSyncSchedulerConfig<TItem> = {
  /** Human label used in error and log messages, e.g. "Washington". */
  stateLabel: string;
  /** BullMQ job name for both the daily and manual jobs. */
  jobName: string;
  /** Job-scheduler id for the daily recurring job. */
  dailySchedulerId: string;
  /** Queue name when the env override is unset. */
  defaultQueueName: string;
  /** Prefix of the deterministic linked-election job id; the ISO date is appended. */
  linkedElectionJobIdPrefix: string;
  /**
   * Env prefix for the runtime overrides: `${envPrefix}_SCHEDULER_QUEUE`,
   * `${envPrefix}_DAILY_CRON`, `${envPrefix}_DAILY_TZ`.
   */
  envPrefix: string;
  /** Cron pattern when `${envPrefix}_DAILY_CRON` is unset. */
  defaultDailyCron: string;
  /** Master flag: gates the recurring scheduler and is never bypassed by force. */
  isEnabled: () => boolean;
  /** Sync flag (master AND sync-or-force): gates manual enqueue and the job run. */
  isSyncEnabled: (force: boolean) => boolean;
  syncDue: (input: StateCandidateFinanceBatchSyncCall) => Promise<StateCandidateFinanceBatchSyncResult<TItem>>;
};

export type StateCandidateFinanceSyncScheduler<TItem> = {
  createQueue: () => Queue<StateCandidateFinanceSyncJobData>;
  buildLinkedElectionSyncJobId: (now?: Date) => string;
  upsertRecurringJobs: (jobData?: StateCandidateFinanceSyncJobData) => Promise<void>;
  enqueueManualJob: (
    jobData?: StateCandidateFinanceSyncJobData,
    options?: StateCandidateFinanceSyncEnqueueOptions
  ) => Promise<string>;
  runJob: (data?: StateCandidateFinanceSyncJobData) => Promise<StateCandidateFinanceSyncJobResult<TItem>>;
  createWorker: () => Worker<StateCandidateFinanceSyncJobData, StateCandidateFinanceSyncJobResult<TItem>>;
};

type SchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

const DISABLED_RESULT_DEFAULT_MAX_CANDIDATES = 0;
const DISABLED_RESULT_DEFAULT_STALE_AFTER_DAYS = 0;

function getQueueConnection(): ConnectionOptions {
  const env = getPipelineEnv();
  return toConnectionOptions(env.REDIS_URL);
}

function defaultJobOptions(): JobsOptions {
  return {
    removeOnComplete: 1000,
    removeOnFail: 1000,
  };
}

export function createStateCandidateFinanceSyncScheduler<TItem>(
  config: StateCandidateFinanceSyncSchedulerConfig<TItem>
): StateCandidateFinanceSyncScheduler<TItem> {
  type JobData = StateCandidateFinanceSyncJobData;
  type JobResult = StateCandidateFinanceSyncJobResult<TItem>;

  function readSchedulerRuntimeConfig(): SchedulerRuntimeConfig {
    return {
      queueName: process.env[`${config.envPrefix}_SCHEDULER_QUEUE`]?.trim() || config.defaultQueueName,
      dailyCron: process.env[`${config.envPrefix}_DAILY_CRON`]?.trim() || config.defaultDailyCron,
      dailyTz: process.env[`${config.envPrefix}_DAILY_TZ`]?.trim() || "UTC",
    };
  }

  function assertPositiveInteger(value: number | undefined, label: string): void {
    // isSafeInteger, not isInteger: Number("9007199254740993") silently rounds
    // to 2^53 and still passes isInteger.
    if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0)) {
      throw new Error(`Invalid ${config.stateLabel} finance sync scheduler ${label}: ${value}`);
    }
  }

  function getQueueName(): string {
    return readSchedulerRuntimeConfig().queueName;
  }

  function normalizeOptionalJobId(value: string | undefined): string | undefined {
    const trimmed = value?.trim();
    if (!trimmed) {
      return undefined;
    }
    if (trimmed.includes(":")) {
      throw new Error(`${config.stateLabel} finance sync scheduler jobId must not contain ':'`);
    }
    return trimmed;
  }

  function jobOptionsWithId(jobId: string | undefined): JobsOptions {
    const normalizedJobId = normalizeOptionalJobId(jobId);
    return normalizedJobId ? { ...defaultJobOptions(), jobId: normalizedJobId } : defaultJobOptions();
  }

  function assertValidJobOptions(data: JobData): void {
    assertPositiveInteger(data.maxCandidates, "maxCandidates");
    assertPositiveInteger(data.staleAfterDays, "staleAfterDays");
    assertPositiveInteger(data.electionLookbackDays, "electionLookbackDays");
    assertPositiveInteger(data.electionLookaheadDays, "electionLookaheadDays");
  }

  function createQueue(): Queue<JobData> {
    return new Queue<JobData>(getQueueName(), {
      connection: getQueueConnection(),
      defaultJobOptions: defaultJobOptions(),
    });
  }

  function buildLinkedElectionSyncJobId(now = new Date()): string {
    if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
      throw new Error(`Invalid ${config.stateLabel} finance linked-election sync job date`);
    }
    return `${config.linkedElectionJobIdPrefix}${now.toISOString().slice(0, 10)}`;
  }

  async function upsertRecurringJobs(jobData: JobData = {}): Promise<void> {
    assertValidJobOptions(jobData);
    if (!config.isEnabled()) {
      const queue = createQueue();
      try {
        await queue.removeJobScheduler(config.dailySchedulerId);
      } finally {
        await queue.close();
      }
      return;
    }

    const runtime = readSchedulerRuntimeConfig();
    const queue = createQueue();

    try {
      await queue.upsertJobScheduler(
        config.dailySchedulerId,
        {
          pattern: runtime.dailyCron,
          tz: runtime.dailyTz,
        },
        {
          name: config.jobName,
          data: {
            dryRun: Boolean(jobData.dryRun),
            force: Boolean(jobData.force),
            maxCandidates: jobData.maxCandidates,
            staleAfterDays: jobData.staleAfterDays,
            electionLookbackDays: jobData.electionLookbackDays,
            electionLookaheadDays: jobData.electionLookaheadDays,
            triggeredBy: "daily",
          },
          opts: defaultJobOptions(),
        }
      );
    } finally {
      await queue.close();
    }
  }

  async function enqueueManualJob(
    jobData: JobData = {},
    options: StateCandidateFinanceSyncEnqueueOptions = {}
  ): Promise<string> {
    assertValidJobOptions(jobData);
    if (!config.isSyncEnabled(Boolean(jobData.force))) {
      return "disabled";
    }

    const queue = createQueue();

    try {
      const job = await queue.add(
        config.jobName,
        {
          dryRun: Boolean(jobData.dryRun),
          force: Boolean(jobData.force),
          maxCandidates: jobData.maxCandidates,
          staleAfterDays: jobData.staleAfterDays,
          electionLookbackDays: jobData.electionLookbackDays,
          electionLookaheadDays: jobData.electionLookaheadDays,
          triggeredBy: "manual",
          requestedAt: new Date().toISOString(),
        },
        jobOptionsWithId(options.jobId)
      );
      return job.id ?? "unknown";
    } finally {
      await queue.close();
    }
  }

  async function runJob(data: JobData = {}): Promise<JobResult> {
    assertValidJobOptions(data);
    const force = Boolean(data.force);
    const dryRun = Boolean(data.dryRun);
    const triggeredBy = data.triggeredBy ?? "unknown";
    const now = new Date();
    const enabled = config.isSyncEnabled(force);

    if (!data.triggeredBy) {
      console.warn(`${config.stateLabel} finance sync job missing triggeredBy; recording as unknown`);
    }

    if (!enabled) {
      return {
        enabled: false,
        force,
        triggeredBy,
        dryRun,
        now: now.toISOString(),
        staleAfterDays: data.staleAfterDays ?? DISABLED_RESULT_DEFAULT_STALE_AFTER_DAYS,
        maxCandidates: data.maxCandidates ?? DISABLED_RESULT_DEFAULT_MAX_CANDIDATES,
        dueCandidateCount: 0,
        selectedCandidateCount: 0,
        syncedCandidateCount: 0,
        failedCandidateCount: 0,
        autoLinkAttemptedCount: 0,
        autoLinkLinkedCount: 0,
        results: [],
      };
    }

    const env = getPipelineEnv();
    const pool = new Pool({ connectionString: env.DATABASE_URL });
    try {
      const result = await config.syncDue({
        db: pool,
        now,
        dryRun,
        maxCandidates: data.maxCandidates,
        staleAfterDays: data.staleAfterDays,
        electionLookbackDays: data.electionLookbackDays,
        electionLookaheadDays: data.electionLookaheadDays,
      });

      return {
        enabled: true,
        force,
        triggeredBy,
        ...result,
      };
    } finally {
      await pool.end();
    }
  }

  function createWorker(): Worker<JobData, JobResult> {
    const connection = getQueueConnection();
    const queueName = getQueueName();

    const processor: Processor<JobData, JobResult> = async (job) => {
      return runJob(job.data ?? {});
    };

    return new Worker<JobData, JobResult>(queueName, processor, {
      connection,
      concurrency: 1,
    });
  }

  return {
    createQueue,
    buildLinkedElectionSyncJobId,
    upsertRecurringJobs,
    enqueueManualJob,
    runJob,
    createWorker,
  };
}
