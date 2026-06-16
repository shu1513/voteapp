import { Queue, Worker, type JobsOptions, type Processor } from "bullmq";
import type { ConnectionOptions } from "bullmq";
import { Pool, type PoolClient } from "pg";

import { getPipelineEnv } from "../config/env.js";
import { isPresidentialElectionsEnabled } from "../config/featureFlags.js";
import {
  runPresidentialPrimaryDateResearchProducer,
  type PresidentialPrimaryDateResearchProducerResult,
} from "../pipeline/producers/presidentialPrimaryDateResearchProducer.js";
import { PRESIDENTIAL_PRIMARY_DATE_STATE_FIPS } from "../pipeline/presidential/presidentialPrimaryDates.js";
import {
  completeExpiredPresidentialPrimaryCycles,
  type CompleteExpiredPresidentialPrimaryCyclesResult,
} from "../pipeline/presidential/presidentialPrimaryCycleCompletion.js";
import {
  getPresidentialPrimaryDateResearchStartAt,
  getPresidentialPrimaryDateResearchStopAt,
} from "../pipeline/presidential/presidentialPrimaryDateResearchPolicy.js";

export const PRESIDENTIAL_PRIMARY_DATE_RESEARCH_ROLLOVER_JOB_NAME =
  "presidential_primary_date_research_rollover";
export const PRESIDENTIAL_PRIMARY_DATE_RESEARCH_DAILY_SCHEDULER_ID =
  "presidential_primary_date_research_daily_rollover";
export const PRESIDENTIAL_PRIMARY_DATE_RESEARCH_ACTIVATION_JOB_ID =
  "presidential_primary_date_research_activation";
export const PRESIDENTIAL_PRIMARY_DATE_RESEARCH_COMPLETION_JOB_ID =
  "presidential_primary_date_research_completion";

export type PresidentialPrimaryDateResearchRolloverJobData = {
  dryRun?: boolean;
  force?: boolean;
  triggeredBy?: "daily" | "manual" | "activation" | "completion" | "unknown";
  requestedAt?: string;
};

export type PresidentialPrimaryDateResearchRolloverJobResult =
  PresidentialPrimaryDateResearchProducerResult & {
    force: boolean;
    triggeredBy: NonNullable<PresidentialPrimaryDateResearchRolloverJobData["triggeredBy"]>;
    schedulerState?: PresidentialPrimaryDateResearchSchedulerState;
    schedulerSync?: Omit<SyncPresidentialPrimaryDateResearchSchedulerResult, "state">;
    cycleCompletion?: CompleteExpiredPresidentialPrimaryCyclesResult;
  };

type SchedulerRuntimeConfig = {
  queueName: string;
  dailyCron: string;
  dailyTz: string;
};

type Queryable = Pick<Pool | PoolClient, "query">;

type PrimaryCycleResearchStateRow = {
  cycle_id: string;
  election_year: number;
  official_found_count: string | number;
};

export type PresidentialPrimaryDateResearchSchedulerMode =
  | "active"
  | "sleep_until"
  | "complete";

export type PresidentialPrimaryDateResearchSchedulerState = {
  mode: PresidentialPrimaryDateResearchSchedulerMode;
  now: string;
  cycleCount: number;
  incompleteCycleCount: number;
  activeMissingCycleCount: number;
  expiredCycleCount: number;
  expiredIncompleteCycleCount: number;
  missingStatePartyRowCount: number;
  expiredMissingStatePartyRowCount: number;
  nextActivationAt: string | null;
  nextCompletionAt: string | null;
};

type SchedulerQueueLike = Pick<
  Queue<PresidentialPrimaryDateResearchRolloverJobData>,
  "add" | "getJob" | "removeJobScheduler" | "upsertJobScheduler"
>;

type ActivationJobRemovalResult = "none" | "removed" | "active";

export type SyncPresidentialPrimaryDateResearchSchedulerOptions = {
  dryRun?: boolean;
  force?: boolean;
  now?: Date;
};

export type SyncPresidentialPrimaryDateResearchSchedulerResult = {
  state: PresidentialPrimaryDateResearchSchedulerState;
  dailyScheduler: "upserted" | "removed" | "disabled";
  activationJob: "scheduled" | "removed" | "none";
  activationScheduledFor: string | null;
  completionJob: "scheduled" | "removed" | "none";
  completionScheduledFor: string | null;
};

function toSchedulerSyncSummary(
  result: SyncPresidentialPrimaryDateResearchSchedulerResult
): Omit<SyncPresidentialPrimaryDateResearchSchedulerResult, "state"> {
  return {
    dailyScheduler: result.dailyScheduler,
    activationJob: result.activationJob,
    activationScheduledFor: result.activationScheduledFor,
    completionJob: result.completionJob,
    completionScheduledFor: result.completionScheduledFor,
  };
}

function readSchedulerRuntimeConfig(): SchedulerRuntimeConfig {
  return {
    queueName:
      process.env.PRESIDENTIAL_PRIMARY_DATE_RESEARCH_SCHEDULER_QUEUE?.trim() ||
      "presidential_primary_date_research_maintenance",
    dailyCron:
      process.env.PRESIDENTIAL_PRIMARY_DATE_RESEARCH_DAILY_CRON?.trim() ||
      "45 8 * * *",
    dailyTz:
      process.env.PRESIDENTIAL_PRIMARY_DATE_RESEARCH_DAILY_TZ?.trim() ||
      "UTC",
  };
}

function parseCount(value: string | number | null | undefined): number {
  if (typeof value === "number") {
    return value;
  }
  const parsed = Number.parseInt(value ?? "0", 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

function assertValidDate(date: Date, label: string): void {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    throw new Error(`Invalid presidential primary date research scheduler ${label}`);
  }
}

export async function loadPresidentialPrimaryDateResearchSchedulerState(
  db: Queryable,
  now: Date = new Date()
): Promise<PresidentialPrimaryDateResearchSchedulerState> {
  assertValidDate(now, "now");

  const expectedStateCount = PRESIDENTIAL_PRIMARY_DATE_STATE_FIPS.length;
  const result = await db.query<PrimaryCycleResearchStateRow>(
    `
      SELECT
        pc.id AS cycle_id,
        pc.election_year,
        COUNT(pspd.id) FILTER (WHERE pspd.date_research_status = 'official_found') AS official_found_count
      FROM public.presidential_cycles AS pc
      LEFT JOIN public.presidential_state_primary_dates AS pspd
        ON pspd.cycle_id = pc.id
       AND pspd.state_fips = ANY($1::text[])
      WHERE pc.stage = 'primary'
        AND pc.status = 'active'
      GROUP BY pc.id, pc.election_year
      ORDER BY pc.election_year ASC, pc.id ASC
    `,
    [PRESIDENTIAL_PRIMARY_DATE_STATE_FIPS]
  );

  let incompleteCycleCount = 0;
  let activeMissingCycleCount = 0;
  let expiredCycleCount = 0;
  let expiredIncompleteCycleCount = 0;
  let missingStatePartyRowCount = 0;
  let expiredMissingStatePartyRowCount = 0;
  let nextActivationAt: Date | null = null;
  let nextCompletionAt: Date | null = null;

  for (const row of result.rows) {
    const researchStopAt = getPresidentialPrimaryDateResearchStopAt(row.election_year);
    if (now.getTime() < researchStopAt.getTime()) {
      if (!nextCompletionAt || researchStopAt.getTime() < nextCompletionAt.getTime()) {
        nextCompletionAt = researchStopAt;
      }
    } else {
      expiredCycleCount += 1;
    }

    const officialFoundCount = parseCount(row.official_found_count);
    const missingOfficialCount = Math.max(0, expectedStateCount - officialFoundCount);
    if (missingOfficialCount === 0) {
      continue;
    }

    const researchStartAt = getPresidentialPrimaryDateResearchStartAt(row.election_year);
    if (now.getTime() >= researchStopAt.getTime()) {
      expiredIncompleteCycleCount += 1;
      expiredMissingStatePartyRowCount += missingOfficialCount;
      continue;
    }

    incompleteCycleCount += 1;
    missingStatePartyRowCount += missingOfficialCount;

    if (now.getTime() >= researchStartAt.getTime()) {
      activeMissingCycleCount += 1;
    } else if (!nextActivationAt || researchStartAt.getTime() < nextActivationAt.getTime()) {
      nextActivationAt = researchStartAt;
    }
  }

  const mode: PresidentialPrimaryDateResearchSchedulerMode =
    activeMissingCycleCount > 0
      ? "active"
      : incompleteCycleCount > 0
      ? "sleep_until"
      : "complete";

  return {
    mode,
    now: now.toISOString(),
    cycleCount: result.rows.length,
    incompleteCycleCount,
    activeMissingCycleCount,
    expiredCycleCount,
    expiredIncompleteCycleCount,
    missingStatePartyRowCount,
    expiredMissingStatePartyRowCount,
    nextActivationAt: nextActivationAt?.toISOString() ?? null,
    nextCompletionAt: nextCompletionAt?.toISOString() ?? null,
  };
}

function toConnectionOptions(redisUrl: string): ConnectionOptions {
  const parsed = new URL(redisUrl);
  if (parsed.protocol !== "redis:" && parsed.protocol !== "rediss:") {
    throw new Error(`Unsupported REDIS_URL protocol: ${parsed.protocol}`);
  }
  const parsedPort = parsed.port ? Number.parseInt(parsed.port, 10) : 6379;
  const parsedDb = parsed.pathname.length > 1 ? Number.parseInt(parsed.pathname.slice(1), 10) : 0;

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

function buildEmptyProducerResult(input: {
  dryRun: boolean;
  force: boolean;
  now: Date;
}): PresidentialPrimaryDateResearchProducerResult {
  return {
    enabled: false,
    forced: input.force,
    dryRun: input.dryRun,
    now: input.now.toISOString(),
    maxRowsPerRun: 0,
    maxStatesPerJob: 0,
    maxJobsPerRun: 0,
    cyclesScanned: 0,
    eligibleCycleCount: 0,
    bootstrapRequestedRowCount: 0,
    bootstrapInsertedRowCount: 0,
    dueRowCount: 0,
    dueGroupCount: 0,
    selectedGroupCount: 0,
    maxRowsHit: false,
    maxGroupsHit: false,
    enqueuedJobCount: 0,
    updatedJobCount: 0,
    skippedActiveJobCount: 0,
  };
}

function buildDisabledSchedulerState(now: Date): PresidentialPrimaryDateResearchSchedulerState {
  return {
    mode: "complete",
    now: now.toISOString(),
    cycleCount: 0,
    incompleteCycleCount: 0,
    activeMissingCycleCount: 0,
    expiredCycleCount: 0,
    expiredIncompleteCycleCount: 0,
    missingStatePartyRowCount: 0,
    expiredMissingStatePartyRowCount: 0,
    nextActivationAt: null,
    nextCompletionAt: null,
  };
}

function buildDisabledSchedulerSync(now: Date): SyncPresidentialPrimaryDateResearchSchedulerResult {
  return {
    state: buildDisabledSchedulerState(now),
    dailyScheduler: "disabled",
    activationJob: "none",
    activationScheduledFor: null,
    completionJob: "none",
    completionScheduledFor: null,
  };
}

async function removeActivationJob(queue: SchedulerQueueLike): Promise<ActivationJobRemovalResult> {
  const existing = await queue.getJob(PRESIDENTIAL_PRIMARY_DATE_RESEARCH_ACTIVATION_JOB_ID);
  if (!existing) {
    return "none";
  }
  if (typeof existing.getState === "function" && (await existing.getState()) === "active") {
    return "active";
  }
  await existing.remove();
  return "removed";
}

async function removeCompletionJob(queue: SchedulerQueueLike): Promise<ActivationJobRemovalResult> {
  const existing = await queue.getJob(PRESIDENTIAL_PRIMARY_DATE_RESEARCH_COMPLETION_JOB_ID);
  if (!existing) {
    return "none";
  }
  if (typeof existing.getState === "function" && (await existing.getState()) === "active") {
    return "active";
  }
  await existing.remove();
  return "removed";
}

async function upsertDailyScheduler(
  queue: SchedulerQueueLike,
  jobData: PresidentialPrimaryDateResearchRolloverJobData
): Promise<void> {
  const config = readSchedulerRuntimeConfig();
  await queue.upsertJobScheduler(
    PRESIDENTIAL_PRIMARY_DATE_RESEARCH_DAILY_SCHEDULER_ID,
    {
      pattern: config.dailyCron,
      tz: config.dailyTz,
    },
    {
      name: PRESIDENTIAL_PRIMARY_DATE_RESEARCH_ROLLOVER_JOB_NAME,
      data: {
        dryRun: Boolean(jobData.dryRun),
        force: Boolean(jobData.force),
        triggeredBy: "daily",
      },
      opts: defaultJobOptions(),
    }
  );
}

async function scheduleActivationJob(
  queue: SchedulerQueueLike,
  activateAt: string,
  now: Date,
  jobData: PresidentialPrimaryDateResearchRolloverJobData
): Promise<void> {
  const activationAt = new Date(activateAt);
  assertValidDate(activationAt, "activation date");
  const delay = Math.max(0, activationAt.getTime() - now.getTime());

  const activationRemoval = await removeActivationJob(queue);
  const jobId =
    activationRemoval === "active"
      ? `${PRESIDENTIAL_PRIMARY_DATE_RESEARCH_ACTIVATION_JOB_ID}:${activationAt.toISOString()}`
      : PRESIDENTIAL_PRIMARY_DATE_RESEARCH_ACTIVATION_JOB_ID;

  await queue.add(
    PRESIDENTIAL_PRIMARY_DATE_RESEARCH_ROLLOVER_JOB_NAME,
    {
      dryRun: Boolean(jobData.dryRun),
      force: Boolean(jobData.force),
      triggeredBy: "activation",
      requestedAt: now.toISOString(),
    },
    {
      ...defaultJobOptions(),
      delay,
      jobId,
    }
  );
}

async function scheduleCompletionJob(
  queue: SchedulerQueueLike,
  completeAt: string,
  now: Date,
  jobData: PresidentialPrimaryDateResearchRolloverJobData
): Promise<void> {
  const completionAt = new Date(completeAt);
  assertValidDate(completionAt, "completion date");
  const delay = Math.max(0, completionAt.getTime() - now.getTime());

  const completionRemoval = await removeCompletionJob(queue);
  const jobId =
    completionRemoval === "active"
      ? `${PRESIDENTIAL_PRIMARY_DATE_RESEARCH_COMPLETION_JOB_ID}:${completionAt.toISOString()}`
      : PRESIDENTIAL_PRIMARY_DATE_RESEARCH_COMPLETION_JOB_ID;

  await queue.add(
    PRESIDENTIAL_PRIMARY_DATE_RESEARCH_ROLLOVER_JOB_NAME,
    {
      dryRun: Boolean(jobData.dryRun),
      force: Boolean(jobData.force),
      triggeredBy: "completion",
      requestedAt: now.toISOString(),
    },
    {
      ...defaultJobOptions(),
      delay,
      jobId,
    }
  );
}

async function syncCompletionJob(
  queue: SchedulerQueueLike,
  state: PresidentialPrimaryDateResearchSchedulerState,
  now: Date,
  options: SyncPresidentialPrimaryDateResearchSchedulerOptions
): Promise<Pick<SyncPresidentialPrimaryDateResearchSchedulerResult, "completionJob" | "completionScheduledFor">> {
  if (state.nextCompletionAt) {
    await scheduleCompletionJob(queue, state.nextCompletionAt, now, options);
    return {
      completionJob: "scheduled",
      completionScheduledFor: state.nextCompletionAt,
    };
  }

  if (state.expiredCycleCount > 0) {
    await scheduleCompletionJob(queue, now.toISOString(), now, options);
    return {
      completionJob: "scheduled",
      completionScheduledFor: now.toISOString(),
    };
  }

  const completionRemoval = await removeCompletionJob(queue);
  return {
    completionJob: completionRemoval === "removed" ? "removed" : "none",
    completionScheduledFor: null,
  };
}

export async function syncPresidentialPrimaryDateResearchScheduler(
  db: Queryable,
  queue: SchedulerQueueLike,
  options: SyncPresidentialPrimaryDateResearchSchedulerOptions = {}
): Promise<SyncPresidentialPrimaryDateResearchSchedulerResult> {
  const now = options.now ?? new Date();
  assertValidDate(now, "now");
  const state = await loadPresidentialPrimaryDateResearchSchedulerState(db, now);
  const completionSync = await syncCompletionJob(queue, state, now, options);

  if (state.mode === "active") {
    await upsertDailyScheduler(queue, options);
    const activationRemoval = await removeActivationJob(queue);
    return {
      state,
      dailyScheduler: "upserted",
      activationJob: activationRemoval === "removed" ? "removed" : "none",
      activationScheduledFor: null,
      ...completionSync,
    };
  }

  await queue.removeJobScheduler(PRESIDENTIAL_PRIMARY_DATE_RESEARCH_DAILY_SCHEDULER_ID);

  if (state.mode === "sleep_until" && state.nextActivationAt) {
    await scheduleActivationJob(queue, state.nextActivationAt, now, options);
    return {
      state,
      dailyScheduler: "removed",
      activationJob: "scheduled",
      activationScheduledFor: state.nextActivationAt,
      ...completionSync,
    };
  }

  const activationRemoval = await removeActivationJob(queue);
  return {
    state,
    dailyScheduler: "removed",
    activationJob: activationRemoval === "removed" ? "removed" : "none",
    activationScheduledFor: null,
    ...completionSync,
  };
}

export function createPresidentialPrimaryDateResearchSchedulerQueue(): Queue<PresidentialPrimaryDateResearchRolloverJobData> {
  return new Queue<PresidentialPrimaryDateResearchRolloverJobData>(getQueueName(), {
    connection: getQueueConnection(),
    defaultJobOptions: defaultJobOptions(),
  });
}

export async function upsertRecurringPresidentialPrimaryDateResearchJobs(
  jobData: PresidentialPrimaryDateResearchRolloverJobData = {}
): Promise<SyncPresidentialPrimaryDateResearchSchedulerResult> {
  const now = new Date();
  if (!isPresidentialElectionsEnabled()) {
    const queue = createPresidentialPrimaryDateResearchSchedulerQueue();
    try {
      await queue.removeJobScheduler(PRESIDENTIAL_PRIMARY_DATE_RESEARCH_DAILY_SCHEDULER_ID);
      await removeActivationJob(queue);
      await removeCompletionJob(queue);
    } finally {
      await queue.close();
    }
    return buildDisabledSchedulerSync(now);
  }

  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const queue = createPresidentialPrimaryDateResearchSchedulerQueue();

  try {
    return await syncPresidentialPrimaryDateResearchScheduler(pool, queue, {
      dryRun: Boolean(jobData.dryRun),
      force: Boolean(jobData.force),
    });
  } finally {
    await queue.close();
    await pool.end();
  }
}

export async function enqueueManualPresidentialPrimaryDateResearchJob(
  jobData: PresidentialPrimaryDateResearchRolloverJobData = {}
): Promise<string> {
  if (!isPresidentialElectionsEnabled()) {
    return "disabled";
  }

  const queue = createPresidentialPrimaryDateResearchSchedulerQueue();

  try {
    const job = await queue.add(
      PRESIDENTIAL_PRIMARY_DATE_RESEARCH_ROLLOVER_JOB_NAME,
      {
        dryRun: Boolean(jobData.dryRun),
        force: Boolean(jobData.force),
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

async function runAdaptivePresidentialPrimaryDateResearchRolloverJob(input: {
  dryRun: boolean;
  force: boolean;
  triggeredBy: NonNullable<PresidentialPrimaryDateResearchRolloverJobData["triggeredBy"]>;
}): Promise<PresidentialPrimaryDateResearchRolloverJobResult> {
  const now = new Date();
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const queue = createPresidentialPrimaryDateResearchSchedulerQueue();

  try {
    const cycleCompletion =
      input.triggeredBy === "completion"
        ? await completeExpiredPresidentialPrimaryCycles(pool, {
            dryRun: input.dryRun,
            now,
          })
        : undefined;
    const initialSync = await syncPresidentialPrimaryDateResearchScheduler(pool, queue, {
      dryRun: input.dryRun,
      force: input.force,
      now,
    });

    if (initialSync.state.mode !== "active") {
      return {
        ...buildEmptyProducerResult({
          dryRun: input.dryRun,
          force: input.force,
          now,
        }),
        force: input.force,
        triggeredBy: input.triggeredBy,
        schedulerState: initialSync.state,
        schedulerSync: toSchedulerSyncSummary(initialSync),
        cycleCompletion,
      };
    }

    const producerResult = await runPresidentialPrimaryDateResearchProducer({
      dryRun: input.dryRun,
      force: input.force,
      now,
    });
    const finalSync = await syncPresidentialPrimaryDateResearchScheduler(pool, queue, {
      dryRun: input.dryRun,
      force: input.force,
      now,
    });

    return {
      ...producerResult,
      force: input.force,
      triggeredBy: input.triggeredBy,
      schedulerState: initialSync.state,
      schedulerSync: toSchedulerSyncSummary(finalSync),
      cycleCompletion,
    };
  } finally {
    await queue.close();
    await pool.end();
  }
}

export async function runPresidentialPrimaryDateResearchRolloverJob(
  data: PresidentialPrimaryDateResearchRolloverJobData = {}
): Promise<PresidentialPrimaryDateResearchRolloverJobResult> {
  const force = Boolean(data.force);
  const triggeredBy = data.triggeredBy ?? "unknown";
  const dryRun = Boolean(data.dryRun);
  const now = new Date();

  if (!data.triggeredBy) {
    console.warn(
      "presidential primary date research rollover job missing triggeredBy; recording as unknown"
    );
  }

  if (!isPresidentialElectionsEnabled()) {
    const schedulerSync = buildDisabledSchedulerSync(now);
    return {
      ...buildEmptyProducerResult({ dryRun, force, now }),
      force,
      triggeredBy,
      schedulerState: schedulerSync.state,
      schedulerSync: toSchedulerSyncSummary(schedulerSync),
    };
  }

  return runAdaptivePresidentialPrimaryDateResearchRolloverJob({
    dryRun,
    force,
    triggeredBy,
  });
}

export function createPresidentialPrimaryDateResearchSchedulerWorker(): Worker<
  PresidentialPrimaryDateResearchRolloverJobData,
  PresidentialPrimaryDateResearchRolloverJobResult
> {
  const connection = getQueueConnection();
  const queueName = getQueueName();

  const processor: Processor<
    PresidentialPrimaryDateResearchRolloverJobData,
    PresidentialPrimaryDateResearchRolloverJobResult
  > = async (job) => {
    return runPresidentialPrimaryDateResearchRolloverJob(job.data ?? {});
  };

  return new Worker<
    PresidentialPrimaryDateResearchRolloverJobData,
    PresidentialPrimaryDateResearchRolloverJobResult
  >(queueName, processor, {
    connection,
    concurrency: 1,
  });
}
