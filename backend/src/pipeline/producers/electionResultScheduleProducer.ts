import { Queue, type JobsOptions } from "bullmq";
import type { Job } from "bullmq";
import { toConnectionOptions } from "../../utils/redisConnection.js";
import { Pool } from "pg";
import { createClient } from "redis";

import { getPipelineEnv } from "../../config/env.js";
import type { ElectionResultPassType } from "../../types/electionResults.js";
import {
  computeElectionResultScheduledAtUtc,
  getElectionResultScheduleForState,
} from "../electionResults/electionResultSchedules.js";
import {
  CERTIFIED_RESULT_MAX_ATTEMPTS,
  CERTIFIED_RESULT_RETRY_INTERVAL_DAYS,
  ELECTION_NIGHT_RESULT_MAX_ATTEMPTS,
} from "../electionResults/electionResultRetryPolicy.js";
import {
  ELECTION_RESULT_PASS_EMITTED_MARKER_TTL_SECONDS,
  isElectionResultPassEmitted,
  markElectionResultPassEmitted,
} from "../electionResults/electionResultPassMarkers.js";

export const ELECTION_RESULT_SEARCH_JOB_NAME = "election_result_search";

export type ElectionResultSearchJobData = {
  state: string;
  election_date: string;
  pass_type: ElectionResultPassType;
  scheduled_for: string;
  election_ids: string[];
  run_id: string;
};

type ElectionResultScheduleRow = {
  election_id: string;
  state: string;
  election_date: string;
  election_night_results_checked_at: string | null;
  certified_results_checked_at: string | null;
  election_night_results_attempt_count: number;
  election_night_results_last_attempted_at: string | null;
  certified_results_attempt_count: number;
  certified_results_last_attempted_at: string | null;
};

type ProducerOptions = {
  dryRun?: boolean;
  force?: boolean;
  now?: Date;
  lookaheadHours?: number;
  maxGroupsPerRun?: number;
};

type DueGroup = {
  state: string;
  electionDate: string;
  passType: ElectionResultPassType;
  scheduledFor: Date;
  electionIds: string[];
};

export type ElectionResultScheduleProducerResult = {
  enabled: boolean;
  forced: boolean;
  dryRun: boolean;
  now: string;
  lookaheadHours: number;
  maxGroupsPerRun: number;
  electionsScanned: number;
  dueElectionCount: number;
  dueGroupCount: number;
  selectedGroupCount: number;
  maxGroupsHit: boolean;
  enqueuedJobCount: number;
  updatedJobCount: number;
  skippedActiveJobCount: number;
  skippedUnknownStateCount: number;
  markerSkippedElectionCount: number;
};

type QueueLike = Pick<Queue<ElectionResultSearchJobData>, "add" | "getJob" | "close">;
type RedisMarkerLike = Pick<ReturnType<typeof createClient>, "connect" | "exists" | "set" | "quit" | "isOpen">;

type UpsertOutcome = "created" | "updated" | "skipped_active";

const ACTIVE_JOB_STATES = new Set(["active"]);
const ELECTION_NIGHT_RESULT_RETRY_MARKER_BUFFER_SECONDS = 12 * 60 * 60;

function parsePositiveIntegerEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw || raw.trim().length === 0) {
    return fallback;
  }
  const normalized = raw.trim();
  if (!/^[1-9]\d*$/.test(normalized)) {
    throw new Error(`Invalid positive integer env ${name}: ${raw}`);
  }
  const parsed = Number.parseInt(normalized, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Invalid positive integer env ${name}: ${raw}`);
  }
  return parsed;
}

function getSearchQueueName(): string {
  return process.env.ELECTION_RESULT_SEARCH_QUEUE?.trim() || "election_result_search";
}

function defaultSearchJobOptions(): JobsOptions {
  return {
    attempts: 3,
    backoff: {
      type: "exponential",
      delay: 5 * 60 * 1000,
    },
    removeOnComplete: 1000,
    removeOnFail: 1000,
  };
}

function createElectionResultSearchQueue(): Queue<ElectionResultSearchJobData> {
  const env = getPipelineEnv();
  return new Queue<ElectionResultSearchJobData>(getSearchQueueName(), {
    connection: toConnectionOptions(env.REDIS_URL),
    defaultJobOptions: defaultSearchJobOptions(),
  });
}

function readProducerPolicy(): { enabled: boolean; lookaheadHours: number; maxGroupsPerRun: number } {
  return {
    enabled: process.env.ELECTION_RESULTS_SCHEDULER_ENABLED === "true",
    lookaheadHours: parsePositiveIntegerEnv("ELECTION_RESULTS_LOOKAHEAD_HOURS", 48),
    maxGroupsPerRun: parsePositiveIntegerEnv("ELECTION_RESULTS_MAX_GROUPS_PER_RUN", 200),
  };
}

function toIsoDate(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function addHours(date: Date, hours: number): Date {
  return new Date(date.getTime() + hours * 60 * 60 * 1000);
}

export function buildElectionResultSearchJobId(input: {
  state: string;
  electionDate: string;
  passType: ElectionResultPassType;
}): string {
  return `election-results:${input.state}:${input.electionDate}:${input.passType}`;
}

async function listPotentialElectionResultRows(
  pool: Pool,
  horizonDate: string,
  now: Date,
  maxRows: number
): Promise<ElectionResultScheduleRow[]> {
  const result = await pool.query<ElectionResultScheduleRow>(
    `
      SELECT
        e.id AS election_id,
        d.state AS state,
        e.election_date::text AS election_date,
        e.election_night_results_checked_at::text AS election_night_results_checked_at,
        e.certified_results_checked_at::text AS certified_results_checked_at,
        e.election_night_results_attempt_count,
        e.election_night_results_last_attempted_at::text AS election_night_results_last_attempted_at,
        e.certified_results_attempt_count,
        e.certified_results_last_attempted_at::text AS certified_results_last_attempted_at
      FROM public.elections AS e
      JOIN public.districts AS d
        ON d.id = e.district_id
      WHERE e.election_date <= $1::date
        AND (
          (
            e.election_night_results_checked_at IS NULL
            AND e.election_night_results_attempt_count < ${ELECTION_NIGHT_RESULT_MAX_ATTEMPTS}
          )
          OR (
            e.certified_results_checked_at IS NULL
            AND e.certified_results_attempt_count < ${CERTIFIED_RESULT_MAX_ATTEMPTS}
            AND (
              e.certified_results_last_attempted_at IS NULL
              OR e.certified_results_last_attempted_at <=
                $3::timestamptz - (${CERTIFIED_RESULT_RETRY_INTERVAL_DAYS} * interval '1 day')
            )
          )
        )
      ORDER BY e.election_date ASC, d.state ASC, e.id ASC
      LIMIT $2::int
    `,
    [horizonDate, maxRows, now.toISOString()]
  );
  return result.rows;
}

async function buildDueGroups(
  pool: Pool,
  rows: readonly ElectionResultScheduleRow[],
  now: Date,
  horizon: Date,
  redis: Pick<RedisMarkerLike, "exists"> | null
): Promise<{
  groups: DueGroup[];
  dueElectionCount: number;
  skippedUnknownStateCount: number;
  markerSkippedElectionCount: number;
}> {
  const groups = new Map<string, DueGroup>();
  const scheduledCache = new Map<string, Date>();
  let dueElectionCount = 0;
  let skippedUnknownStateCount = 0;
  let markerSkippedElectionCount = 0;

  for (const row of rows) {
    const state = row.state.trim().toUpperCase();
    if (!getElectionResultScheduleForState(state)) {
      skippedUnknownStateCount += 1;
      continue;
    }

    const passTypes: ElectionResultPassType[] = [];
    if (
      !row.election_night_results_checked_at &&
      row.election_night_results_attempt_count < ELECTION_NIGHT_RESULT_MAX_ATTEMPTS
    ) {
      passTypes.push("election_night");
    }
    if (
      !row.certified_results_checked_at &&
      row.certified_results_attempt_count < CERTIFIED_RESULT_MAX_ATTEMPTS &&
      (
        !row.certified_results_last_attempted_at ||
        new Date(row.certified_results_last_attempted_at).getTime() <=
          now.getTime() - CERTIFIED_RESULT_RETRY_INTERVAL_DAYS * 24 * 60 * 60 * 1000
      )
    ) {
      passTypes.push("certified");
    }

    for (const passType of passTypes) {
      const scheduleKey = `${state}|${row.election_date}|${passType}`;
      let scheduledFor = scheduledCache.get(scheduleKey);
      if (!scheduledFor) {
        scheduledFor = await computeElectionResultScheduledAtUtc(pool, {
          state,
          electionDate: row.election_date,
          passType,
        });
        scheduledCache.set(scheduleKey, scheduledFor);
      }

      if (scheduledFor.getTime() > horizon.getTime()) {
        continue;
      }
      if (
        redis &&
        (await isElectionResultPassEmitted(redis, {
          electionId: row.election_id,
          passType,
        }))
      ) {
        markerSkippedElectionCount += 1;
        continue;
      }

      const groupKey = scheduleKey;
      const existing = groups.get(groupKey);
      if (existing) {
        existing.electionIds.push(row.election_id);
      } else {
        groups.set(groupKey, {
          state,
          electionDate: row.election_date,
          passType,
          scheduledFor,
          electionIds: [row.election_id],
        });
      }
      dueElectionCount += 1;
    }
  }

  return {
    groups: [...groups.values()].sort((a, b) => {
      const timeDiff = a.scheduledFor.getTime() - b.scheduledFor.getTime();
      if (timeDiff !== 0) return timeDiff;
      return `${a.state}:${a.electionDate}:${a.passType}`.localeCompare(
        `${b.state}:${b.electionDate}:${b.passType}`
      );
    }),
    dueElectionCount,
    skippedUnknownStateCount,
    markerSkippedElectionCount,
  };
}

async function upsertDelayedElectionResultSearchJob(
  queue: QueueLike,
  group: DueGroup,
  runId: string,
  now: Date
): Promise<UpsertOutcome> {
  const jobId = buildElectionResultSearchJobId({
    state: group.state,
    electionDate: group.electionDate,
    passType: group.passType,
  });
  const existing = await queue.getJob(jobId);
  let electionIds = group.electionIds;
  if (existing) {
    const state = await (existing as Job).getState();
    if (ACTIVE_JOB_STATES.has(state)) {
      return "skipped_active";
    }
    const existingElectionIds = Array.isArray((existing as Job<ElectionResultSearchJobData>).data?.election_ids)
      ? (existing as Job<ElectionResultSearchJobData>).data.election_ids
      : [];
    electionIds = [...existingElectionIds, ...group.electionIds];
    await existing.remove();
  }

  const scheduledForMs = group.scheduledFor.getTime();
  const delay = Math.max(0, scheduledForMs - now.getTime());
  await queue.add(
    ELECTION_RESULT_SEARCH_JOB_NAME,
    {
      state: group.state,
      election_date: group.electionDate,
      pass_type: group.passType,
      scheduled_for: group.scheduledFor.toISOString(),
      election_ids: [...new Set(electionIds)],
      run_id: runId,
    },
    {
      ...defaultSearchJobOptions(),
      jobId,
      delay,
    }
  );

  return existing ? "updated" : "created";
}

function emittedMarkerTtlSeconds(input: {
  passType: ElectionResultPassType;
  scheduledFor: Date;
  now: Date;
}): number {
  if (input.passType !== "election_night") {
    return ELECTION_RESULT_PASS_EMITTED_MARKER_TTL_SECONDS;
  }
  const delaySeconds = Math.max(0, Math.ceil((input.scheduledFor.getTime() - input.now.getTime()) / 1000));
  return delaySeconds + ELECTION_NIGHT_RESULT_RETRY_MARKER_BUFFER_SECONDS;
}

export async function runElectionResultScheduleProducer(
  options: ProducerOptions = {}
): Promise<ElectionResultScheduleProducerResult> {
  const policy = readProducerPolicy();
  const force = Boolean(options.force);
  const dryRun = Boolean(options.dryRun);
  const enabled = force || policy.enabled;
  const now = options.now ?? new Date();
  const lookaheadHours = options.lookaheadHours ?? policy.lookaheadHours;
  const maxGroupsPerRun = options.maxGroupsPerRun ?? policy.maxGroupsPerRun;

  if (!enabled) {
    return {
      enabled: false,
      forced: force,
      dryRun,
      now: now.toISOString(),
      lookaheadHours,
      maxGroupsPerRun,
      electionsScanned: 0,
      dueElectionCount: 0,
      dueGroupCount: 0,
      selectedGroupCount: 0,
      maxGroupsHit: false,
      enqueuedJobCount: 0,
      updatedJobCount: 0,
      skippedActiveJobCount: 0,
      skippedUnknownStateCount: 0,
      markerSkippedElectionCount: 0,
    };
  }

  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const queue = dryRun ? null : createElectionResultSearchQueue();
  const redis: RedisMarkerLike | null = dryRun ? null : createClient({ url: env.REDIS_URL });
  const horizon = addHours(now, lookaheadHours);
  const runId = `election_result_schedule_${now.toISOString()}`;

  try {
    if (redis) {
      await redis.connect();
    }
    const rows = await listPotentialElectionResultRows(pool, toIsoDate(horizon), now, maxGroupsPerRun * 100);
    const due = await buildDueGroups(pool, rows, now, horizon, redis);
    const selectedGroups = due.groups.slice(0, maxGroupsPerRun);
    const maxGroupsHit = due.groups.length > selectedGroups.length;

    let enqueuedJobCount = 0;
    let updatedJobCount = 0;
    let skippedActiveJobCount = 0;

    if (queue) {
      for (const group of selectedGroups) {
        const outcome = await upsertDelayedElectionResultSearchJob(queue, group, runId, now);
        if (outcome === "created") enqueuedJobCount += 1;
        if (outcome === "updated") updatedJobCount += 1;
        if (outcome === "skipped_active") skippedActiveJobCount += 1;
        if (outcome !== "skipped_active" && redis) {
          const emittedAt = now.toISOString();
          for (const electionId of group.electionIds) {
            await markElectionResultPassEmitted(redis, {
              electionId,
              passType: group.passType,
              emittedAt,
              ttlSeconds: emittedMarkerTtlSeconds({
                passType: group.passType,
                scheduledFor: group.scheduledFor,
                now,
              }),
            });
          }
        }
      }
    }

    return {
      enabled: true,
      forced: force,
      dryRun,
      now: now.toISOString(),
      lookaheadHours,
      maxGroupsPerRun,
      electionsScanned: rows.length,
      dueElectionCount: due.dueElectionCount,
      dueGroupCount: due.groups.length,
      selectedGroupCount: selectedGroups.length,
      maxGroupsHit,
      enqueuedJobCount,
      updatedJobCount,
      skippedActiveJobCount,
      skippedUnknownStateCount: due.skippedUnknownStateCount,
      markerSkippedElectionCount: due.markerSkippedElectionCount,
    };
  } finally {
    if (queue) {
      await queue.close();
    }
    if (redis?.isOpen) {
      await redis.quit();
    }
    await pool.end();
  }
}
