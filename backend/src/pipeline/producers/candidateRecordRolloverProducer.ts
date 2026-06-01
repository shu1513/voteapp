import { Pool } from "pg";
import { createClient } from "redis";

import { getPipelineEnv } from "../../config/env.js";
import { enqueueCandidateRecordDrafts } from "../candidates/candidateRecordDraftEmitter.js";

type CandidateRecordRolloverRow = {
  candidate_id: string;
  election_id: string;
};

type ProducerOptions = {
  force?: boolean;
};

export type CandidateRecordRolloverProducerResult = {
  enabled: boolean;
  forced: boolean;
  asOfDate: string;
  cooldownDays: number;
  maxEnqueuePerRun: number;
  dueRows: number;
  selectedRows: number;
  maxEnqueueHit: boolean;
  emittedRows: number;
  markerSkippedRows: number;
};

const ENABLE_CANDIDATE_RECORD_DAILY_ROLLOVER_PRODUCER =
  process.env.CANDIDATE_RECORD_ENABLE_DAILY_ROLLOVER_PRODUCER === "true";

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

function readRolloverPolicy(): { asOfDate: string; cooldownDays: number; maxEnqueuePerRun: number } {
  const now = new Date();
  return {
    asOfDate: now.toISOString().slice(0, 10),
    cooldownDays: parsePositiveIntegerEnv("CANDIDATE_RECORDS_SEARCH_COOLDOWN_DAYS", 30),
    maxEnqueuePerRun: parsePositiveIntegerEnv("CANDIDATE_RECORDS_ROLLOVER_MAX_ENQUEUE", 2000),
  };
}

async function listDueCandidateRecordRows(
  pool: Pool,
  asOfIso: string,
  cooldownDays: number,
  maxEnqueuePerRun: number,
  force: boolean
): Promise<{ rows: CandidateRecordRolloverRow[]; totalDueRows: number }> {
  const result = await pool.query<{
    candidate_id: string;
    election_id: string;
    total_due_rows: string;
  }>(
    `
      WITH candidate_office_context AS (
        SELECT
          ce.candidate_id,
          ce.election_id,
          c.last_records_searched_at,
          e.election_date,
          row_number() OVER (
            PARTITION BY ce.candidate_id
            ORDER BY e.election_date DESC, ce.created_at DESC, ce.id DESC
          ) AS candidate_rank
        FROM public.candidate_elections AS ce
        JOIN public.candidates AS c
          ON c.id = ce.candidate_id
        JOIN public.elections AS e
          ON e.id = ce.election_id
        WHERE c.deleted_at IS NULL
          AND e.race_type = 'office'
          AND (
            $4::boolean
            OR c.last_records_searched_at IS NULL
            OR c.last_records_searched_at < ($1::date - make_interval(days => $2::int))
          )
      ),
      due AS (
        SELECT
          candidate_id,
          election_id,
          last_records_searched_at,
          COUNT(*) OVER () AS total_due_rows
        FROM candidate_office_context
        WHERE candidate_rank = 1
      )
      SELECT
        candidate_id,
        election_id,
        total_due_rows::text
      FROM due
      ORDER BY last_records_searched_at ASC NULLS FIRST, candidate_id ASC
      LIMIT $3::int
    `,
    [asOfIso, cooldownDays, maxEnqueuePerRun, force]
  );

  const totalDueRows =
    result.rows.length === 0 ? 0 : Number.parseInt(result.rows[0]!.total_due_rows, 10);

  return {
    rows: result.rows.map((row) => ({
      candidate_id: row.candidate_id,
      election_id: row.election_id,
    })),
    totalDueRows: Number.isFinite(totalDueRows) ? totalDueRows : 0,
  };
}

export async function runCandidateRecordRolloverProducer(
  options: ProducerOptions = {}
): Promise<CandidateRecordRolloverProducerResult> {
  const { force = false } = options;
  const enabled = force || ENABLE_CANDIDATE_RECORD_DAILY_ROLLOVER_PRODUCER;
  const policy = readRolloverPolicy();

  if (!enabled) {
    console.log(
      `candidate_record rollover producer skipped: disabled by flag (as_of=${policy.asOfDate})`
    );
    return {
      enabled: false,
      forced: force,
      asOfDate: policy.asOfDate,
      cooldownDays: policy.cooldownDays,
      maxEnqueuePerRun: policy.maxEnqueuePerRun,
      dueRows: 0,
      selectedRows: 0,
      maxEnqueueHit: false,
      emittedRows: 0,
      markerSkippedRows: 0,
    };
  }

  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });

  try {
    await redis.connect();

    const due = await listDueCandidateRecordRows(
      pool,
      policy.asOfDate,
      policy.cooldownDays,
      policy.maxEnqueuePerRun,
      force
    );

    const enqueue = await enqueueCandidateRecordDrafts(
      redis,
      due.rows.map((row) => ({
        candidateId: row.candidate_id,
        electionId: row.election_id,
        runId: `candidate_record_rollover_${new Date().toISOString()}`,
      }))
    );

    const selectedRows = due.rows.length;
    const maxEnqueueHit = due.totalDueRows > selectedRows;

    return {
      enabled: true,
      forced: force,
      asOfDate: policy.asOfDate,
      cooldownDays: policy.cooldownDays,
      maxEnqueuePerRun: policy.maxEnqueuePerRun,
      dueRows: due.totalDueRows,
      selectedRows,
      maxEnqueueHit,
      emittedRows: enqueue.emittedCount,
      markerSkippedRows: enqueue.skippedCount,
    };
  } finally {
    try {
      await redis.quit();
    } catch {
      // no-op
    }
    await pool.end();
  }
}
