import { Pool } from "pg";
import { createClient } from "redis";

import { getPipelineEnv } from "../../config/env.js";
import { enqueueCandidateRosterDrafts } from "../candidates/candidateRosterDraftEmitter.js";
import {
  defaultOfficeCandidateEligibilityConfig,
  listOfficeCandidateEligibilityForUpcomingOffices,
  summarizeOfficeCandidateEligibilityReasons,
} from "../candidates/officeCandidateEligibility.js";

const ENABLE_CANDIDATE_ROSTER_DAILY_ROLLOVER_PRODUCER =
  process.env.CANDIDATE_ROSTER_ENABLE_DAILY_ROLLOVER_PRODUCER === "true";

type ProducerOptions = {
  force?: boolean;
};

export type CandidateRosterRolloverProducerResult = {
  enabled: boolean;
  asOfDate: string;
  eligibleRows: number;
  emittedRows: number;
  markerSkippedRows: number;
  alreadyWrittenRows: number;
  bufferBlockedRows: number;
  notNearestRows: number;
  tooFarFutureRows: number;
};

export async function runCandidateRosterRolloverProducer(
  options: ProducerOptions = {}
): Promise<CandidateRosterRolloverProducerResult> {
  const { force = false } = options;
  const enabled = force || ENABLE_CANDIDATE_ROSTER_DAILY_ROLLOVER_PRODUCER;
  const config = defaultOfficeCandidateEligibilityConfig();

  if (!enabled) {
    console.log(
      `candidate_roster rollover producer skipped: disabled by flag (as_of=${config.asOfDate})`
    );
    return {
      enabled: false,
      asOfDate: config.asOfDate,
      eligibleRows: 0,
      emittedRows: 0,
      markerSkippedRows: 0,
      alreadyWrittenRows: 0,
      bufferBlockedRows: 0,
      notNearestRows: 0,
      tooFarFutureRows: 0,
    };
  }

  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });

  try {
    await redis.connect();
    const rows = await listOfficeCandidateEligibilityForUpcomingOffices(pool, config);
    const counts = summarizeOfficeCandidateEligibilityReasons(rows);
    const eligibleIds = rows.filter((row) => row.reason === "eligible").map((row) => row.election_id);
    const enqueue = await enqueueCandidateRosterDrafts(
      redis,
      eligibleIds,
      `candidate_roster_rollover_${new Date().toISOString()}`
    );

    return {
      enabled: true,
      asOfDate: config.asOfDate,
      eligibleRows: eligibleIds.length,
      emittedRows: enqueue.emittedCount,
      markerSkippedRows: enqueue.skippedCount,
      alreadyWrittenRows: counts.already_written,
      bufferBlockedRows: counts.buffer_not_elapsed,
      notNearestRows: counts.not_nearest_in_track,
      tooFarFutureRows: counts.too_far_in_future,
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
