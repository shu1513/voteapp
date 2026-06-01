import { Pool } from "pg";
import { createClient } from "redis";

import { getPipelineEnv } from "../config/env.js";
import { enqueueCandidateRecordDrafts } from "../pipeline/candidates/candidateRecordDraftEmitter.js";
import { runCandidateRecordEnricher } from "../pipeline/enrichers/candidateRecordEnricher.js";

type CandidateElectionPair = {
  candidateId: string;
  electionId: string;
};

type CandidateSnapshot = {
  recordsCount: number;
  tagsCount: number;
  lastRecordsSearchedAt: string | null;
  lastRecordsResearchedThrough: string | null;
};

function parseFlagValue(name: string): string | null {
  const prefix = `--${name}=`;
  const token = process.argv.find((part) => part.startsWith(prefix));
  if (!token) {
    return null;
  }
  return token.slice(prefix.length).trim() || null;
}

function parsePositiveFlag(name: string, fallback: number): number {
  const value = parseFlagValue(name);
  if (!value) {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

async function findCandidateElectionPair(pool: Pool): Promise<CandidateElectionPair | null> {
  const result = await pool.query<{
    candidate_id: string;
    election_id: string;
  }>(
    `
      SELECT ce.candidate_id, ce.election_id
      FROM public.candidate_elections ce
      JOIN public.candidates c
        ON c.id = ce.candidate_id
      JOIN public.elections e
        ON e.id = ce.election_id
      WHERE c.deleted_at IS NULL
        AND e.race_type = 'office'
        AND e.office_id IS NOT NULL
      ORDER BY ce.created_at DESC, ce.id DESC
      LIMIT 1
    `
  );

  const row = result.rows[0];
  if (!row) {
    return null;
  }
  return {
    candidateId: row.candidate_id,
    electionId: row.election_id,
  };
}

async function loadSnapshot(pool: Pool, candidateId: string): Promise<CandidateSnapshot> {
  const candidateResult = await pool.query<{
    last_records_searched_at: string | null;
    last_records_researched_through: string | null;
  }>(
    `
      SELECT
        last_records_searched_at::text,
        last_records_researched_through::text
      FROM public.candidates
      WHERE id = $1
      LIMIT 1
    `,
    [candidateId]
  );

  const recordsCountResult = await pool.query<{ count: string }>(
    `
      SELECT COUNT(*)::text AS count
      FROM public.candidate_records
      WHERE candidate_id = $1
    `,
    [candidateId]
  );

  const tagsCountResult = await pool.query<{ count: string }>(
    `
      SELECT COUNT(*)::text AS count
      FROM public.candidate_record_area_tags t
      JOIN public.candidate_records r
        ON r.id = t.candidate_record_id
      WHERE r.candidate_id = $1
    `,
    [candidateId]
  );

  return {
    recordsCount: Number.parseInt(recordsCountResult.rows[0]?.count ?? "0", 10),
    tagsCount: Number.parseInt(tagsCountResult.rows[0]?.count ?? "0", 10),
    lastRecordsSearchedAt: candidateResult.rows[0]?.last_records_searched_at ?? null,
    lastRecordsResearchedThrough: candidateResult.rows[0]?.last_records_researched_through ?? null,
  };
}

function toTimestamp(value: string | null): number | null {
  if (!value) {
    return null;
  }
  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? null : parsed;
}

async function main(): Promise<void> {
  const env = getPipelineEnv();
  const pool = new Pool({ connectionString: env.DATABASE_URL });
  const redis = createClient({ url: env.REDIS_URL });

  try {
    const candidateIdFlag = parseFlagValue("candidate-id");
    const electionIdFlag = parseFlagValue("election-id");
    const loops = parsePositiveFlag("loops", 3);
    const batchSize = parsePositiveFlag("batch-size", 50);

    const pair =
      candidateIdFlag && electionIdFlag
        ? { candidateId: candidateIdFlag, electionId: electionIdFlag }
        : await findCandidateElectionPair(pool);

    if (!pair) {
      throw new Error("No candidate/election office pair found for live pipeline smoke");
    }

    const startedAt = new Date();
    const before = await loadSnapshot(pool, pair.candidateId);

    await redis.connect();
    const enqueue = await enqueueCandidateRecordDrafts(redis, [
      {
        candidateId: pair.candidateId,
        electionId: pair.electionId,
        runId: `candidate_record_live_pipeline_smoke_${startedAt.toISOString()}`,
      },
    ]);

    let completedInWindow = false;
    for (let i = 0; i < loops; i += 1) {
      await runCandidateRecordEnricher({ once: true, batchSize, blockMs: 2000 });
      const snapshot = await loadSnapshot(pool, pair.candidateId);
      const lastSearchedAt = toTimestamp(snapshot.lastRecordsSearchedAt);
      if (lastSearchedAt !== null && lastSearchedAt >= startedAt.getTime()) {
        completedInWindow = true;
        break;
      }
    }

    const after = await loadSnapshot(pool, pair.candidateId);

    const output = {
      type: "candidate_record_live_pipeline_smoke",
      ts: new Date().toISOString(),
      candidate_id: pair.candidateId,
      election_id: pair.electionId,
      started_at: startedAt.toISOString(),
      enqueue,
      completed_in_window: completedInWindow,
      before,
      after,
      deltas: {
        records: after.recordsCount - before.recordsCount,
        tags: after.tagsCount - before.tagsCount,
      },
    };

    console.log(JSON.stringify(output, null, 2));
  } finally {
    try {
      await redis.quit();
    } catch {
      // no-op
    }
    await pool.end();
  }
}

main().catch((error) => {
  console.error("candidate record live pipeline smoke failed:", error);
  process.exit(1);
});
