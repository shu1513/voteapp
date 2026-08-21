import type { Pool, PoolClient } from "pg";

import type { CompetitivenessLabel } from "./competitivenessLabels.js";
import type {
  CurrentRaceRatingConfidence,
  CurrentRaceRatingEvidenceStatus,
  CurrentRaceRatingMethod,
} from "./currentRaceRatingConsensus.js";
import { CURRENT_RACE_RATING_SCHEMA_VERSION } from "./currentRaceRatingConsensus.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type CurrentRaceRatingRecord = {
  election_id: string;
  method: CurrentRaceRatingMethod;
  evidence_status: CurrentRaceRatingEvidenceStatus;
  competitiveness_label: CompetitivenessLabel | null;
  confidence: CurrentRaceRatingConfidence | null;
  as_of: string | null;
  decisive_round: string | null;
  evidence: Record<string, unknown>;
  source_url: string;
};

export type CurrentRaceRatingWriteResult = {
  requested: number;
  rowsWritten: number;
};

type ExistingRatingRow = {
  evidence_status: CurrentRaceRatingEvidenceStatus;
  as_of: string | null;
};

// One row per election, no history. A stale payload must not clobber a newer
// stored rating, and a none_found retry must not erase a stored rating —
// both need --force.
function assertUpsertAllowed(record: CurrentRaceRatingRecord, existing: ExistingRatingRow): void {
  if (existing.evidence_status !== "rated") {
    return;
  }
  if (record.evidence_status === "none_found") {
    throw new Error(
      `current race rating upsert refused for election ${record.election_id}: ` +
        `stored row is rated (as_of ${existing.as_of}) but payload is none_found; use --force to overwrite`
    );
  }
  if (record.as_of !== null && existing.as_of !== null && record.as_of < existing.as_of) {
    throw new Error(
      `current race rating upsert refused for election ${record.election_id}: ` +
        `payload as_of ${record.as_of} is older than stored as_of ${existing.as_of}; use --force to overwrite`
    );
  }
}

async function upsertCurrentRaceRating(
  db: Queryable,
  record: CurrentRaceRatingRecord,
  researchedAt: Date,
  force: boolean
): Promise<number> {
  if (!force) {
    const existing = await db.query<ExistingRatingRow>(
      `
        SELECT evidence_status, as_of::text AS as_of
        FROM public.current_race_ratings
        WHERE election_id = $1
      `,
      [record.election_id]
    );
    const existingRow = existing.rows[0];
    if (existingRow) {
      assertUpsertAllowed(record, existingRow);
    }
  }

  const result = await db.query(
    `
      INSERT INTO public.current_race_ratings (
        election_id,
        schema_version,
        competitiveness_label,
        method,
        confidence,
        evidence_status,
        as_of,
        decisive_round,
        evidence,
        source_url,
        researched_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11::timestamptz)
      ON CONFLICT (election_id)
      DO UPDATE SET
        schema_version = EXCLUDED.schema_version,
        competitiveness_label = EXCLUDED.competitiveness_label,
        method = EXCLUDED.method,
        confidence = EXCLUDED.confidence,
        evidence_status = EXCLUDED.evidence_status,
        as_of = EXCLUDED.as_of,
        decisive_round = EXCLUDED.decisive_round,
        evidence = EXCLUDED.evidence,
        source_url = EXCLUDED.source_url,
        researched_at = EXCLUDED.researched_at,
        updated_at = now()
    `,
    [
      record.election_id,
      CURRENT_RACE_RATING_SCHEMA_VERSION,
      record.competitiveness_label,
      record.method,
      record.confidence,
      record.evidence_status,
      record.as_of,
      record.decisive_round,
      JSON.stringify(record.evidence),
      record.source_url,
      researchedAt.toISOString(),
    ]
  );
  if (result.rowCount !== 1) {
    throw new Error(
      `current race rating upsert expected to write exactly one row, wrote ${result.rowCount ?? 0}: ` +
        record.election_id
    );
  }
  return result.rowCount;
}

export async function upsertCurrentRaceRatings(
  db: Queryable,
  records: readonly CurrentRaceRatingRecord[],
  options: { researchedAt?: Date; force?: boolean } = {}
): Promise<CurrentRaceRatingWriteResult> {
  const researchedAt = options.researchedAt ?? new Date();
  const force = options.force ?? false;
  let rowsWritten = 0;

  for (const record of records) {
    rowsWritten += await upsertCurrentRaceRating(db, record, researchedAt, force);
  }

  return {
    requested: records.length,
    rowsWritten,
  };
}
