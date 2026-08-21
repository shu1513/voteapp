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

// Refused upserts read the stored row afterwards only to build a useful
// error message; the refusal itself already happened atomically in SQL.
async function refusalError(
  db: Queryable,
  record: CurrentRaceRatingRecord
): Promise<Error> {
  const existing = await db.query<ExistingRatingRow>(
    `
      SELECT evidence_status, as_of::text AS as_of
      FROM public.current_race_ratings
      WHERE election_id = $1
    `,
    [record.election_id]
  );
  const storedAsOf = existing.rows[0]?.as_of ?? "unknown";
  if (record.evidence_status === "none_found") {
    return new Error(
      `current race rating upsert refused for election ${record.election_id}: ` +
        `stored row is rated (as_of ${storedAsOf}) but payload is none_found; use --force to overwrite`
    );
  }
  return new Error(
    `current race rating upsert refused for election ${record.election_id}: ` +
      `payload as_of ${record.as_of} is older than stored as_of ${storedAsOf}; use --force to overwrite`
  );
}

async function upsertCurrentRaceRating(
  db: Queryable,
  record: CurrentRaceRatingRecord,
  researchedAt: Date,
  force: boolean
): Promise<number> {
  // One row per election, no history. The DO UPDATE WHERE guard makes the
  // refusal atomic — a stale payload cannot clobber a newer stored rating
  // and a none_found retry cannot erase a stored rating, even under
  // concurrent writers; both need --force. A guarded-out update reports
  // rowCount 0.
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
      WHERE $12::boolean
        OR current_race_ratings.evidence_status = 'none_found'
        OR (EXCLUDED.evidence_status = 'rated' AND EXCLUDED.as_of >= current_race_ratings.as_of)
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
      force,
    ]
  );
  if (result.rowCount === 0) {
    throw await refusalError(db, record);
  }
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
