import type { Pool, PoolClient, QueryResultRow } from "pg";

import type { CompetitivenessLabel } from "./competitivenessLabels.js";
import type {
  CurrentRaceRatingConfidence,
  CurrentRaceRatingEvidenceStatus,
  CurrentRaceRatingMethod,
} from "./currentRaceRatingConsensus.js";

type Queryable = Pick<Pool | PoolClient, "query">;

// A rated row overrides history while its as_of is at most this many days old.
export const CURRENT_RACE_RATING_FRESH_DAYS = 60;
// A none_found row keeps the race off the due list for this many days.
export const CURRENT_RACE_RATING_NONE_FOUND_RETRY_DAYS = 30;

export type CurrentRaceRatingLookupRecord = {
  election_id: string;
  election_date: string;
  competitiveness_label: CompetitivenessLabel | null;
  method: CurrentRaceRatingMethod;
  confidence: CurrentRaceRatingConfidence | null;
  evidence_status: CurrentRaceRatingEvidenceStatus;
  as_of: string | null;
  decisive_round: string | null;
  evidence: unknown;
  source_url: string;
  researched_on: string;
};

type CurrentRaceRatingLookupRow = QueryResultRow & CurrentRaceRatingLookupRecord;

const ISO_DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

function parseUtcDate(value: string): number {
  if (!ISO_DATE_PATTERN.test(value)) {
    throw new Error(`Invalid current race rating date: ${value}`);
  }
  const parsed = Date.parse(`${value}T00:00:00.000Z`);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid current race rating date: ${value}`);
  }
  return parsed;
}

function utcDayFloor(value: Date): number {
  return Date.UTC(value.getUTCFullYear(), value.getUTCMonth(), value.getUTCDate());
}

function daysOld(dateValue: string, today: Date): number {
  return Math.floor((utcDayFloor(today) - parseUtcDate(dateValue)) / 86_400_000);
}

/**
 * Whether this row replaces the historical competitiveness label: rated with
 * high|medium confidence, the election still upcoming, and as_of within the
 * freshness window (fresh at exactly 60 days old, stale at 61).
 */
export function currentRaceRatingOverridesHistory(
  record: CurrentRaceRatingLookupRecord,
  today: Date = new Date()
): boolean {
  if (record.evidence_status !== "rated" || record.as_of === null) {
    return false;
  }
  if (record.confidence !== "high" && record.confidence !== "medium") {
    return false;
  }
  if (daysOld(record.election_date, today) > 0) {
    return false;
  }
  return daysOld(record.as_of, today) <= CURRENT_RACE_RATING_FRESH_DAYS;
}

/**
 * Whether this row keeps the election off the research due list: a rated row
 * within the freshness window, or a none_found row within the retry window.
 * Confidence and election date are irrelevant here — a fresh low-confidence
 * row still records that research already happened.
 */
export function currentRaceRatingBlocksResearch(
  record: CurrentRaceRatingLookupRecord,
  today: Date = new Date()
): boolean {
  if (record.evidence_status === "rated") {
    return record.as_of !== null && daysOld(record.as_of, today) <= CURRENT_RACE_RATING_FRESH_DAYS;
  }
  return daysOld(record.researched_on, today) <= CURRENT_RACE_RATING_NONE_FOUND_RETRY_DAYS;
}

export async function loadCurrentRaceRatings(
  db: Queryable,
  electionIds: readonly string[]
): Promise<Map<string, CurrentRaceRatingLookupRecord>> {
  const ids = [...new Set(electionIds.map((id) => id.trim()).filter((id) => id.length > 0))];
  if (ids.length === 0) {
    return new Map();
  }

  const result = await db.query<CurrentRaceRatingLookupRow>(
    `
      SELECT
        crr.election_id,
        e.election_date::text AS election_date,
        crr.competitiveness_label,
        crr.method,
        crr.confidence,
        crr.evidence_status,
        crr.as_of::text AS as_of,
        crr.decisive_round,
        crr.evidence,
        crr.source_url,
        crr.researched_at::date::text AS researched_on
      FROM public.current_race_ratings AS crr
      JOIN public.elections AS e ON e.id = crr.election_id
      WHERE crr.election_id = ANY($1::uuid[])
    `,
    [ids]
  );

  return new Map(result.rows.map((row) => [row.election_id, row]));
}

/**
 * Loads only the rows that override history (the read-path helper). Rows that
 * are stale, low-confidence, none_found, or for past elections are dropped.
 */
export async function loadOverridingCurrentRaceRatings(
  db: Queryable,
  electionIds: readonly string[],
  options: { today?: Date } = {}
): Promise<Map<string, CurrentRaceRatingLookupRecord>> {
  const today = options.today ?? new Date();
  const ratings = await loadCurrentRaceRatings(db, electionIds);
  return new Map(
    [...ratings].filter(([, record]) => currentRaceRatingOverridesHistory(record, today))
  );
}
