import type { Pool, PoolClient } from "pg";

import { MAX_USER_RESEARCH_AREA_PREFERENCES } from "../../constants/userResearchAreaPreferences.js";
import { isUuid } from "../../utils/uuid.js";

type Queryable = Pick<Pool | PoolClient, "query">;

// Shared weighting for everything that scores content against a user's saved
// research areas (ballot election sort server-side; candidate and record
// sorts client-side — the frontend mirrors this formula in its own lib, the
// same way it mirrors BALLOT_SORTS).
//
// weight = 8 - rank  (rank 1 → 7 … rank 7 → 1); a selected-but-unranked area
// weighs 1, the same as rank 7. A weight is always >= 1, so any match beats
// no match regardless of rank.

/** Rank used for tiebreaks when a selected area has no explicit rank. */
export const UNRANKED_RESEARCH_AREA_RANK = MAX_USER_RESEARCH_AREA_PREFERENCES + 1;

export function researchAreaWeightForRank(rank: number | null): number {
  if (rank === null) {
    return 1;
  }
  return MAX_USER_RESEARCH_AREA_PREFERENCES + 1 - rank;
}

export type UserResearchAreaWeight = {
  weight: number;
  /** Explicit rank 1..7, or UNRANKED_RESEARCH_AREA_RANK when unranked. */
  rank: number;
};

export type UserResearchAreaWeights = Map<string, UserResearchAreaWeight>;

type PreferenceWeightRow = {
  research_area_id: string;
  rank: number | string | null;
};

/**
 * Loads the user's saved research areas as a research_area_id → weight map.
 * Anonymous callers (null/empty userId) and users with no saved areas get an
 * empty map, which callers treat as "no personalization available".
 */
export async function loadUserResearchAreaWeights(
  db: Queryable,
  userId: string | null | undefined
): Promise<UserResearchAreaWeights> {
  const weights: UserResearchAreaWeights = new Map();
  const trimmedUserId = typeof userId === "string" ? userId.trim() : "";
  if (!isUuid(trimmedUserId)) {
    return weights;
  }

  const result = await db.query<PreferenceWeightRow>(
    `
      SELECT preference.research_area_id, preference.rank
      FROM public.user_research_area_preferences AS preference
      JOIN public.users AS u
        ON u.id = preference.user_id
       AND u.deleted_at IS NULL
      WHERE preference.user_id = $1::uuid
    `,
    [trimmedUserId]
  );

  for (const row of result.rows) {
    const rank =
      row.rank === null ? null : typeof row.rank === "number" ? row.rank : Number.parseInt(row.rank, 10);
    const normalizedRank = rank !== null && Number.isFinite(rank) ? rank : null;
    weights.set(row.research_area_id, {
      weight: researchAreaWeightForRank(normalizedRank),
      rank: normalizedRank ?? UNRANKED_RESEARCH_AREA_RANK,
    });
  }
  return weights;
}

export type ResearchAreaMatchScore = {
  /** Sum of matched-area weights; 0 when nothing matches. */
  score: number;
  /** Best (lowest) rank among matched areas; UNRANKED_RESEARCH_AREA_RANK + 1 when nothing matches. */
  bestRank: number;
};

export const NO_MATCH_BEST_RANK = UNRANKED_RESEARCH_AREA_RANK + 1;

/**
 * Scores a piece of content (an election, a candidate, a record) by the
 * research areas attached to it. Duplicate area ids in the input count once.
 */
export function scoreResearchAreaMatch(
  areaIds: Iterable<string>,
  weights: UserResearchAreaWeights
): ResearchAreaMatchScore {
  let score = 0;
  let bestRank = NO_MATCH_BEST_RANK;
  const seen = new Set<string>();
  for (const areaId of areaIds) {
    if (seen.has(areaId)) {
      continue;
    }
    seen.add(areaId);
    const entry = weights.get(areaId);
    if (!entry) {
      continue;
    }
    score += entry.weight;
    if (entry.rank < bestRank) {
      bestRank = entry.rank;
    }
  }
  return { score, bestRank };
}
