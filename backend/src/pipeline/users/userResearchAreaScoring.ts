import type { Pool, PoolClient } from "pg";

import { isUuid } from "../../utils/uuid.js";

type Queryable = Pick<Pool | PoolClient, "query">;

// Shared weighting for everything that scores content against a user's saved
// research areas (ballot election sort and chatbot ranker server-side;
// candidate and record sorts client-side — the frontend mirrors this formula
// in packages/api-client/src/researchAreaScoring.ts, the same way it mirrors
// BALLOT_SORTS). Also the weight auto-pick uses per ranked issue.
//
// weight = 0.75^(rank - 1): rank 1 → 1, 2 → 0.75, 3 → 0.5625, 5 → 0.32,
// 7 → 0.18, 10 → 0.075. Geometric decay so the top issue dominates however
// many issues are ranked (there is no cap on the count). Every weight is > 0,
// so any match still beats no match. Results are exact binary fractions up to
// rank 35, so equal sums compare equal (ties are real ties).
//
// A selected-but-unranked area (legacy rows only; the editors always send
// ranks 1..n) weighs as if ranked just after the user's last ranked area —
// see loadUserResearchAreaWeights.

export const RESEARCH_AREA_RANK_DECAY = 0.75;

export function researchAreaWeightForRank(rank: number): number {
  return RESEARCH_AREA_RANK_DECAY ** (rank - 1);
}

/**
 * Rank used for tiebreaks when a selected area has no explicit rank: sorts
 * after every explicit rank, before "no match" (NO_MATCH_BEST_RANK).
 */
export const UNRANKED_RESEARCH_AREA_RANK = Number.MAX_SAFE_INTEGER;

export type UserResearchAreaWeight = {
  weight: number;
  /** Explicit rank >= 1, or UNRANKED_RESEARCH_AREA_RANK when unranked. */
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

  const ranks = result.rows.map((row) => {
    const rank =
      row.rank === null ? null : typeof row.rank === "number" ? row.rank : Number.parseInt(row.rank, 10);
    return rank !== null && Number.isFinite(rank) ? rank : null;
  });
  // Unranked areas weigh as one rank below the last ranked one.
  const unrankedWeightRank = ranks.filter((rank) => rank !== null).length + 1;
  result.rows.forEach((row, index) => {
    const rank = ranks[index] ?? null;
    weights.set(row.research_area_id, {
      weight: researchAreaWeightForRank(rank ?? unrankedWeightRank),
      rank: rank ?? UNRANKED_RESEARCH_AREA_RANK,
    });
  });
  return weights;
}

export type ResearchAreaMatchScore = {
  /** Sum of matched-area weights; 0 when nothing matches. */
  score: number;
  /** Best (lowest) rank among matched areas; NO_MATCH_BEST_RANK when nothing matches. */
  bestRank: number;
};

export const NO_MATCH_BEST_RANK = Number.POSITIVE_INFINITY;

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
