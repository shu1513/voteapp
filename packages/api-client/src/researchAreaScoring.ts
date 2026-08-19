import type { CandidateRecord, ResearchAreaPreference } from "./types";

// Client mirror of backend/src/pipeline/users/userResearchAreaScoring.ts —
// the shared weighting for everything that scores content against the user's
// saved research areas. Keep the formula in sync: weight = 0.75^(rank - 1)
// (rank 1 → 1, 2 → 0.75, 3 → 0.5625 …), no cap on how many areas a user
// ranks; every weight is > 0, so any match beats no match regardless of
// rank. A selected-but-unranked area (legacy rows) weighs as if ranked just
// after the user's last ranked area.

export const RESEARCH_AREA_RANK_DECAY = 0.75;

export function researchAreaWeightForRank(rank: number): number {
  return RESEARCH_AREA_RANK_DECAY ** (rank - 1);
}

/**
 * Rank used for tiebreaks when a selected area has no explicit rank: sorts
 * after every explicit rank, before "no match" (NO_MATCH_BEST_RANK).
 */
export const UNRANKED_RESEARCH_AREA_RANK = Number.MAX_SAFE_INTEGER;
export const NO_MATCH_BEST_RANK = Number.POSITIVE_INFINITY;

export type ResearchAreaWeight = {
  weight: number;
  /** Explicit rank >= 1, or UNRANKED_RESEARCH_AREA_RANK when unranked. */
  rank: number;
};

/** Saved preferences → research_area_id → weight map for match scoring. */
export function buildResearchAreaWeights(
  preferences: readonly ResearchAreaPreference[]
): Map<string, ResearchAreaWeight> {
  const weights = new Map<string, ResearchAreaWeight>();
  // Unranked = one rank below the highest explicit rank (max, not count —
  // ranks need not be contiguous; mirrors loadUserResearchAreaWeights).
  const unrankedWeightRank =
    preferences.reduce((last, preference) => (preference.rank !== null && preference.rank > last ? preference.rank : last), 0) + 1;
  for (const preference of preferences) {
    weights.set(preference.research_area_id, {
      weight: researchAreaWeightForRank(preference.rank ?? unrankedWeightRank),
      rank: preference.rank ?? UNRANKED_RESEARCH_AREA_RANK,
    });
  }
  return weights;
}

export type RecordAreaStance = {
  research_area_id: string;
  slug: string;
  name: string;
  for_count: number;
  against_count: number;
};

/**
 * Aggregates a candidate's record tags into per-area for/against counts.
 * Stance-bearing tags only: a general (null-stance) tag marks relevance, not
 * a position, and is deliberately excluded from the chips. Output is sorted
 * by slug so rendering is deterministic.
 */
export function aggregateRecordAreaStances(records: readonly CandidateRecord[]): RecordAreaStance[] {
  const byArea = new Map<string, RecordAreaStance>();
  for (const record of records) {
    for (const tag of record.research_area_tags) {
      if (tag.stance !== "for" && tag.stance !== "against") {
        continue;
      }
      let entry = byArea.get(tag.research_area_id);
      if (!entry) {
        entry = {
          research_area_id: tag.research_area_id,
          slug: tag.slug,
          name: tag.name,
          for_count: 0,
          against_count: 0,
        };
        byArea.set(tag.research_area_id, entry);
      }
      if (tag.stance === "for") {
        entry.for_count += 1;
      } else {
        entry.against_count += 1;
      }
    }
  }
  return [...byArea.values()].sort((a, b) => (a.slug < b.slug ? -1 : a.slug > b.slug ? 1 : 0));
}

export type StanceRelevanceScore = {
  /** Sum of saved-area weights where the candidate has stance-bearing records. */
  score: number;
  /** Total stance-bearing records on saved areas; tiebreak under equal weights. */
  recordCount: number;
};

/**
 * Scores a candidate for the "My issues first" sort: unique matched areas
 * (weighted) dominate, record volume breaks ties. Relevance, not agreement —
 * the label is direction-neutral, so a candidate with only *against* records
 * on a saved issue still has a track record on it and must outrank one with
 * no relevant records at all (the chips carry the direction). Candidates
 * with no stance-bearing saved-area records score 0 and keep their
 * alphabetical order.
 */
export function scoreStanceRelevance(
  stances: readonly RecordAreaStance[],
  weights: Map<string, ResearchAreaWeight>
): StanceRelevanceScore {
  let score = 0;
  let recordCount = 0;
  for (const stance of stances) {
    const weight = weights.get(stance.research_area_id);
    if (!weight) {
      continue;
    }
    // Every aggregated stance has for_count + against_count >= 1, but keep
    // the guard so a hand-built empty stance can't buy weight.
    const count = stance.for_count + stance.against_count;
    if (count === 0) {
      continue;
    }
    score += weight.weight;
    recordCount += count;
  }
  return { score, recordCount };
}
