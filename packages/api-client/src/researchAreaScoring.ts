import type { CandidateRecord, ResearchAreaPreference } from "./types";

// Client mirror of backend/src/pipeline/users/userResearchAreaScoring.ts —
// the shared weighting for everything that scores content against the user's
// saved research areas. Keep the formula in sync: weight = 8 - rank (rank 1
// → 7 … rank 7 → 1); a selected-but-unranked area weighs 1, so any match
// beats no match regardless of rank.

export const MAX_RESEARCH_AREA_RANK = 7;

/** Rank used for tiebreaks when a selected area has no explicit rank. */
export const UNRANKED_RESEARCH_AREA_RANK = MAX_RESEARCH_AREA_RANK + 1;

export function researchAreaWeightForRank(rank: number | null): number {
  if (rank === null) {
    return 1;
  }
  return MAX_RESEARCH_AREA_RANK + 1 - rank;
}

export type ResearchAreaWeight = {
  weight: number;
  /** Explicit rank 1..7, or UNRANKED_RESEARCH_AREA_RANK when unranked. */
  rank: number;
};

/** Saved preferences → research_area_id → weight map for match scoring. */
export function buildResearchAreaWeights(
  preferences: readonly ResearchAreaPreference[]
): Map<string, ResearchAreaWeight> {
  const weights = new Map<string, ResearchAreaWeight>();
  for (const preference of preferences) {
    weights.set(preference.research_area_id, {
      weight: researchAreaWeightForRank(preference.rank),
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
