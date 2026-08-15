import type { ResearchAreaWeight } from "./researchAreaScoring";
import { UNRANKED_RESEARCH_AREA_RANK } from "./researchAreaScoring";

// The detail rail's client-side re-sort of its nav-state snapshot. Mirrors
// backend/src/pipeline/address/ballotElectionOrdering.ts compareBySort the
// way researchAreaScoring mirrors userResearchAreaScoring: same primary
// keys, same null handling, same date → race_type → numeric-title → id
// tiebreak tail, and the awaiting-candidates tail stays sunk under every
// sort (the backend's hasNothingToRead sink). district_size sorts are NOT
// offered — district population never reaches the client. Followed-first
// grouping is deliberately not applied either: the rail is an organizing
// view, and the authoritative order returns from the backend the moment
// the reader navigates back to the list.

export type RailSortKey = "my_areas" | "vote_power" | "soonest" | "alphabetical";

/** The snapshot fields the rail sorts on — a NavContest subset. */
export type RailSortEntry = {
  id: string;
  title: string;
  race_type?: string;
  vote_power_score?: number | null;
  election_date?: string;
  research_area_ids?: string[];
  awaiting_candidates?: boolean;
};

/** Rail order, matching the list pages' labels where the sort is the same
 * one. alphabetical is rail-only (the list has no A–Z sort). */
export const RAIL_SORTS: readonly { value: RailSortKey; label: string }[] = [
  { value: "my_areas", label: "My issues" },
  { value: "vote_power", label: "My vote impact" },
  { value: "soonest", label: "Soonest first" },
  { value: "alphabetical", label: "A–Z" },
];

// Same collator as the backend's BALLOT_TITLE_COLLATOR: numeric-aware so
// "Proposition 4" sorts before "Proposition 33", fixed "en" locale for
// deterministic order everywhere.
const TITLE_COLLATOR = new Intl.Collator("en", { numeric: true });

/**
 * Which rail sorts the snapshot can honor. Every entry must carry the sort
 * keys (an old history entry predates them — no keys, no sort control);
 * my_areas additionally needs the viewer to have saved research areas and
 * every entry's area ids. Empty result = hide the control.
 */
export function railSortsOffered(entries: RailSortEntry[], hasSavedAreas: boolean): RailSortKey[] {
  if (entries.length < 2) {
    return [];
  }
  const keyed = entries.every(
    (entry) => entry.vote_power_score !== undefined && entry.election_date !== undefined
  );
  if (!keyed) {
    return [];
  }
  const myAreas =
    hasSavedAreas && entries.every((entry) => entry.research_area_ids !== undefined);
  return RAIL_SORTS.map((option) => option.value).filter(
    (value) => value !== "my_areas" || myAreas
  );
}

type AreaMatch = { score: number; bestRank: number };

const NO_MATCH: AreaMatch = { score: 0, bestRank: UNRANKED_RESEARCH_AREA_RANK + 1 };

// Mirror of the backend's scoreResearchAreaMatch: summed weights of matched
// unique areas, best (lowest) matched rank for the tiebreak.
function scoreAreaMatch(
  areaIds: readonly string[] | undefined,
  weights: Map<string, ResearchAreaWeight>
): AreaMatch {
  if (!areaIds) {
    return NO_MATCH;
  }
  let score = 0;
  let bestRank = NO_MATCH.bestRank;
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

function compareVotePower(a: RailSortEntry, b: RailSortEntry): number {
  // Higher score first; unknown scores (null/absent) sort last.
  const aScore = typeof a.vote_power_score === "number" ? a.vote_power_score : Number.NEGATIVE_INFINITY;
  const bScore = typeof b.vote_power_score === "number" ? b.vote_power_score : Number.NEGATIVE_INFINITY;
  return bScore - aScore;
}

// The backend's shared tiebreak tail: earliest date first, then race_type,
// then numeric-aware title, then id.
function compareTail(a: RailSortEntry, b: RailSortEntry): number {
  const aDate = a.election_date ?? "";
  const bDate = b.election_date ?? "";
  if (aDate !== bDate) {
    return aDate < bDate ? -1 : 1;
  }
  const aType = a.race_type ?? "";
  const bType = b.race_type ?? "";
  if (aType !== bType) {
    return aType < bType ? -1 : 1;
  }
  const byTitle = TITLE_COLLATOR.compare(a.title, b.title);
  if (byTitle !== 0) {
    return byTitle;
  }
  return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
}

/**
 * Returns a new array in the requested order; the input is not mutated.
 * `weights` (the viewer's saved research areas) is only read by my_areas —
 * with no weights the my_areas order degrades to vote_power, matching the
 * backend's degradation for viewers without saved areas.
 */
export function sortRailEntries<Entry extends RailSortEntry>(
  entries: readonly Entry[],
  sort: RailSortKey,
  weights?: Map<string, ResearchAreaWeight>
): Entry[] {
  const areaWeights = weights ?? new Map<string, ResearchAreaWeight>();
  return [...entries].sort((a, b) => {
    // The awaiting-candidates sink outranks every sort key, as on the list.
    const aAwaiting = a.awaiting_candidates ? 1 : 0;
    const bAwaiting = b.awaiting_candidates ? 1 : 0;
    if (aAwaiting !== bAwaiting) {
      return aAwaiting - bAwaiting;
    }
    if (sort === "alphabetical") {
      const byTitle = TITLE_COLLATOR.compare(a.title, b.title);
      if (byTitle !== 0) {
        return byTitle;
      }
      return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
    }
    if (sort === "my_areas") {
      const aMatch = scoreAreaMatch(a.research_area_ids, areaWeights);
      const bMatch = scoreAreaMatch(b.research_area_ids, areaWeights);
      if (aMatch.score !== bMatch.score) {
        return bMatch.score - aMatch.score;
      }
      if (aMatch.bestRank !== bMatch.bestRank) {
        return aMatch.bestRank - bMatch.bestRank;
      }
      const byPower = compareVotePower(a, b);
      if (byPower !== 0) {
        return byPower;
      }
      return compareTail(a, b);
    }
    if (sort === "vote_power") {
      const byPower = compareVotePower(a, b);
      if (byPower !== 0) {
        return byPower;
      }
    }
    // soonest, and the tail for equal primary keys.
    return compareTail(a, b);
  });
}
