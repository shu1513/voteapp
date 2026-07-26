import type { Pool, PoolClient } from "pg";

import type { BallotLookupElectionSummary, BallotSummaryResult } from "./ballotLookup.js";
import {
  loadUserResearchAreaWeights,
  scoreResearchAreaMatch,
  type ResearchAreaMatchScore,
} from "../users/userResearchAreaScoring.js";

// ---------------------------------------------------------------------------
// Personalized ballot election ordering (sort modes, followed-candidates
// surfacing, and the "followed first" grouping).
//
// This feature is deliberately isolated as a decorator over the plain ballot
// summary reader: `lookupBallotSummariesByDistrictIds` knows nothing about it.
// To REMOVE the feature entirely, delete:
//   - this file (+ tests/pipeline/address/ballotElectionOrdering.test.ts)
//   - src/pipeline/users/userBallotPreferences.ts (+ its tests)
//   - db/migrations/144_add_user_ballot_preferences.sql (+ the table)
// and the blocks tagged [ballot-personalized-ordering] in:
//   - src/api/apiValidation.ts   (path const + two parsers)
//   - src/api/apiServer.ts       (prefs route, me/ballot merge, path lists)
//   - src/api/apiErrors.ts       (UserBallotPreferencesError mapping)
//   - src/api/addressApiTypes.ts (options/result types, prefs option fns)
//   - src/scripts/runAddressApiServer.ts (decorator composition, prefs wiring)
// The base endpoints then return the reader's natural date ordering again.
// ---------------------------------------------------------------------------

type Queryable = Pick<Pool | PoolClient, "query">;

// Ordering the caller wants for the elections list. `vote_power` (the default)
// sorts by the computed vote-power score descending; `soonest` sorts by
// election date ascending (the reader's natural order); `district_size` sorts
// by the election's district population descending (largest electorate first);
// `district_size_smallest` is the same key ascending. Unknown populations sort
// last in both directions. `my_areas` sorts by how strongly the election's
// research areas match the user's saved research-area preferences (summed
// weights, see userResearchAreaScoring.ts) and degrades to `vote_power` for
// anonymous callers and users with no saved areas.
// Keep this list in sync with the user_ballot_preferences sort CHECK
// constraint (db/migrations/152) and the frontend BALLOT_SORTS mirror.
export type BallotSummarySort = "vote_power" | "soonest" | "district_size" | "district_size_smallest" | "my_areas";

export const BALLOT_SUMMARY_SORTS: readonly BallotSummarySort[] = [
  "vote_power",
  "soonest",
  "district_size",
  "district_size_smallest",
  "my_areas",
];

export function isBallotSummarySort(value: unknown): value is BallotSummarySort {
  return typeof value === "string" && (BALLOT_SUMMARY_SORTS as readonly string[]).includes(value);
}

export type BallotSummaryOptions = {
  // When set, followed candidates are resolved for this user and attached to
  // each election. Anonymous lookups omit it and every election gets [].
  userId?: string | null;
  sort?: BallotSummarySort;
  // When true (the default), elections that contain at least one followed
  // candidate are grouped ahead of the rest, each group still ordered by
  // `sort`. A no-op for anonymous lookups, which never have follows.
  followedFirst?: boolean;
};

export type BallotFollowedCandidate = {
  candidate_id: string;
  display_name: string;
};

// Candidates this user follows who are running in this election. Always
// present on ordered results; empty for anonymous lookups or elections with
// no followed candidate. Drives the "followed first" grouping and FE chips.
export type OrderedBallotElectionSummary = BallotLookupElectionSummary & {
  followed_candidates: BallotFollowedCandidate[];
};

export type OrderedBallotSummaryResult = Omit<BallotSummaryResult, "elections"> & {
  elections: OrderedBallotElectionSummary[];
};

// Decorates a plain ballot summary result with followed-candidate annotations
// and applies the requested ordering. This is the single entry point the API
// wiring composes over the reader.
export async function applyBallotElectionOrdering(
  db: Queryable,
  result: BallotSummaryResult,
  options: BallotSummaryOptions = {}
): Promise<OrderedBallotSummaryResult> {
  // Independent queries keyed only on the user: load them in parallel so the
  // my_areas sort does not add a sequential round trip to the ballot path.
  const [followedByElection, weights] = await Promise.all([
    loadFollowedCandidatesByElection(
      db,
      options.userId ?? null,
      result.elections.map((election) => election.id)
    ),
    options.sort === "my_areas"
      ? loadUserResearchAreaWeights(db, options.userId ?? null)
      : Promise.resolve(null),
  ]);

  const elections: OrderedBallotElectionSummary[] = result.elections.map((election) => ({
    ...election,
    followed_candidates: followedByElection.get(election.id) ?? [],
  }));

  let sort = options.sort ?? "vote_power";
  let areaScoresByElection: Map<string, ResearchAreaMatchScore> | null = null;
  if (sort === "my_areas") {
    if (!weights || weights.size === 0) {
      // Anonymous caller or no saved areas: nothing to match against, so the
      // sort degrades to the default rather than erroring.
      sort = "vote_power";
    } else {
      areaScoresByElection = new Map(
        elections.map((election) => [
          election.id,
          scoreResearchAreaMatch(
            election.research_areas.map((area) => area.id),
            weights
          ),
        ])
      );
    }
  }

  sortBallotElections(elections, sort, options.followedFirst ?? true, areaScoresByElection);

  return { ...result, elections };
}

type FollowedCandidateRow = {
  election_id: string;
  candidate_id: string;
  display_name: string;
};

// Resolves, per election, the candidates this user follows who are actually on
// that ballot. Returns an empty map for anonymous callers so every election
// gets [] without a database round-trip.
async function loadFollowedCandidatesByElection(
  db: Queryable,
  userId: string | null,
  electionIds: readonly string[]
): Promise<Map<string, BallotFollowedCandidate[]>> {
  const followed = new Map<string, BallotFollowedCandidate[]>();
  const trimmedUserId = typeof userId === "string" ? userId.trim() : "";
  if (trimmedUserId.length === 0 || electionIds.length === 0) {
    return followed;
  }

  const result = await db.query<FollowedCandidateRow>(
    `
      SELECT
        ce.election_id,
        c.id AS candidate_id,
        COALESCE(NULLIF(trim(c.display_name), ''), trim(concat_ws(' ', c.first_name, c.last_name))) AS display_name
      FROM public.user_candidate_follows AS f
      JOIN public.candidate_elections AS ce
        ON ce.candidate_id = f.candidate_id
       AND ce.status NOT IN ('withdrawn', 'lost')
      JOIN public.candidates AS c
        ON c.id = f.candidate_id
       AND c.deleted_at IS NULL
       AND c.merged_into_candidate_id IS NULL
      WHERE f.user_id = $1
        AND ce.election_id = ANY($2::uuid[])
      ORDER BY ce.election_id, display_name, c.id
    `,
    [trimmedUserId, electionIds]
  );

  for (const row of result.rows) {
    const list = followed.get(row.election_id);
    const entry = { candidate_id: row.candidate_id, display_name: row.display_name };
    if (list) {
      list.push(entry);
    } else {
      followed.set(row.election_id, [entry]);
    }
  }
  return followed;
}

// Stable, in-place ordering of the elections list. `Array.prototype.sort` is
// stable, so the reader's SQL order (election_date, race_type, title, id) is
// the deterministic tiebreak whenever the chosen keys are equal.
function sortBallotElections(
  elections: OrderedBallotElectionSummary[],
  sort: BallotSummarySort,
  followedFirst: boolean,
  areaScoresByElection: Map<string, ResearchAreaMatchScore> | null = null
): void {
  elections.sort((a, b) => {
    if (followedFirst) {
      const aFollowed = a.followed_candidates.length > 0 ? 0 : 1;
      const bFollowed = b.followed_candidates.length > 0 ? 0 : 1;
      if (aFollowed !== bFollowed) {
        return aFollowed - bFollowed;
      }
    }
    return compareBySort(a, b, sort, areaScoresByElection);
  });
}

const NO_AREA_MATCH: ResearchAreaMatchScore = { score: 0, bestRank: Number.POSITIVE_INFINITY };

// Numeric-aware title compare so "Proposition 4" sorts before "Proposition 33"
// (plain string compare puts 33 first). Fixed "en" locale keeps the order
// deterministic across server environments.
const BALLOT_TITLE_COLLATOR = new Intl.Collator("en", { numeric: true });

function compareBySort(
  a: OrderedBallotElectionSummary,
  b: OrderedBallotElectionSummary,
  sort: BallotSummarySort,
  areaScoresByElection: Map<string, ResearchAreaMatchScore> | null = null
): number {
  if (sort === "my_areas") {
    // Higher summed weight of matched saved areas first; among equal sums the
    // election touching the user's best-ranked area wins; then the default
    // vote-power ordering decides.
    const aMatch = areaScoresByElection?.get(a.id) ?? NO_AREA_MATCH;
    const bMatch = areaScoresByElection?.get(b.id) ?? NO_AREA_MATCH;
    if (aMatch.score !== bMatch.score) {
      return bMatch.score - aMatch.score;
    }
    if (aMatch.bestRank !== bMatch.bestRank) {
      return aMatch.bestRank - bMatch.bestRank;
    }
    return compareBySort(a, b, "vote_power");
  }
  if (sort === "vote_power") {
    // Higher vote-power score first; unknown scores (null) sort last.
    const aScore = typeof a.vote_power.score === "number" ? a.vote_power.score : Number.NEGATIVE_INFINITY;
    const bScore = typeof b.vote_power.score === "number" ? b.vote_power.score : Number.NEGATIVE_INFINITY;
    if (aScore !== bScore) {
      return bScore - aScore;
    }
  }
  if (sort === "district_size" || sort === "district_size_smallest") {
    // district_size: larger population first; district_size_smallest: smaller
    // first. Unknown populations (null) sort last in both directions.
    const missing = sort === "district_size" ? Number.NEGATIVE_INFINITY : Number.POSITIVE_INFINITY;
    const aPopulation = typeof a.district.population === "number" ? a.district.population : missing;
    const bPopulation = typeof b.district.population === "number" ? b.district.population : missing;
    if (aPopulation !== bPopulation) {
      return sort === "district_size" ? bPopulation - aPopulation : aPopulation - bPopulation;
    }
  }
  // `soonest`, and the tiebreak for equal primary keys: earliest date first.
  if (a.election_date !== b.election_date) {
    return a.election_date < b.election_date ? -1 : 1;
  }
  if (a.race_type !== b.race_type) {
    return a.race_type < b.race_type ? -1 : 1;
  }
  if (a.official_ballot_title !== b.official_ballot_title) {
    const byTitle = BALLOT_TITLE_COLLATOR.compare(a.official_ballot_title, b.official_ballot_title);
    if (byTitle !== 0) {
      return byTitle;
    }
  }
  return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
}
