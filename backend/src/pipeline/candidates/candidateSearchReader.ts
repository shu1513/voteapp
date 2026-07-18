import type { Pool, PoolClient } from "pg";

type Queryable = Pick<Pool | PoolClient, "query">;

export type CandidateSearchMatch = {
  candidate_id: string;
  display_name: string;
  party: string;
  state: string;
  current_office: string | null;
};

export type CandidateSearchResult = {
  candidates: CandidateSearchMatch[];
};

export const CANDIDATE_SEARCH_LIMIT = 10;

/** Literal text → safe ILIKE pattern fragment: the caller wraps it in its own
 * wildcards, so any %/_ (and the \ escape itself) in user input must match
 * literally instead of acting as metacharacters. */
export function escapeIlikePattern(text: string): string {
  return text.replace(/[\\%_]/g, "\\$&");
}

type CandidateSearchRow = {
  candidate_id: string;
  display_name: string;
  party: string;
  state: string;
  current_office: string | null;
};

/**
 * Name typeahead: case-insensitive substring match on the same coalesced
 * display name the candidate detail endpoint serves. Prefix matches sort
 * first so "Hilar" ranks "Hilary …" above "Ann Hilary …".
 */
export async function searchCandidatesByName(db: Queryable, query: string): Promise<CandidateSearchResult> {
  const trimmed = query.trim();
  if (trimmed.length === 0) {
    return { candidates: [] };
  }
  const escaped = escapeIlikePattern(trimmed);

  const result = await db.query<CandidateSearchRow>(
    `
      SELECT
        candidate.id::text AS candidate_id,
        COALESCE(
          NULLIF(trim(candidate.display_name), ''),
          trim(concat_ws(' ', candidate.first_name, candidate.last_name))
        ) AS display_name,
        candidate.party,
        candidate.state,
        candidate.current_office
      FROM public.candidates AS candidate
      WHERE candidate.deleted_at IS NULL
        AND candidate.merged_into_candidate_id IS NULL
        AND COALESCE(
          NULLIF(trim(candidate.display_name), ''),
          trim(concat_ws(' ', candidate.first_name, candidate.last_name))
        ) ILIKE '%' || $1 || '%'
      ORDER BY
        COALESCE(
          NULLIF(trim(candidate.display_name), ''),
          trim(concat_ws(' ', candidate.first_name, candidate.last_name))
        ) ILIKE $1 || '%' DESC,
        display_name ASC
      LIMIT $2
    `,
    [escaped, CANDIDATE_SEARCH_LIMIT]
  );

  return { candidates: result.rows };
}
