import { randomBytes } from "node:crypto";

import type { Pool, PoolClient } from "pg";

import { isUuid } from "../../utils/uuid.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type TransactionalDb = Pick<Pool, "connect">;
type TransactionClient = Pick<PoolClient, "query" | "release">;

export type UserPickCardShare = {
  token: string;
  election_date: string;
};

export type PublicPickCardEntry = {
  election_id: string;
  official_ballot_title: string;
  race_type: "office" | "ballot_measure";
  district_name: string;
  picks: { candidate_id: string; display_name: string; candidacy_status: string }[];
  measure_position: "yes" | "no" | null;
};

/** The public payload behind /picks/<token>. Everything here is public
 * information (races, candidate names, results); the only thing the token
 * gates is the ASSOCIATION of these picks with one anonymous voter. No user
 * identity fields, deliberately. */
export type PublicPickCard = {
  election_date: string;
  entries: PublicPickCardEntry[];
};

export type UserPickCardSharesErrorCode =
  | "invalid_user_id"
  | "invalid_election_date"
  | "user_not_found"
  | "no_picks_to_share";

export class UserPickCardSharesError extends Error {
  constructor(
    readonly code: UserPickCardSharesErrorCode,
    message: string
  ) {
    super(message);
    this.name = "UserPickCardSharesError";
  }
}

const ELECTION_DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

function normalizeUserId(userId: string): string {
  const normalized = userId.trim();
  if (!isUuid(normalized)) {
    throw new UserPickCardSharesError("invalid_user_id", "User ID must be a valid UUID");
  }
  return normalized;
}

function normalizeElectionDate(electionDate: string): string {
  const normalized = electionDate.trim();
  if (!ELECTION_DATE_PATTERN.test(normalized) || Number.isNaN(Date.parse(normalized))) {
    throw new UserPickCardSharesError("invalid_election_date", "election_date must be a valid YYYY-MM-DD date");
  }
  return normalized;
}

async function rollbackQuietly(client: TransactionClient): Promise<void> {
  try {
    await client.query("ROLLBACK");
  } catch {
    // Preserve the original failure.
  }
}

/**
 * Create (or return the existing) share link for one date's pick card.
 * Idempotent per (user, election_date): the first click mints a token, every
 * later click returns the same one, so a re-share never invalidates a link
 * already posted somewhere.
 */
export async function getOrCreateUserPickCardShare(
  db: TransactionalDb,
  userId: string,
  electionDate: string
): Promise<{ share: UserPickCardShare }> {
  const normalizedUserId = normalizeUserId(userId);
  const normalizedDate = normalizeElectionDate(electionDate);
  const client = await db.connect();

  try {
    await client.query("BEGIN");
    const user = await client.query<{ id: string }>(
      `
        SELECT id FROM public.users
        WHERE id = $1::uuid AND deleted_at IS NULL
        FOR UPDATE
      `,
      [normalizedUserId]
    );
    if (user.rows.length === 0) {
      throw new UserPickCardSharesError("user_not_found", "User not found");
    }

    // An empty card is not shareable: a public page with zero picks says
    // nothing, and minting tokens for every date a user glances at would
    // scatter live capability URLs nobody asked for.
    const anyPick = await client.query<{ one: number }>(
      `
        SELECT 1 AS one
        FROM public.user_election_choices AS choice
        JOIN public.elections AS election
          ON election.id = choice.election_id
        WHERE choice.user_id = $1::uuid
          AND election.election_date = $2::date
        LIMIT 1
      `,
      [normalizedUserId, normalizedDate]
    );
    if (anyPick.rows.length === 0) {
      throw new UserPickCardSharesError("no_picks_to_share", "No picks recorded for that election date");
    }

    // 256-bit random capability. ON CONFLICT keeps the FIRST token: a no-op
    // update lets RETURNING hand back the existing row instead of burning
    // the fresh token into it (which would silently break links already
    // shared).
    const token = randomBytes(32).toString("base64url");
    const saved = await client.query<{ token: string; election_date: string }>(
      `
        INSERT INTO public.user_pick_card_shares (user_id, election_date, token)
        VALUES ($1::uuid, $2::date, $3)
        ON CONFLICT (user_id, election_date)
        DO UPDATE SET user_id = user_pick_card_shares.user_id
        RETURNING token, election_date::text AS election_date
      `,
      [normalizedUserId, normalizedDate, token]
    );

    await client.query("COMMIT");
    const row = saved.rows[0];
    if (!row) {
      throw new Error("pick card share upsert returned no row");
    }
    return { share: { token: row.token, election_date: row.election_date } };
  } catch (error) {
    await rollbackQuietly(client);
    throw error;
  } finally {
    client.release();
  }
}

type PickCardRow = {
  election_date: string;
  election_id: string;
  official_ballot_title: string;
  race_type: "office" | "ballot_measure";
  district_name: string;
  candidate_id: string | null;
  display_name: string | null;
  candidacy_status: string | null;
  measure_position: "yes" | "no" | null;
};

/**
 * The live public view behind a share token. Null when the token matches no
 * share (or its owner was deleted) — the API turns that into a 404. Picks
 * whose candidate has since been deleted or merged drop out, mirroring the
 * owner's own list read (userElectionChoices).
 */
export async function lookupPublicPickCard(db: Queryable, token: string): Promise<PublicPickCard | null> {
  const trimmed = token.trim();
  if (trimmed.length === 0) {
    return null;
  }
  const result = await db.query<PickCardRow>(
    `
      SELECT
        share.election_date::text AS election_date,
        election.id::text AS election_id,
        election.official_ballot_title,
        election.race_type,
        district.name AS district_name,
        choice.candidate_id::text AS candidate_id,
        COALESCE(
          NULLIF(trim(candidate.display_name), ''),
          trim(concat_ws(' ', candidate.first_name, candidate.last_name))
        ) AS display_name,
        candidate_election.status AS candidacy_status,
        choice.measure_position
      FROM public.user_pick_card_shares AS share
      JOIN public.users AS user_row
        ON user_row.id = share.user_id
       AND user_row.deleted_at IS NULL
      JOIN public.user_election_choices AS choice
        ON choice.user_id = share.user_id
      JOIN public.elections AS election
        ON election.id = choice.election_id
       AND election.election_date = share.election_date
      JOIN public.districts AS district
        ON district.id = election.district_id
      LEFT JOIN public.candidates AS candidate
        ON candidate.id = choice.candidate_id
       AND candidate.deleted_at IS NULL
       AND candidate.merged_into_candidate_id IS NULL
      LEFT JOIN public.candidate_elections AS candidate_election
        ON candidate_election.candidate_id = choice.candidate_id
       AND candidate_election.election_id = choice.election_id
      WHERE share.token = $1
      ORDER BY election.official_ballot_title ASC, election.id ASC, choice.created_at ASC, choice.id ASC
    `,
    [trimmed]
  );
  if (result.rows.length === 0) {
    // Distinguish "no such share" from "share with zero surviving picks":
    // the latter still renders a (bare) card rather than a 404, so a link
    // someone posted keeps resolving even if its picks were since cleared.
    const share = await db.query<{ election_date: string }>(
      `
        SELECT share.election_date::text AS election_date
        FROM public.user_pick_card_shares AS share
        JOIN public.users AS user_row
          ON user_row.id = share.user_id
         AND user_row.deleted_at IS NULL
        WHERE share.token = $1
      `,
      [trimmed]
    );
    const bare = share.rows[0];
    return bare ? { election_date: bare.election_date, entries: [] } : null;
  }

  const byElection = new Map<string, PublicPickCardEntry>();
  for (const row of result.rows) {
    let entry = byElection.get(row.election_id);
    if (!entry) {
      entry = {
        election_id: row.election_id,
        official_ballot_title: row.official_ballot_title,
        race_type: row.race_type,
        district_name: row.district_name,
        picks: [],
        measure_position: null,
      };
      byElection.set(row.election_id, entry);
    }
    if (row.measure_position !== null) {
      entry.measure_position = row.measure_position;
    } else if (row.candidate_id && row.display_name && row.candidacy_status) {
      entry.picks.push({
        candidate_id: row.candidate_id,
        display_name: row.display_name,
        candidacy_status: row.candidacy_status,
      });
    }
  }
  return {
    election_date: result.rows[0].election_date,
    entries: [...byElection.values()].filter(
      (entry) => entry.picks.length > 0 || entry.measure_position !== null
    ),
  };
}
