import type { PoolClient } from "pg";

export type CandidateRecordsSearchClaimQueryable = Pick<PoolClient, "query">;

export type CandidateRecordsClaimInput = {
  candidateId: string;
  asOf?: Date;
  cooldownDays?: number;
  leaseHours?: number;
  ignoreCooldown?: boolean;
};

export type CandidateRecordsClaimResult = {
  claimed: boolean;
  candidateId: string;
  lastRecordsSearchedAt: string | null;
  lastRecordsResearchedThrough: string | null;
};

const DEFAULT_COOLDOWN_DAYS = 30;
const DEFAULT_LEASE_HOURS = 2;

function toDateOnly(value: Date | string): string {
  if (value instanceof Date) {
    return value.toISOString().slice(0, 10);
  }
  const trimmed = value.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    return trimmed;
  }
  const parsed = new Date(trimmed);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Invalid researchedThrough date: ${value}`);
  }
  return parsed.toISOString().slice(0, 10);
}

export async function claimCandidateRecordsSearch(
  client: CandidateRecordsSearchClaimQueryable,
  input: CandidateRecordsClaimInput
): Promise<CandidateRecordsClaimResult> {
  const asOf = input.asOf ?? new Date();
  const cooldownDays = input.cooldownDays ?? DEFAULT_COOLDOWN_DAYS;
  const leaseHours = input.leaseHours ?? DEFAULT_LEASE_HOURS;
  const ignoreCooldown = input.ignoreCooldown === true;

  const result = await client.query<{
    id: string;
    last_records_searched_at: string | null;
    last_records_researched_through: string | null;
  }>(
    `
      UPDATE public.candidates
      SET records_search_claimed_at = $2::timestamptz,
          updated_at = now()
      WHERE id = $1
        AND deleted_at IS NULL
        AND (
          $5::boolean = true
          OR
          last_records_searched_at IS NULL
          OR last_records_searched_at < ($2::timestamptz - make_interval(days => $3::int))
        )
        AND (
          records_search_claimed_at IS NULL
          OR records_search_claimed_at < ($2::timestamptz - make_interval(hours => $4::int))
        )
      RETURNING
        id,
        last_records_searched_at::text,
        last_records_researched_through::text
    `,
    [input.candidateId, asOf.toISOString(), cooldownDays, leaseHours, ignoreCooldown]
  );

  const row = result.rows[0];
  if (!row) {
    return {
      claimed: false,
      candidateId: input.candidateId,
      lastRecordsSearchedAt: null,
      lastRecordsResearchedThrough: null,
    };
  }

  return {
    claimed: true,
    candidateId: row.id,
    lastRecordsSearchedAt: row.last_records_searched_at ?? null,
    lastRecordsResearchedThrough: row.last_records_researched_through ?? null,
  };
}

export async function markCandidateRecordsSearchCompleted(
  client: CandidateRecordsSearchClaimQueryable,
  candidateId: string,
  researchedThrough: Date | string
): Promise<void> {
  const researchedThroughDate = toDateOnly(researchedThrough);
  await client.query(
    `
      UPDATE public.candidates
      SET last_records_searched_at = now(),
          last_records_researched_through = $2::date,
          records_search_claimed_at = NULL,
          updated_at = now()
      WHERE id = $1
    `,
    [candidateId, researchedThroughDate]
  );
}

export async function releaseCandidateRecordsSearchClaim(
  client: CandidateRecordsSearchClaimQueryable,
  candidateId: string
): Promise<void> {
  await client.query(
    `
      UPDATE public.candidates
      SET records_search_claimed_at = NULL,
          updated_at = now()
      WHERE id = $1
    `,
    [candidateId]
  );
}
