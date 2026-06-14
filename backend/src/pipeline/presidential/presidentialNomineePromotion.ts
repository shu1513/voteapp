import type { Pool, PoolClient, QueryResult, QueryResultRow } from "pg";

import type { PresidentialNomineeResolutionResult } from "./presidentialNomineeResolver.js";
import { isUuid } from "../../utils/uuid.js";

type TransactionalDb = Pick<Pool, "connect">;
type TransactionClient = Pick<PoolClient, "query" | "release">;

type PrimaryCycleRow = {
  id: string;
  nominee_candidate_id: string | null;
};

type GeneralCycleRow = {
  id: string;
};

type PrimaryCycleCandidateRow = {
  running_mate_candidate_id: string | null;
};

export type PromotePresidentialNomineeErrorCode =
  | "invalid_input"
  | "primary_cycle_not_found"
  | "primary_cycle_already_has_different_nominee"
  | "nominee_not_active_primary_candidate"
  | "general_cycle_not_found";

export class PromotePresidentialNomineeError extends Error {
  constructor(
    readonly code: PromotePresidentialNomineeErrorCode,
    message: string
  ) {
    super(message);
    this.name = "PromotePresidentialNomineeError";
  }
}

export type PromotePresidentialNomineeResult =
  | {
      status: "promoted";
      primaryCycleId: string;
      generalCycleId: string;
      nomineeCandidateId: string;
      party: string;
      sources: string[];
    }
  | {
      status: "skipped";
      reason: "no_matched_nominee";
      resolutionStatus: Exclude<PresidentialNomineeResolutionResult["status"], "matched">;
    };

function normalizeUuid(value: string, fieldName: string): string {
  const normalized = value.trim();
  if (!isUuid(normalized)) {
    throw new PromotePresidentialNomineeError("invalid_input", `${fieldName} must be a valid UUID`);
  }
  return normalized;
}

function normalizeParty(value: string): string {
  const normalized = value.trim();
  if (normalized.length === 0) {
    throw new PromotePresidentialNomineeError("invalid_input", "presidential nominee party is required");
  }
  return normalized;
}

function assertElectionYear(electionYear: number): void {
  if (!Number.isInteger(electionYear) || electionYear < 2000 || electionYear > 2100 || electionYear % 4 !== 0) {
    throw new PromotePresidentialNomineeError(
      "invalid_input",
      `Invalid presidential nominee election year: ${electionYear}`
    );
  }
}

function assertValidDate(value: Date): void {
  if (!(value instanceof Date) || Number.isNaN(value.getTime())) {
    throw new PromotePresidentialNomineeError("invalid_input", "Invalid presidential nominee confirmed_at");
  }
}

function normalizeSources(values: readonly string[]): string[] {
  const normalized: string[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    const trimmed = value.trim();
    if (trimmed.length === 0 || seen.has(trimmed)) {
      continue;
    }
    seen.add(trimmed);
    normalized.push(trimmed);
  }
  return normalized;
}

async function rollbackQuietly(client: TransactionClient): Promise<void> {
  try {
    await client.query("ROLLBACK");
  } catch {
    // Preserve the original error.
  }
}

async function query<T extends QueryResultRow = QueryResultRow>(
  client: TransactionClient,
  text: string,
  values?: unknown[]
): Promise<QueryResult<T>> {
  return client.query<T>(text, values);
}

export async function promoteMatchedPresidentialNominee(input: {
  db: TransactionalDb;
  primaryCycleId: string;
  electionYear: number;
  party: string;
  nomineeCandidateId: string;
  sources: readonly string[];
  confirmedAt?: Date;
}): Promise<Extract<PromotePresidentialNomineeResult, { status: "promoted" }>> {
  const primaryCycleId = normalizeUuid(input.primaryCycleId, "presidential primary cycle id");
  const nomineeCandidateId = normalizeUuid(input.nomineeCandidateId, "presidential nominee candidate id");
  const party = normalizeParty(input.party);
  assertElectionYear(input.electionYear);
  const confirmedAt = input.confirmedAt ?? new Date();
  assertValidDate(confirmedAt);
  const sources = normalizeSources(input.sources);

  const client = await input.db.connect();
  try {
    await client.query("BEGIN");

    const primaryCycle = await query<PrimaryCycleRow>(
      client,
      `
        SELECT id, nominee_candidate_id
        FROM public.presidential_cycles
        WHERE id = $1::uuid
          AND election_year = $2::int
          AND stage = 'primary'
          AND party = $3
        FOR UPDATE
      `,
      [primaryCycleId, input.electionYear, party]
    );
    const primary = primaryCycle.rows[0];
    if (!primary) {
      throw new PromotePresidentialNomineeError(
        "primary_cycle_not_found",
        `Presidential primary cycle not found for ${input.electionYear} ${party}`
      );
    }
    if (
      primary.nominee_candidate_id &&
      primary.nominee_candidate_id.toLowerCase() !== nomineeCandidateId.toLowerCase()
    ) {
      throw new PromotePresidentialNomineeError(
        "primary_cycle_already_has_different_nominee",
        "Presidential primary cycle already has a different nominee"
      );
    }

    const primaryCandidate = await query(
      client,
      `
        SELECT running_mate_candidate_id
        FROM public.presidential_cycle_candidates
        WHERE cycle_id = $1::uuid
          AND candidate_id = $2::uuid
          AND status = 'active'
        FOR UPDATE
      `,
      [primaryCycleId, nomineeCandidateId]
    );
    const primaryCandidateRow = primaryCandidate.rows[0] as PrimaryCycleCandidateRow | undefined;
    if (!primaryCandidateRow) {
      throw new PromotePresidentialNomineeError(
        "nominee_not_active_primary_candidate",
        "Nominee candidate is not an active candidate in the primary cycle"
      );
    }

    const generalCycle = await query<GeneralCycleRow>(
      client,
      `
        SELECT id
        FROM public.presidential_cycles
        WHERE election_year = $1::int
          AND stage = 'general'
          AND party IS NULL
        FOR UPDATE
      `,
      [input.electionYear]
    );
    const general = generalCycle.rows[0];
    if (!general) {
      throw new PromotePresidentialNomineeError(
        "general_cycle_not_found",
        `Presidential general cycle not found for ${input.electionYear}`
      );
    }

    await client.query(
      `
        UPDATE public.presidential_cycles
        SET status = 'completed',
            nominee_candidate_id = $2::uuid,
            nominee_confirmed_at = COALESCE(nominee_confirmed_at, $3::timestamptz),
            nominee_sources = $4::jsonb,
            updated_at = now()
        WHERE id = $1::uuid
      `,
      [primaryCycleId, nomineeCandidateId, confirmedAt.toISOString(), JSON.stringify(sources)]
    );

    await client.query(
      `
        INSERT INTO public.presidential_cycle_candidates (
          cycle_id,
          candidate_id,
          party,
          running_mate_candidate_id,
          status,
          sources
        )
        VALUES ($1::uuid, $2::uuid, $3, $4::uuid, 'active', $5::jsonb)
        ON CONFLICT (cycle_id, candidate_id) DO UPDATE
        SET party = EXCLUDED.party,
            running_mate_profile_researched = CASE
              WHEN public.presidential_cycle_candidates.running_mate_candidate_id IS NOT DISTINCT FROM EXCLUDED.running_mate_candidate_id
                THEN public.presidential_cycle_candidates.running_mate_profile_researched
              ELSE false
            END,
            running_mate_candidate_id = EXCLUDED.running_mate_candidate_id,
            status = 'active',
            sources = EXCLUDED.sources,
            updated_at = now()
      `,
      [
        general.id,
        nomineeCandidateId,
        party,
        primaryCandidateRow.running_mate_candidate_id,
        JSON.stringify(sources),
      ]
    );

    await client.query("COMMIT");
    return {
      status: "promoted",
      primaryCycleId,
      generalCycleId: general.id,
      nomineeCandidateId,
      party,
      sources,
    };
  } catch (error) {
    await rollbackQuietly(client);
    throw error;
  } finally {
    client.release();
  }
}

export async function promotePresidentialNomineeFromResolution(input: {
  db: TransactionalDb;
  primaryCycleId: string;
  electionYear: number;
  party: string;
  resolution: PresidentialNomineeResolutionResult;
  confirmedAt?: Date;
}): Promise<PromotePresidentialNomineeResult> {
  if (input.resolution.status !== "matched") {
    return {
      status: "skipped",
      reason: "no_matched_nominee",
      resolutionStatus: input.resolution.status,
    };
  }

  return promoteMatchedPresidentialNominee({
    db: input.db,
    primaryCycleId: input.primaryCycleId,
    electionYear: input.electionYear,
    party: input.party,
    nomineeCandidateId: input.resolution.candidateId,
    sources: input.resolution.sources,
    confirmedAt: input.confirmedAt,
  });
}
