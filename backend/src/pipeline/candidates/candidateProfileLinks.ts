import type { PoolClient } from "pg";

type Queryable = Pick<PoolClient, "query">;

export type PresidentialCycleCandidateStatus = "active" | "withdrawn";

export type CandidateElectionUpsertResult = {
  created: boolean;
};

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function normalizeSources(values: readonly string[] | undefined): string[] {
  const normalized: string[] = [];
  const seen = new Set<string>();
  for (const value of values ?? []) {
    const trimmed = value.trim();
    if (trimmed.length === 0 || seen.has(trimmed)) {
      continue;
    }
    seen.add(trimmed);
    normalized.push(trimmed);
  }
  return normalized;
}

export async function upsertCandidateElection(input: {
  client: PoolClient;
  candidateId: string;
  electionId: string;
  isIncumbent: boolean | undefined;
}): Promise<CandidateElectionUpsertResult> {
  const result = await input.client.query<{ created: boolean }>(
    `
      INSERT INTO public.candidate_elections (
        candidate_id,
        election_id,
        is_incumbent,
        status
      )
      VALUES ($1, $2, $3, 'declared')
      ON CONFLICT (candidate_id, election_id) DO UPDATE
      SET is_incumbent = EXCLUDED.is_incumbent,
          status = EXCLUDED.status,
          updated_at = now()
      RETURNING (xmax = 0) AS created
    `,
    [input.candidateId, input.electionId, input.isIncumbent ?? false]
  );

  return { created: Boolean(result.rows[0]?.created) };
}

export async function setCandidateElectionRunningMate(input: {
  db: Queryable;
  electionId: string;
  candidateId: string;
  runningMateCandidateId: string;
}): Promise<{ updatedCount: number }> {
  const electionId = requireNonEmpty(input.electionId, "election id");
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const runningMateCandidateId = requireNonEmpty(input.runningMateCandidateId, "running mate candidate id");
  if (candidateId === runningMateCandidateId) {
    throw new Error("running mate candidate id must differ from the ticket lead candidate id");
  }

  const result = await input.db.query(
    `
      UPDATE public.candidate_elections
      SET running_mate_candidate_id = $3::uuid,
          updated_at = now()
      WHERE election_id = $1
        AND candidate_id = $2
        AND running_mate_candidate_id IS DISTINCT FROM $3::uuid
    `,
    [electionId, candidateId, runningMateCandidateId]
  );

  return { updatedCount: result.rowCount ?? 0 };
}

export async function findTicketLeadCandidateIdByDisplayName(input: {
  db: Queryable;
  electionId: string;
  leadDisplayName: string;
}): Promise<{ ok: true; candidateId: string } | { ok: false; reason: "not_found" | "ambiguous" }> {
  const electionId = requireNonEmpty(input.electionId, "election id");
  const leadDisplayName = requireNonEmpty(input.leadDisplayName, "ticket lead display name");

  const result = await input.db.query<{ candidate_id: string }>(
    `
      SELECT ce.candidate_id
      FROM public.candidate_elections AS ce
      JOIN public.candidates AS c
        ON c.id = ce.candidate_id
      WHERE ce.election_id = $1
        AND c.deleted_at IS NULL
        AND (
          lower(trim(coalesce(NULLIF(c.display_name, ''), c.first_name || ' ' || c.last_name))) = lower(trim($2))
          OR lower(trim(c.first_name || ' ' || c.last_name)) = lower(trim($2))
          OR (
            position(',' in $2) > 0
            AND lower(trim(c.first_name || ' ' || c.last_name)) = lower(trim(split_part($2, ',', 2) || ' ' || split_part($2, ',', 1)))
          )
        )
    `,
    [electionId, leadDisplayName]
  );

  if (result.rows.length === 0) {
    return { ok: false, reason: "not_found" };
  }
  if (result.rows.length > 1) {
    return { ok: false, reason: "ambiguous" };
  }
  return { ok: true, candidateId: result.rows[0]!.candidate_id };
}

export async function upsertPresidentialCycleCandidate(input: {
  client: PoolClient;
  cycleId: string;
  candidateId: string;
  party: string;
  sources?: readonly string[];
  status?: PresidentialCycleCandidateStatus;
}): Promise<void> {
  const cycleId = requireNonEmpty(input.cycleId, "presidential cycle id");
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const party = requireNonEmpty(input.party, "presidential cycle candidate party");
  const status = input.status ?? "active";
  const sources = normalizeSources(input.sources);

  await input.client.query(
    `
      INSERT INTO public.presidential_cycle_candidates (
        cycle_id,
        candidate_id,
        party,
        status,
        sources
      )
      VALUES ($1, $2, $3, $4, $5::jsonb)
      ON CONFLICT (cycle_id, candidate_id) DO UPDATE
      SET party = EXCLUDED.party,
          status = EXCLUDED.status,
          sources = EXCLUDED.sources,
          updated_at = now()
    `,
    [cycleId, candidateId, party, status, JSON.stringify(sources)]
  );
}

export async function markPresidentialCycleCandidateProfileResearched(input: {
  db: Queryable;
  cycleId: string;
  candidateId: string;
}): Promise<{ updatedCount: number }> {
  const cycleId = requireNonEmpty(input.cycleId, "presidential cycle id");
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");

  const result = await input.db.query(
    `
      UPDATE public.presidential_cycle_candidates
      SET presidential_profile_researched = true,
          updated_at = now()
      WHERE cycle_id = $1
        AND candidate_id = $2
        AND presidential_profile_researched = false
    `,
    [cycleId, candidateId]
  );

  return { updatedCount: result.rowCount ?? 0 };
}

export async function findPresidentialCycleCandidateIdByFecId(input: {
  db: Queryable;
  cycleId: string;
  fecCandidateId: string;
}): Promise<string | null> {
  const cycleId = requireNonEmpty(input.cycleId, "presidential cycle id");
  const fecCandidateId = requireNonEmpty(input.fecCandidateId, "presidential FEC candidate id").toUpperCase();

  const result = await input.db.query<{ candidate_id: string }>(
    `
      SELECT cycle_candidate.candidate_id
      FROM public.presidential_cycle_candidates AS cycle_candidate
      JOIN public.candidates AS candidate
        ON candidate.id = cycle_candidate.candidate_id
      WHERE cycle_candidate.cycle_id = $1
        AND candidate.deleted_at IS NULL
        AND EXISTS (
          SELECT 1
          FROM jsonb_array_elements_text(candidate.fec_ids) AS fec_id(value)
          WHERE upper(trim(fec_id.value)) = $2
        )
      LIMIT 1
    `,
    [cycleId, fecCandidateId]
  );

  return result.rows[0]?.candidate_id ?? null;
}

export async function setPresidentialCycleCandidateRunningMate(input: {
  db: Queryable;
  cycleId: string;
  candidateId: string;
  runningMateCandidateId: string;
}): Promise<{ updatedCount: number }> {
  const cycleId = requireNonEmpty(input.cycleId, "presidential cycle id");
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const runningMateCandidateId = requireNonEmpty(input.runningMateCandidateId, "running mate candidate id");

  const result = await input.db.query(
    `
      UPDATE public.presidential_cycle_candidates
      SET running_mate_profile_researched = false,
          running_mate_candidate_id = $3::uuid,
          updated_at = now()
      WHERE cycle_id = $1
        AND candidate_id = $2
        AND running_mate_candidate_id IS DISTINCT FROM $3::uuid
    `,
    [cycleId, candidateId, runningMateCandidateId]
  );

  return { updatedCount: result.rowCount ?? 0 };
}

export async function markPresidentialCycleCandidateRunningMateProfileResearched(input: {
  db: Queryable;
  cycleId: string;
  candidateId: string;
  runningMateCandidateId: string;
}): Promise<{ updatedCount: number }> {
  const cycleId = requireNonEmpty(input.cycleId, "presidential cycle id");
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");
  const runningMateCandidateId = requireNonEmpty(input.runningMateCandidateId, "running mate candidate id");

  const result = await input.db.query(
    `
      UPDATE public.presidential_cycle_candidates
      SET running_mate_profile_researched = true,
          updated_at = now()
      WHERE cycle_id = $1
        AND candidate_id = $2
        AND running_mate_candidate_id = $3::uuid
        AND running_mate_profile_researched = false
    `,
    [cycleId, candidateId, runningMateCandidateId]
  );

  return { updatedCount: result.rowCount ?? 0 };
}

export async function withdrawPresidentialCycleCandidateByFecId(input: {
  db: Queryable;
  cycleId: string;
  fecCandidateId: string;
}): Promise<{ updatedCount: number }> {
  const cycleId = requireNonEmpty(input.cycleId, "presidential cycle id");
  const fecCandidateId = requireNonEmpty(input.fecCandidateId, "presidential FEC candidate id").toUpperCase();

  const result = await input.db.query(
    `
      UPDATE public.presidential_cycle_candidates AS cycle_candidate
      SET status = 'withdrawn',
          updated_at = now()
      FROM public.candidates AS candidate
      WHERE cycle_candidate.candidate_id = candidate.id
        AND cycle_candidate.cycle_id = $1
        AND cycle_candidate.status <> 'withdrawn'
        AND candidate.deleted_at IS NULL
        AND EXISTS (
          SELECT 1
          FROM jsonb_array_elements_text(candidate.fec_ids) AS fec_id(value)
          WHERE upper(trim(fec_id.value)) = $2
        )
    `,
    [cycleId, fecCandidateId]
  );

  return { updatedCount: result.rowCount ?? 0 };
}

export async function withdrawPresidentialCycleCandidateByCandidateId(input: {
  db: Queryable;
  cycleId: string;
  candidateId: string;
}): Promise<{ updatedCount: number }> {
  const cycleId = requireNonEmpty(input.cycleId, "presidential cycle id");
  const candidateId = requireNonEmpty(input.candidateId, "candidate id");

  const result = await input.db.query(
    `
      UPDATE public.presidential_cycle_candidates
      SET status = 'withdrawn',
          updated_at = now()
      WHERE cycle_id = $1
        AND candidate_id = $2
        AND status <> 'withdrawn'
    `,
    [cycleId, candidateId]
  );

  return { updatedCount: result.rowCount ?? 0 };
}
