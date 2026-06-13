import type { PoolClient } from "pg";

type Queryable = Pick<PoolClient, "query">;

export type PresidentialCycleCandidateStatus = "active" | "withdrawn";

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
}): Promise<void> {
  await input.client.query(
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
    `,
    [input.candidateId, input.electionId, input.isIncumbent ?? false]
  );
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
