import type { PoolClient } from "pg";

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
