import type { Pool, PoolClient } from "pg";

export type Queryable = Pick<Pool | PoolClient, "query">;

export type ActivePresidentialCycleCandidateForReconciliation = {
  candidateId: string;
  displayName: string;
  party: string;
  fecIds: string[];
  sources: string[];
};

type ActivePresidentialCycleCandidateRow = {
  candidate_id: string;
  display_name: string | null;
  first_name: string;
  last_name: string;
  party: string;
  fec_ids: unknown;
  cycle_sources: unknown;
};

function requireNonEmpty(value: string, fieldName: string): string {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${fieldName} is required`);
  }
  return trimmed;
}

function parseStringArray(raw: unknown): string[] {
  if (!Array.isArray(raw)) {
    return [];
  }
  return raw
    .filter((item): item is string => typeof item === "string")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
}

function normalizeFecIds(raw: unknown): string[] {
  const normalized: string[] = [];
  const seen = new Set<string>();
  for (const value of parseStringArray(raw)) {
    const fecId = value.toUpperCase();
    if (!/^P\d{8}$/.test(fecId) || seen.has(fecId)) {
      continue;
    }
    seen.add(fecId);
    normalized.push(fecId);
  }
  return normalized;
}

function normalizeSources(raw: unknown): string[] {
  const normalized: string[] = [];
  const seen = new Set<string>();
  for (const value of parseStringArray(raw)) {
    if (seen.has(value)) {
      continue;
    }
    seen.add(value);
    normalized.push(value);
  }
  return normalized;
}

function displayNameForRow(row: ActivePresidentialCycleCandidateRow): string {
  const storedDisplayName = row.display_name?.trim();
  if (storedDisplayName) {
    return storedDisplayName;
  }
  return `${row.first_name} ${row.last_name}`.replace(/\s+/g, " ").trim();
}

export async function loadActivePresidentialCycleCandidatesForReconciliation(
  db: Queryable,
  cycleId: string
): Promise<ActivePresidentialCycleCandidateForReconciliation[]> {
  const normalizedCycleId = requireNonEmpty(cycleId, "presidential cycle id");
  const result = await db.query<ActivePresidentialCycleCandidateRow>(
    `
      SELECT
        candidate.id AS candidate_id,
        candidate.display_name,
        candidate.first_name,
        candidate.last_name,
        cycle_candidate.party,
        candidate.fec_ids,
        cycle_candidate.sources AS cycle_sources
      FROM public.presidential_cycle_candidates AS cycle_candidate
      JOIN public.candidates AS candidate
        ON candidate.id = cycle_candidate.candidate_id
      WHERE cycle_candidate.cycle_id = $1
        AND cycle_candidate.status = 'active'
        AND candidate.deleted_at IS NULL
      ORDER BY lower(COALESCE(NULLIF(trim(candidate.display_name), ''), trim(candidate.first_name || ' ' || candidate.last_name))) ASC
    `,
    [normalizedCycleId]
  );

  const rows: ActivePresidentialCycleCandidateForReconciliation[] = [];
  for (const row of result.rows) {
    const fecIds = normalizeFecIds(row.fec_ids);
    if (fecIds.length === 0) {
      continue;
    }
    const displayName = displayNameForRow(row);
    if (displayName.length === 0) {
      continue;
    }
    rows.push({
      candidateId: row.candidate_id,
      displayName,
      party: row.party.trim(),
      fecIds,
      sources: normalizeSources(row.cycle_sources),
    });
  }

  return rows;
}
