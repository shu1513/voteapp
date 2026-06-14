import type { Pool, PoolClient } from "pg";

import type { PresidentialNomineePayload } from "../../contracts/presidentialNomineePayloadContract.js";
import { normalizeCandidateName } from "../../utils/candidateIdentity.js";

export type Queryable = Pick<Pool | PoolClient, "query">;

export type PresidentialNomineeCandidateForResolution = {
  candidateId: string;
  displayName: string;
  party: string;
  fecIds: string[];
};

export type PresidentialNomineeMatchMethod = "exact_fec_id" | "exact_name";

export type PresidentialNomineeResolutionResult =
  | {
      status: "no_nominee_found";
      sources: string[];
    }
  | {
      status: "matched";
      candidateId: string;
      displayName: string;
      method: PresidentialNomineeMatchMethod;
      candidateName: string;
      fecCandidateId?: string;
      sources: string[];
    }
  | {
      status: "unmatched";
      reason: string;
      candidateName: string;
      fecCandidateId?: string;
      sources: string[];
    }
  | {
      status: "ambiguous";
      reason: string;
      candidateName: string;
      fecCandidateId?: string;
      candidates: PresidentialNomineeCandidateForResolution[];
      sources: string[];
    };

type PresidentialNomineeCandidateRow = {
  candidate_id: string;
  display_name: string | null;
  first_name: string;
  last_name: string;
  party: string;
  fec_ids: unknown;
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

function normalizeOptionalFecId(value: string | undefined): string | undefined {
  const normalized = value?.trim().toUpperCase();
  return normalized && /^P\d{8}$/.test(normalized) ? normalized : undefined;
}

function displayNameForRow(row: PresidentialNomineeCandidateRow): string {
  const storedDisplayName = row.display_name?.trim();
  if (storedDisplayName) {
    return storedDisplayName;
  }
  return `${row.first_name} ${row.last_name}`.replace(/\s+/g, " ").trim();
}

function candidatesWithFecId(
  candidates: readonly PresidentialNomineeCandidateForResolution[],
  fecCandidateId: string
): PresidentialNomineeCandidateForResolution[] {
  return candidates.filter((candidate) => candidate.fecIds.includes(fecCandidateId));
}

function candidatesWithExactName(
  candidates: readonly PresidentialNomineeCandidateForResolution[],
  candidateName: string
): PresidentialNomineeCandidateForResolution[] {
  const normalizedName = normalizeCandidateName(candidateName);
  if (normalizedName.length === 0) {
    return [];
  }
  return candidates.filter((candidate) => normalizeCandidateName(candidate.displayName) === normalizedName);
}

export async function loadActivePresidentialCycleCandidatesForNomineeResolution(
  db: Queryable,
  cycleId: string
): Promise<PresidentialNomineeCandidateForResolution[]> {
  const normalizedCycleId = requireNonEmpty(cycleId, "presidential cycle id");
  const result = await db.query<PresidentialNomineeCandidateRow>(
    `
      SELECT
        candidate.id AS candidate_id,
        candidate.display_name,
        candidate.first_name,
        candidate.last_name,
        cycle_candidate.party,
        candidate.fec_ids
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

  const candidates: PresidentialNomineeCandidateForResolution[] = [];
  for (const row of result.rows) {
    const displayName = displayNameForRow(row);
    if (displayName.length === 0) {
      continue;
    }
    candidates.push({
      candidateId: row.candidate_id,
      displayName,
      party: row.party.trim(),
      fecIds: normalizeFecIds(row.fec_ids),
    });
  }

  return candidates;
}

export function resolvePresidentialNomineeCandidate(input: {
  payload: PresidentialNomineePayload;
  candidates: readonly PresidentialNomineeCandidateForResolution[];
}): PresidentialNomineeResolutionResult {
  if (!input.payload.nominee_found) {
    return {
      status: "no_nominee_found",
      sources: input.payload.sources,
    };
  }

  const candidateName = input.payload.candidate_name.trim();
  const fecCandidateId = normalizeOptionalFecId(input.payload.fec_candidate_id);

  if (fecCandidateId) {
    const fecMatches = candidatesWithFecId(input.candidates, fecCandidateId);
    if (fecMatches.length === 1) {
      const candidate = fecMatches[0]!;
      return {
        status: "matched",
        candidateId: candidate.candidateId,
        displayName: candidate.displayName,
        method: "exact_fec_id",
        candidateName,
        fecCandidateId,
        sources: input.payload.sources,
      };
    }
    if (fecMatches.length > 1) {
      return {
        status: "ambiguous",
        reason: "multiple active cycle candidates share the nominee FEC ID",
        candidateName,
        fecCandidateId,
        candidates: fecMatches,
        sources: input.payload.sources,
      };
    }
  }

  const nameMatches = candidatesWithExactName(input.candidates, candidateName);
  if (nameMatches.length === 1) {
    const candidate = nameMatches[0]!;
    return {
      status: "matched",
      candidateId: candidate.candidateId,
      displayName: candidate.displayName,
      method: "exact_name",
      candidateName,
      ...(fecCandidateId ? { fecCandidateId } : {}),
      sources: input.payload.sources,
    };
  }
  if (nameMatches.length > 1) {
    return {
      status: "ambiguous",
      reason: "multiple active cycle candidates match the nominee name",
      candidateName,
      ...(fecCandidateId ? { fecCandidateId } : {}),
      candidates: nameMatches,
      sources: input.payload.sources,
    };
  }

  return {
    status: "unmatched",
    reason: fecCandidateId
      ? "nominee FEC ID and nominee name did not match any active cycle candidate"
      : "nominee name did not match any active cycle candidate",
    candidateName,
    ...(fecCandidateId ? { fecCandidateId } : {}),
    sources: input.payload.sources,
  };
}
