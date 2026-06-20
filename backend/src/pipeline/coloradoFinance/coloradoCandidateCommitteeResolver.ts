import type { ColoradoTracerContributionRow } from "./coloradoTracerContributionReader.js";

export type ColoradoCandidateCommitteeResolution =
  | {
      status: "matched";
      committeeId: string;
      committeeName: string;
      sourceUrl: string | null;
    }
  | {
      status: "unmatched" | "ambiguous";
      reason: string;
    };

type CandidateCommitteeMatch = {
  committeeId: string;
  committeeName: string;
};

function normalizeTextKey(value: string): string {
  return value
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeColoradoCandidateNameKeys(value: string): Set<string> {
  const keys = new Set<string>();
  const trimmed = value.trim();
  const direct = normalizeTextKey(trimmed);
  if (direct) {
    keys.add(direct);
  }

  const commaParts = trimmed.split(",");
  if (commaParts.length >= 2) {
    const lastName = commaParts[0]?.trim() ?? "";
    const firstNames = commaParts.slice(1).join(" ").trim();
    const flipped = normalizeTextKey(`${firstNames} ${lastName}`);
    if (flipped) {
      keys.add(flipped);
    }
  }

  return keys;
}

function parseColoradoTracerDateYear(raw: string): number | null {
  const trimmed = raw.trim();
  if (trimmed.length === 0) {
    return null;
  }
  const slashMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})\b/.exec(trimmed);
  if (slashMatch) {
    return Number(slashMatch[3]);
  }
  const isoMatch = /^(\d{4})-\d{2}-\d{2}/.exec(trimmed);
  if (isoMatch) {
    return Number(isoMatch[1]);
  }
  return null;
}

function isElectionCycleContribution(row: ColoradoTracerContributionRow, electionYear: number): boolean {
  const year = parseColoradoTracerDateYear(row.ContributionDate);
  return year !== null && year >= electionYear - 1 && year <= electionYear;
}

function isCandidateCommittee(row: ColoradoTracerContributionRow): boolean {
  return normalizeTextKey(row.CommitteeType).includes("CANDIDATE");
}

function rowMatchesCandidateName(input: {
  row: ColoradoTracerContributionRow;
  candidateNameKeys: ReadonlySet<string>;
}): boolean {
  for (const key of normalizeColoradoCandidateNameKeys(input.row.CandidateName)) {
    if (input.candidateNameKeys.has(key)) {
      return true;
    }
  }
  return false;
}

export function resolveColoradoCandidateCommittee(input: {
  candidateName: string;
  electionYear: number;
  contributionRows: readonly ColoradoTracerContributionRow[];
  sourceUrl?: string | null;
}): ColoradoCandidateCommitteeResolution {
  if (!Number.isInteger(input.electionYear) || input.electionYear < 2001 || input.electionYear > 2100) {
    throw new Error(`Invalid Colorado candidate committee election year: ${input.electionYear}`);
  }

  const candidateNameKeys = normalizeColoradoCandidateNameKeys(input.candidateName);
  if (candidateNameKeys.size === 0) {
    return { status: "unmatched", reason: "missing_candidate_name" };
  }

  const matches = new Map<string, CandidateCommitteeMatch>();
  for (const row of input.contributionRows) {
    const committeeId = row.CO_ID.trim().toUpperCase();
    const committeeName = row.CommitteeName.trim();
    if (!committeeId || !committeeName) {
      continue;
    }
    if (!isCandidateCommittee(row) || !isElectionCycleContribution(row, input.electionYear)) {
      continue;
    }
    if (!rowMatchesCandidateName({ row, candidateNameKeys })) {
      continue;
    }
    matches.set(committeeId, {
      committeeId,
      committeeName,
    });
  }

  if (matches.size === 0) {
    return { status: "unmatched", reason: "no_candidate_committee_match" };
  }
  if (matches.size > 1) {
    return { status: "ambiguous", reason: "multiple_matching_committees" };
  }

  const match = [...matches.values()][0];
  if (!match) {
    return { status: "unmatched", reason: "no_candidate_committee_match" };
  }
  return {
    status: "matched",
    committeeId: match.committeeId,
    committeeName: match.committeeName,
    sourceUrl: input.sourceUrl ?? null,
  };
}
