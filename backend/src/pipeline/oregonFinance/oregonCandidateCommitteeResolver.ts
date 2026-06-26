import type { OregonOrestarTransactionSearchResultRow } from "./oregonOrestarParser.js";

export type OregonCandidateCommitteeMatch = {
  committeeId: string;
  committeeName: string;
  confidence: "exact" | "name_contains_candidate";
  source: "orestar_public";
  sourceUrl: string | null;
  matchedCommitteeRowCount: number;
};

export type OregonCandidateCommitteeResolution =
  | ({ status: "matched" } & OregonCandidateCommitteeMatch)
  | {
      status: "no_match";
      reason: string;
      matchedCommitteeRowCount: number;
    }
  | {
      status: "ambiguous";
      reason: string;
      matches: OregonCandidateCommitteeMatch[];
      matchedCommitteeRowCount: number;
    };

export type OregonCandidateCommitteeResolver = (input: {
  candidateName: string;
  searchRows: readonly OregonOrestarTransactionSearchResultRow[];
  sourceUrl?: string | null;
}) => Promise<OregonCandidateCommitteeResolution> | OregonCandidateCommitteeResolution;

export function normalizeOregonCandidateNameForStorage(value: string): string {
  return value
    .trim()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function committeeKey(input: { committeeId: string; committeeName: string }): string {
  return `${input.committeeId}\u0000${normalizeOregonCandidateNameForStorage(input.committeeName)}`;
}

function candidateTokens(candidateName: string): string[] {
  return normalizeOregonCandidateNameForStorage(candidateName)
    .split(" ")
    .filter((token) => token.length > 1);
}

function isCommitteeNameCandidateMatch(input: { candidateName: string; committeeName: string }): boolean {
  const candidate = normalizeOregonCandidateNameForStorage(input.candidateName);
  const committee = normalizeOregonCandidateNameForStorage(input.committeeName);
  if (!candidate || !committee) {
    return false;
  }
  if (committee.includes(candidate)) {
    return true;
  }
  const tokens = candidateTokens(input.candidateName);
  return tokens.length >= 2 && tokens.every((token) => committee.includes(token));
}

export function resolveOregonCandidateCommitteeFromSearchRows(input: {
  candidateName: string;
  searchRows: readonly OregonOrestarTransactionSearchResultRow[];
  sourceUrl?: string | null;
}): OregonCandidateCommitteeResolution {
  const candidateName = normalizeOregonCandidateNameForStorage(input.candidateName);
  if (!candidateName) {
    throw new Error("Oregon candidate name is required");
  }

  const matches = new Map<string, OregonCandidateCommitteeMatch>();
  let matchedCommitteeRowCount = 0;
  for (const row of input.searchRows) {
    const committeeId = row.filerCommitteeId?.trim();
    const committeeName = row.filerCommitteeName?.trim();
    if (!committeeId || !committeeName) {
      continue;
    }
    if (!isCommitteeNameCandidateMatch({ candidateName: input.candidateName, committeeName })) {
      continue;
    }
    matchedCommitteeRowCount += 1;
    const key = committeeKey({ committeeId, committeeName });
    const existing = matches.get(key);
    if (existing) {
      matches.set(key, {
        ...existing,
        matchedCommitteeRowCount: existing.matchedCommitteeRowCount + 1,
      });
      continue;
    }
    matches.set(key, {
      committeeId,
      committeeName,
      confidence: normalizeOregonCandidateNameForStorage(committeeName).includes(candidateName)
        ? "exact"
        : "name_contains_candidate",
      source: "orestar_public",
      sourceUrl: row.committeeUrl ?? input.sourceUrl ?? null,
      matchedCommitteeRowCount: 1,
    });
  }

  const uniqueMatches = [...matches.values()].sort((left, right) =>
    left.committeeName.localeCompare(right.committeeName)
  );
  if (uniqueMatches.length === 0) {
    return {
      status: "no_match",
      reason: "No ORESTAR committee rows matched the candidate name",
      matchedCommitteeRowCount,
    };
  }
  if (uniqueMatches.length > 1) {
    return {
      status: "ambiguous",
      reason: "Multiple ORESTAR committees matched the candidate name",
      matches: uniqueMatches,
      matchedCommitteeRowCount,
    };
  }
  return {
    status: "matched",
    ...uniqueMatches[0]!,
    matchedCommitteeRowCount,
  };
}
