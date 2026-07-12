import type {
  LosAngelesEthicsCandidateTotal,
  LosAngelesEthicsElection,
} from "./losAngelesCityEthicsClient.js";

export function normalizeLosAngelesCandidateName(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^A-Za-z0-9 ]/g, " ")
    .replace(/\b(?:JR|SR|II|III|IV)\b/gi, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();
}

export function resolveLosAngelesEthicsElection(input: {
  elections: readonly LosAngelesEthicsElection[];
  electionYear: number;
}): LosAngelesEthicsElection | null {
  const matches = input.elections.filter(
    (election) =>
      election.electionYear === input.electionYear &&
      /CITY\s+AND\s+LAUSD\s+ELECTIONS/i.test(election.description),
  );
  return matches.length === 1 ? matches[0]! : null;
}

export type LosAngelesCandidateCommitteeResolution =
  | { status: "matched"; candidate: LosAngelesEthicsCandidateTotal }
  | { status: "not_found" | "ambiguous"; reason: string };

export function resolveLosAngelesCandidateCommittee(input: {
  candidateName: string;
  officeName: string;
  candidates: readonly LosAngelesEthicsCandidateTotal[];
}): LosAngelesCandidateCommitteeResolution {
  const name = normalizeLosAngelesCandidateName(input.candidateName);
  const matches = input.candidates.filter(
    (candidate) =>
      candidate.officeName.trim().toUpperCase() ===
        input.officeName.trim().toUpperCase() &&
      normalizeLosAngelesCandidateName(candidate.candidateName) === name,
  );
  if (matches.length === 0)
    return {
      status: "not_found",
      reason:
        "No exact candidate and office match in Los Angeles Ethics totals",
    };
  if (matches.length > 1)
    return {
      status: "ambiguous",
      reason:
        "Multiple exact candidate and office matches in Los Angeles Ethics totals",
    };
  return { status: "matched", candidate: matches[0]! };
}
