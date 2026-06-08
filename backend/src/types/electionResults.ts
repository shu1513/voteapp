export const ELECTION_RESULT_PASS_TYPES = ["election_night", "certified"] as const;
export type ElectionResultPassType = (typeof ELECTION_RESULT_PASS_TYPES)[number];

export const ELECTION_RESULT_STATUSES = [
  "projected",
  "unofficial",
  "certified",
  "not_found",
  "not_final_yet",
] as const;
export type ElectionResultStatus = (typeof ELECTION_RESULT_STATUSES)[number];

export const ELECTION_RESULT_SOURCE_TYPES = ["official", "ap", "news", "other"] as const;
export type ElectionResultSourceType = (typeof ELECTION_RESULT_SOURCE_TYPES)[number];

export const ELECTION_RESULT_OUTCOMES = [
  "too_close",
  "won",
  "advanced",
  "runoff",
  "unknown",
] as const;
export type ElectionResultOutcome = (typeof ELECTION_RESULT_OUTCOMES)[number];

export const ELECTION_RESULT_MATCH_STATUSES = [
  "matched",
  "partial",
  "unmatched",
  "not_applicable",
  "not_found",
] as const;
export type ElectionResultMatchStatus = (typeof ELECTION_RESULT_MATCH_STATUSES)[number];

export const BALLOT_MEASURE_RESULT_OUTCOMES = [
  "passed",
  "failed",
  "unknown",
] as const;
export type BallotMeasureResultOutcome = (typeof BALLOT_MEASURE_RESULT_OUTCOMES)[number];

export const CANDIDATE_ELECTION_STATUSES = [
  "declared",
  "withdrawn",
  "won",
  "lost",
  "advanced",
  "runoff",
] as const;
export type CandidateElectionStatus = (typeof CANDIDATE_ELECTION_STATUSES)[number];

const CANONICAL_RESULT_STATUSES = new Set<ElectionResultStatus>(["certified"]);

export type CanonicalResultProjectionInput = {
  passType: ElectionResultPassType;
  resultStatus: ElectionResultStatus;
  sourceType: ElectionResultSourceType;
};

export function canProjectToCanonicalElectionStatus(input: CanonicalResultProjectionInput): boolean {
  return (
    input.passType === "certified" &&
    input.sourceType === "official" &&
    CANONICAL_RESULT_STATUSES.has(input.resultStatus)
  );
}
