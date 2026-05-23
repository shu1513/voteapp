import { shouldIncludeCandidatePartyByPolicy } from "./electionPartisanshipPolicy.js";

export function shouldIncludePartyForCandidateContest(input: {
  districtType: string;
  state: string;
  officialBallotTitle: string;
}): boolean {
  return shouldIncludeCandidatePartyByPolicy({
    districtType: input.districtType,
    state: input.state,
    officialBallotTitle: input.officialBallotTitle,
  });
}

export function resolveIncludePartyForCandidateContest(input: {
  districtType: string;
  state: string;
  officialBallotTitle: string;
  electionIsPartisan?: boolean | null;
}): boolean {
  if (typeof input.electionIsPartisan === "boolean") {
    return input.electionIsPartisan;
  }

  return shouldIncludePartyForCandidateContest({
    districtType: input.districtType,
    state: input.state,
    officialBallotTitle: input.officialBallotTitle,
  });
}
