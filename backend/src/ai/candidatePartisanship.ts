import {
  resolveCandidateContestPartisanshipByPolicy,
  shouldIncludeCandidatePartyByPolicy,
} from "./electionPartisanshipPolicy.js";

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
  const fixedPolicyValue = resolveCandidateContestPartisanshipByPolicy(input);
  if (typeof input.electionIsPartisan === "boolean") {
    if (fixedPolicyValue !== undefined && input.electionIsPartisan !== fixedPolicyValue) {
      throw new Error(
        `Election is_partisan=${input.electionIsPartisan} contradicts fixed partisanship policy ` +
          `for ${input.state} ${input.districtType} contest "${input.officialBallotTitle}" ` +
          `(expected ${fixedPolicyValue}). Correct the election through the elections wrapper before candidate work.`
      );
    }
    return input.electionIsPartisan;
  }

  if (fixedPolicyValue !== undefined) {
    return fixedPolicyValue;
  }

  return shouldIncludePartyForCandidateContest({
    districtType: input.districtType,
    state: input.state,
    officialBallotTitle: input.officialBallotTitle,
  });
}

function isNonpartisanPlaceholder(value: string): boolean {
  const normalized = value.trim().toLowerCase().replace(/[-_]+/g, " ").replace(/\s+/g, " ");
  return normalized === "nonpartisan" || normalized === "non partisan" || normalized === "unknown";
}

export function assertCandidatePartyWillNotBeDiscarded(input: {
  includeParty: boolean;
  partyLabels: readonly (string | null | undefined)[];
}): void {
  if (input.includeParty) {
    return;
  }
  const discarded = [...new Set(
    input.partyLabels
      .map((value) => value?.trim())
      .filter((value): value is string => Boolean(value))
      .filter((value) => !isNonpartisanPlaceholder(value))
  )];
  if (discarded.length > 0) {
    throw new Error(
      `Nonpartisan candidate storage would discard candidate party label(s): ${discarded.join(", ")}. ` +
        "Correct the election partisanship or remove unsupported party data before writing."
    );
  }
}
