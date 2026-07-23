import type { ElectionDraftPayload, ElectionRaceType } from "../types/election.js";

type ContestFamily = "all" | "non_judicial_office" | "judicial_office" | "ballot_measure" | "us_senate";
type SchoolPartisanshipMode = "partisan" | "mixed" | "nonpartisan";
type PartisanshipMode = "force_true" | "force_false" | "ask_ai";

const SCHOOL_DISTRICT_TYPES = new Set([
  "school_elementary",
  "school_secondary",
  "school_unified",
]);

const SCHOOL_PARTISAN_STATES = new Set(["AL", "CT", "IN", "LA", "PA"]);
const SCHOOL_MIXED_STATES = new Set(["GA", "NC", "RI", "SC", "TN"]);
// Policy lists are based on election-law summaries (e.g., Ballotpedia/NCSC); update as laws evolve.
const PARTISAN_JUDICIAL_STATES = new Set([
  "AL",
  "IL",
  "IN",
  "KS",
  "LA",
  "MO",
  "NM",
  "NY",
  "NC",
  "OH",
  "PA",
  "TN",
  "TX",
]);

function isWashingtonStateLegislativeContest(input: {
  districtType: string;
  state: string;
}): boolean {
  return normalizeState(input.state) === "WA" &&
    (input.districtType === "state_upper" || input.districtType === "state_lower");
}

function normalizeState(value: string): string {
  return value.trim().toUpperCase();
}

function isSchoolDistrictType(districtType: string): boolean {
  return SCHOOL_DISTRICT_TYPES.has(districtType);
}

function getSchoolPartisanshipMode(state: string): SchoolPartisanshipMode {
  const normalized = normalizeState(state);
  if (SCHOOL_PARTISAN_STATES.has(normalized)) {
    return "partisan";
  }
  if (SCHOOL_MIXED_STATES.has(normalized)) {
    return "mixed";
  }
  return "nonpartisan";
}

export function isJudicialOfficeTitle(title: string): boolean {
  return /\b(judge|justice|judicial|superior court|court of appeal(s)?|supreme court|retention|magistrate)\b/i.test(
    title
  );
}

export function isJudicialRetentionTitle(title: string): boolean {
  return /\b(retention|retain(?:ed|ing)?|be retained)\b/i.test(title);
}

function isJudicialContest(
  contestFamily: ContestFamily,
  raceType: ElectionRaceType,
  officialBallotTitle: string
): boolean {
  if (raceType !== "office") {
    return false;
  }
  if (contestFamily === "judicial_office") {
    return true;
  }
  return isJudicialOfficeTitle(officialBallotTitle);
}

function getPartisanshipModeForContest(args: {
  draft: ElectionDraftPayload;
  contestFamily: ContestFamily;
  raceType: ElectionRaceType;
  officialBallotTitle: string;
}): PartisanshipMode {
  if (args.raceType === "ballot_measure" || args.contestFamily === "ballot_measure") {
    return "force_false";
  }

  if (args.contestFamily === "us_senate") {
    return "force_true";
  }

  // Washington's Top Two primary does not make legislative offices
  // nonpartisan: candidates state a party preference on the ballot.
  if (isWashingtonStateLegislativeContest({
    districtType: args.draft.district_type,
    state: args.draft.state,
  })) {
    return "force_true";
  }

  if (isSchoolDistrictType(args.draft.district_type)) {
    const schoolMode = getSchoolPartisanshipMode(args.draft.state);
    if (schoolMode === "partisan") {
      return "force_true";
    }
    if (schoolMode === "mixed") {
      return "ask_ai";
    }
    return "force_false";
  }

  if (isJudicialContest(args.contestFamily, args.raceType, args.officialBallotTitle)) {
    if (isJudicialRetentionTitle(args.officialBallotTitle)) {
      return "force_false";
    }
    return PARTISAN_JUDICIAL_STATES.has(normalizeState(args.draft.state))
      ? "force_true"
      : "force_false";
  }

  return "ask_ai";
}

function getPartisanshipModeForOfficeScope(input: {
  districtType: string;
  state: string;
  officialBallotTitle: string;
}): PartisanshipMode {
  if (isWashingtonStateLegislativeContest(input)) {
    return "force_true";
  }

  if (isSchoolDistrictType(input.districtType)) {
    const schoolMode = getSchoolPartisanshipMode(input.state);
    if (schoolMode === "partisan") {
      return "force_true";
    }
    if (schoolMode === "mixed") {
      return "ask_ai";
    }
    return "force_false";
  }

  if (isJudicialOfficeTitle(input.officialBallotTitle)) {
    if (isJudicialRetentionTitle(input.officialBallotTitle)) {
      return "force_false";
    }
    return PARTISAN_JUDICIAL_STATES.has(normalizeState(input.state)) ? "force_true" : "force_false";
  }

  return "ask_ai";
}

export function resolveCandidateContestPartisanshipByPolicy(input: {
  districtType: string;
  state: string;
  officialBallotTitle: string;
}): boolean | undefined {
  const mode = getPartisanshipModeForOfficeScope(input);
  if (mode === "force_true") {
    return true;
  }
  if (mode === "force_false") {
    return false;
  }
  return undefined;
}

export function shouldIncludeCandidatePartyByPolicy(input: {
  districtType: string;
  state: string;
  officialBallotTitle: string;
}): boolean {
  const mode = getPartisanshipModeForOfficeScope(input);
  return mode !== "force_false";
}

export function shouldAskIsPartisanInPrompt(args: {
  draft: ElectionDraftPayload;
  contestFamily: ContestFamily;
}): boolean {
  const raceType: ElectionRaceType =
    args.contestFamily === "ballot_measure" ? "ballot_measure" : "office";
  const mode = getPartisanshipModeForContest({
    draft: args.draft,
    contestFamily: args.contestFamily,
    raceType,
    officialBallotTitle: "",
  });
  return mode === "ask_ai";
}

export function resolveElectionIsPartisan(args: {
  draft: ElectionDraftPayload;
  contestFamily: ContestFamily;
  raceType: ElectionRaceType;
  officialBallotTitle: string;
  aiValue: boolean | undefined;
}): boolean | undefined {
  const mode = getPartisanshipModeForContest({
    draft: args.draft,
    contestFamily: args.contestFamily,
    raceType: args.raceType,
    officialBallotTitle: args.officialBallotTitle,
  });

  if (mode === "force_true") {
    return true;
  }
  if (mode === "force_false") {
    return false;
  }
  if (typeof args.aiValue === "boolean") {
    return args.aiValue;
  }
  return undefined;
}
