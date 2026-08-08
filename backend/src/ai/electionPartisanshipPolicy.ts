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
  // Arizona counties under the merit-selection threshold elect Superior Court
  // judges and justices of the peace through partisan primaries: the certified
  // 2026 primary canvass prints "REP Judge of Superior Court Div. 2" and "DEM
  // Justice of the Peace Prec. 2" (Yuma County). The merit-selection counties
  // (Maricopa, Pima, Pinal) put their judges on the ballot as retention
  // questions instead, which the retention rule below already forces false.
  "AZ",
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
  "SC",
  "TN",
  "TX",
]);
// A state's partisan judicial rule can stop short of some scopes. Arizona's
// partisan judicial elections are county offices; its city courts are creatures
// of the city charter and run on the same nonpartisan ballot as the mayor
// ("Presiding Municipal Judge, City of Yuma", live), and its appellate benches
// are merit-selected retention questions. This is Arizona's rule, not a general
// one — Indiana, New York and Pennsylvania all elect municipal judges on a
// party ballot — so states absent from this map keep every scope.
const PARTISAN_JUDICIAL_SCOPES_BY_STATE = new Map<string, ReadonlySet<string>>([
  ["AZ", new Set(["county"])],
]);

function isPartisanJudicialContest(input: { state: string; districtType: string }): boolean {
  const state = normalizeState(input.state);
  if (!PARTISAN_JUDICIAL_STATES.has(state)) {
    return false;
  }
  const partisanScopes = PARTISAN_JUDICIAL_SCOPES_BY_STATE.get(state);
  return !partisanScopes || partisanScopes.has(input.districtType);
}

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

// Offices that administer or prosecute before a court name that court in their
// own title without being judgeships: "Clerk of Superior Court" (every Arizona
// and North Carolina county), "Elkhart County Circuit Court Clerk" (Indiana),
// "Prosecuting Attorney ... 34th Judicial Circuit", "State Attorney, 4th
// Judicial Circuit" (Florida), "Constable, Justice Precinct 2" (Texas). Their
// partisanship follows the state's rule for ordinary county offices, not its
// judicial rule, so classifying them as judicial forced the wrong answer in
// both directions. Mirrors the office matcher's own non-judicial markers.
const NON_JUDICIAL_OFFICE_TITLE_MARKERS =
  /\b(clerk|prosecut(?:or|ing attorney)|district attorney|state'?s? attorney|county attorney|attorney general|solicitor|constable|sheriff|marshal|recorder|coroner)\b/i;

export function isJudicialOfficeTitle(title: string): boolean {
  if (NON_JUDICIAL_OFFICE_TITLE_MARKERS.test(title)) {
    return false;
  }
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
    return isPartisanJudicialContest({
      state: args.draft.state,
      districtType: args.draft.district_type,
    })
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
    return isPartisanJudicialContest(input) ? "force_true" : "force_false";
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
