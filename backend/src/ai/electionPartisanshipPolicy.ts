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
// `is_partisan` is BALLOT-FACING: it records whether the party appears next to
// the candidate's name on the general-election ballot, not how the candidate
// was nominated. A partisan primary feeding a nonpartisan general is the single
// most common way a state-level summary (Ballotpedia/NCSC) misleads here, so
// every entry below is stated per court level and sourced to the ballot statute
// rather than to the selection-method summary.
//
// `partisan`/`nonpartisan` force the value and make the contract reject the
// other one. `ask` forces nothing: the researcher's or the model's value stands.
// `ask` is the honest answer where the ballot format varies by COUNTY rather
// than by title — this classifier only sees the state and the title, so a
// county-varying state must not be forced either way.
type JudicialBallotMode = "partisan" | "nonpartisan" | "ask";

type JudicialTitleRule = {
  pattern: RegExp;
  mode: JudicialBallotMode;
};

type StateJudicialBallotPolicy = {
  // Applies to any judicial title the rules below do not match.
  fallback: JudicialBallotMode;
  // First match wins, so order these most-specific first.
  titleRules?: readonly JudicialTitleRule[];
};

// Ohio prints the party for the two appellate courts ONLY. H.B. 149 / S.B. 80
// (134th G.A., enacted 2021) added judicial party designation to the office-type
// ballot: ORC 3505.03 lists chief justice, justice of the supreme court, and
// judge of the court of appeals among the offices whose candidates get "the
// name of the political party by which the candidate was nominated" printed
// under the name. ORC 3505.04 leaves "judges of a municipal court, county
// court, or court of common pleas" on the NONPARTISAN ballot, where "[n]o name
// or designation of any political party ... shall be printed under or after any
// nonpartisan candidate's name" — those judges are nominated at a party primary
// and still appear in November with no party. Forcing every Ohio judicial
// contest partisan rejected the legally correct common-pleas rows on rewrite
// (live 2026-08-08: near-identical common pleas titles stored as both t and f).
//
// Ohio is therefore expressed as nonpartisan-with-two-partisan-titles rather
// than the other way round. Enumerating the lower courts cannot be made safe:
// the probate and juvenile courts ARE divisions of the court of common pleas
// (ORC 2101.01 defines "probate court" as "the probate division of the court of
// common pleas" and its judge as "the judge of the court of common pleas who is
// judge of the probate division"), so a board that titles the contest "PROBATE
// COURT JUDGE, JUVENILE DIVISION" names no lower court a pattern could key on
// and would fall through to partisan. Naming the two courts that DO print a
// party is closed: ORC 3505.03 lists exactly the supreme court and the courts
// of appeals, and every other elected Ohio judge sits below them.
const OHIO_PARTISAN_JUDICIAL_TITLE = /\b(?:supreme court|court of appeals?)\b/i;

// Indiana runs partisan trial-court elections in most counties, but Vanderburgh
// puts its circuit and superior judges on the ballot "without party
// designation" by statute (Ind. Code 33-33-82-31), and Allen County's superior
// judges are nonpartisan as well (its circuit judge stays partisan). Lake,
// St. Joseph, and Marion counties use merit selection, so their judges reach
// the ballot as retention questions and never get here.
const INDIANA_NONPARTISAN_JUDICIAL_TITLE =
  /\bvanderburgh\b|\ballen\s+(?:county\s+)?superior\s+court\b|\bsuperior\s+court\b[^.]{0,24}\ballen\s+county\b/i;

const STATE_JUDICIAL_BALLOT_POLICY = new Map<string, StateJudicialBallotPolicy>([
  // Every court but the municipal bench (appointed) is elected with a party
  // label; Alabama runs no judicial retention elections at all.
  ["AL", { fallback: "partisan" }],
  // Nonpartisan bench with one partisan precinct office: the Justice of the
  // Peace is printed in section one of the general ballot with the party in
  // bold-faced letters (A.R.S. § 16-502(C)(5)). Every other elected Arizona
  // judge is printed "without partisan or other designation" — Superior Court
  // judges in the counties that elect rather than retain them are nonpartisan
  // on the general ballot by constitutional command (Ariz. Const. art. 6
  // § 12(A)) and sit in section two with the appellate courts (A.R.S.
  // § 16-502(J)), whatever party nominated them in the primary.
  ["AZ", {
    fallback: "nonpartisan",
    titleRules: [{ pattern: /\bjustice of the peace\b/i, mode: "partisan" }],
  }],
  // Partisan initial election at every level; later terms are nonpartisan
  // retention questions, which the retention rule catches before this map.
  ["IL", { fallback: "partisan" }],
  ["IN", {
    fallback: "partisan",
    titleRules: [{ pattern: INDIANA_NONPARTISAN_JUDICIAL_TITLE, mode: "nonpartisan" }],
  }],
  // 14 judicial districts elect district judges on the party ballot; the other
  // 17, plus the appellate courts, use merit selection and reach voters as
  // retention questions.
  ["KS", { fallback: "partisan" }],
  ["LA", { fallback: "partisan" }],
  // Partisan election in most circuits; the supreme court, court of appeals,
  // and the six Nonpartisan Court Plan circuits appear as retention questions.
  ["MO", { fallback: "partisan" }],
  ["NM", { fallback: "partisan" }],
  // Supreme court, county, surrogate, family, city, district, and town/village
  // justices are all elected on the party ballot; the Court of Appeals is
  // appointed and never appears.
  ["NY", { fallback: "partisan" }],
  // Partisan at every level again since the 2017-2018 statutes.
  ["NC", { fallback: "partisan" }],
  // Partisan initial election, then retention; magisterial district judges are
  // always partisan.
  ["PA", { fallback: "partisan" }],
  // The legislature elects the appellate and circuit bench, so the only South
  // Carolina judges who reach a ballot are the county probate judges, elected
  // with a party label.
  ["SC", { fallback: "partisan" }],
  // Tennessee forces nothing. Its appellate bench stands for retention (caught
  // by the retention rule before this map) and every trial level is decided by
  // the COUNTY, not by the title: circuit, chancery, and criminal judges are
  // partisan unless the county opts to run them nonpartisan, general sessions
  // judges are summarized statewide as nonpartisan but several counties do
  // print the party, and municipal judges follow each city's own ordinance.
  // Live 2026-08-06 August ballots show the split inside one title: Knox,
  // Davidson, Hamilton, and Montgomery general sessions contests carry a party
  // while Shelby's do not. A classifier that sees only the state and the title
  // cannot decide this, so the researched ballot-facing value stands.
  ["TN", { fallback: "ask" }],
  ["OH", {
    fallback: "nonpartisan",
    titleRules: [{ pattern: OHIO_PARTISAN_JUDICIAL_TITLE, mode: "partisan" }],
  }],
  ["TX", { fallback: "partisan" }],
]);

// States absent from the map print no party for any judicial contest.
function getJudicialBallotMode(input: {
  state: string;
  officialBallotTitle: string;
}): JudicialBallotMode {
  const policy = STATE_JUDICIAL_BALLOT_POLICY.get(normalizeState(input.state));
  if (!policy) {
    return "nonpartisan";
  }
  for (const rule of policy.titleRules ?? []) {
    if (rule.pattern.test(input.officialBallotTitle)) {
      return rule.mode;
    }
  }
  return policy.fallback;
}

function getJudicialPartisanshipMode(input: {
  state: string;
  officialBallotTitle: string;
}): PartisanshipMode {
  const mode = getJudicialBallotMode(input);
  if (mode === "partisan") {
    return "force_true";
  }
  if (mode === "nonpartisan") {
    return "force_false";
  }
  return "ask_ai";
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

// A court clerk keeps the case files; they do not judge. The office catalog
// resolves "Clerk of Superior Court" and friends to the non-judicial county
// office "Clerk of Court", but the bare "superior court" token below swallowed
// them and handed the whole office to judicial policy (live 2026-08-08: Yuma
// County's partisan Clerk of Superior Court contest was rejected as a judge).
const COURT_CLERK_TITLE_PATTERN = /\bclerks?\b/i;

export function isJudicialOfficeTitle(title: string): boolean {
  if (COURT_CLERK_TITLE_PATTERN.test(title)) {
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
    return getJudicialPartisanshipMode({
      state: args.draft.state,
      officialBallotTitle: args.officialBallotTitle,
    });
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
    return getJudicialPartisanshipMode(input);
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
