import type { PoolClient } from "pg";

import { getStateNameByAbbreviation } from "../../constants/usStates.js";
import type { ElectionContestFamily, ElectionDistrictType } from "../../types/election.js";
import { normalizeElectionTitleKey } from "../../utils/normalizeElectionTitleKey.js";
import { isUsSenateOfficeTitle } from "../../utils/senateOffice.js";

type OfficeAliasRow = {
  office_id: string;
  normalized_alias: string;
};

type OfficeRow = {
  id: string;
  canonical_name: string;
};

type OfficeCandidate = {
  id: string;
  canonicalName: string;
  canonicalMatcherKey: string;
  canonicalTokens: string[];
};

type OfficeMatchInput = {
  scope: ElectionDistrictType;
  districtName: string;
  state: string;
  officialBallotTitle: string;
  discoveryContestFamily?: ElectionContestFamily;
};

type OfficeMatchMethod = "alias_exact" | "deterministic_fallback" | "none" | "ambiguous";

export type OfficeMatchResult = {
  officeId: string | null;
  method: OfficeMatchMethod;
  confidence: number;
  normalizedAlias: string;
  aliasMemoryKey: string;
  shouldPersistAlias: boolean;
};

const MIN_CONFIDENCE = 0.56;
const MIN_MARGIN = 0.12;
const US_SENATE_CANONICAL_NAME = "United States Senator";
const US_HOUSE_CANONICAL_NAME = "United States Representative";
const STATE_UPPER_CANONICAL_NAME = "State Senator";
const STATE_LOWER_CANONICAL_NAME = "State Lower Chamber Legislator";
const SCHOOL_BOARD_CANONICAL_NAME = "School Board Member";
const STATE_LEVEL_JUDGE_CANONICAL_NAME = "State Level Judge";
const COUNTY_LEVEL_JUDGE_CANONICAL_NAME = "County Level Judge";
const PLACE_LEVEL_JUDGE_CANONICAL_NAME = "Place Level Judge";
const CLERK_OF_COURT_CANONICAL_NAME = "Clerk of Court";
const JUSTICE_OF_THE_PEACE_CANONICAL_NAME = "Justice of the Peace";
const JUDGE_CANONICAL_NAMES = new Set([
  STATE_LEVEL_JUDGE_CANONICAL_NAME,
  COUNTY_LEVEL_JUDGE_CANONICAL_NAME,
  PLACE_LEVEL_JUDGE_CANONICAL_NAME,
  // A justice of the peace is an elected judicial officer, so the entry's own
  // contest family governs it exactly as it governs the trial-court offices:
  // a non-judicial entry never matches into it, by alias or by score.
  JUSTICE_OF_THE_PEACE_CANONICAL_NAME,
]);

export function isJudicialOfficeCanonicalName(canonicalName: string): boolean {
  return JUDGE_CANONICAL_NAMES.has(canonicalName);
}

const SCHOOL_DISTRICT_SCOPES = new Set<ElectionDistrictType>([
  "school_elementary",
  "school_secondary",
  "school_unified",
]);
const NON_US_SENATE_OFFICE_MARKERS = [
  /\bstate senate\b/,
  /\bstate senator\b/,
  /\bgovernor\b/,
  /\blieutenant governor\b/,
  /\battorney general\b/,
  /\bsecretary of state\b/,
  /\btreasurer\b/,
  /\bauditor\b/,
  /\bcomptroller\b/,
  /\bcontroller\b/,
  /\bcommissioner\b/,
  /\bsuperintendent\b/,
  /\brepresentative\b/,
  /\bhouse\b/,
  /\bassembly\b/,
  /\bdelegate\b/,
  /\bmayor\b/,
  /\bcouncil\b/,
  /\balderman\b/,
  /\bsheriff\b/,
  /\bassessor\b/,
  /\bclerk\b/,
  /\brecorder\b/,
  /\bcoroner\b/,
  /\bjudge\b/,
  /\bjustice\b/,
  /\bcourt\b/,
  /\bschool\b/,
  /\bboard of education\b/,
];
const JUDICIAL_TITLE_ALLOW_MARKERS = [
  /\bjudge\b/,
  /\bjustice\b/,
  /\bcourt\b/,
  /\bjudicial\b/,
  /\bmagistrate\b/,
  /\bretention\b/,
  /\bretain(?:ed|ing)?\b/,
];
const NON_JUDICIAL_TITLE_MARKERS = [
  // A city marshal is the city court's law-enforcement officer, on the same
  // footing as the constable below: a Louisiana city marshal is elected under
  // La. R.S. 13:1879 et seq. and the title names the court it serves ("City
  // Marshal, City Court of Shreveport"), whose court words are judicial
  // allow-markers. Without this the judge fast path returned Place Level Judge
  // at confidence 1.000 — a silent, fully confident mis-file (Shreveport live,
  // three qualifiers). The office is catalogued in its own right (migration
  // 224), so a stamped-judicial marshal title still resolves through the alias
  // table and the token scorer.
  /\bmarshal\b/,
  // A constable is the justice court's law-enforcement officer, not one of its
  // judges, and Louisiana titles the seat by the court it serves ("Constable
  // 2nd Justice Court", "Constable Justice of the Peace Ward 7") — every one of
  // those court words is a judicial allow-marker. Without this the judge
  // fallback would hand a judicial-family constable entry to a judge office.
  /\bconstable\b/,
  /\bdistrict attorney\b/,
  /\bcounty district attorney\b/,
  /\bprosecuting attorney\b/,
  /\bcounty prosecutor\b/,
  /\bprosecutor\b/,
  /\battorney general\b/,
  /\bgovernor\b/,
  /\blieutenant governor\b/,
  /\bsenate\b/,
  /\bsenator\b/,
  /\brepresentative\b/,
  /\bhouse\b/,
  /\bassembly\b/,
  /\bdelegate\b/,
  /\bmayor\b/,
  /\bcity council\b/,
  /\btown council\b/,
  /\bcouncil member\b/,
  /\balderman\b/,
  /\bsheriff\b/,
  /\bassessor\b/,
  /\bclerk\b/,
  /\brecorder\b/,
  /\btreasurer\b/,
  /\bcoroner\b/,
  /\bsecretary of state\b/,
  /\bauditor\b/,
  /\bcomptroller\b/,
  /\bcontroller\b/,
  /\bcounty commissioner\b/,
  /\bboard of supervisors\b/,
  /\bsuperintendent\b/,
  /\bschool board\b/,
  /\bboard of education\b/,
];
// Tuned so a plain partial token overlap (around F1 ~= 0.5) is rejected unless boosted by stronger
// canonical phrase agreement. This keeps deterministic fallback conservative.
const STOPWORDS = new Set([
  "of",
  "the",
  "and",
  "for",
  "in",
  "to",
  "primary",
  "general",
  "runoff",
  "special",
  "election",
  "vacancy",
  "unexpired",
  "term",
]);

function normalizeMatcherText(value: string): string {
  return value
    .toLowerCase()
    .replace(/\bu\.?\s*s\.?\b/g, "united states")
    // Several cities title the office as one word ("City of Flagstaff
    // Councilmember", live); the catalog and its aliases key on the two-word
    // form, and one word tokenizes into zero overlap.
    .replace(/\bcouncilmembers?\b/g, "council member")
    // California-style county ballots title the supervisor seat by its
    // governing body ("MEMBER, BOARD OF SUPERVISORS DISTRICT NO. 5", San
    // Diego live); the catalog keys on "County Supervisor", and the body
    // form tokenizes into zero overlap ("supervisors" ≠ "supervisor").
    .replace(/\bmember,? board of supervisors\b/g, "county supervisor")
    // "TREASURER/TAX COLLECTOR" (San Diego live) is the county treasurer's
    // combined office; the compound form scores 1-of-3 token overlap against
    // "County Treasurer" and misses the confidence floor.
    .replace(/\btreasurer\s*[/-]\s*tax collector\b/g, "treasurer")
    // "(s)" is an optional-plural marker on the office word, never a word of
    // its own: St. Tammany Parish LA titles 19 live seats "Constable(s) Justice
    // of the Peace Ward N" and "Justice(s) of the Peace ...". The generic
    // punctuation fold below would leave a stray "s" token mid-phrase, which
    // both breaks the office phrase and costs a token of precision — the
    // constable form failed at exactly 0.520, the same way Jefferson's did.
    // Dropped before that fold so the phrase survives intact.
    .replace(/\(s\)/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    // Louisiana's SOS concatenates the office name onto the ballot title, so
    // every justice-of-the-peace contest reads "Justice of the Peace Justice
    // of the Peace Ward N" (Caddo Parish live; the same source doubles
    // "Magistrate Magistrate Section"). Collapse the repeat so the key names
    // the office once and every parish's JP seats share one alias key. Runs
    // after punctuation folding so a separator between the two copies does
    // not hide the repeat.
    .replace(/\bjustice of the peace (?=justice of the peace\b)/g, "");
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

const GENERIC_DISTRICT_SUFFIX_TOKENS = new Set([
  "county",
  "parish",
  "borough",
  "city",
  "town",
  "village",
  "township",
  "municipality",
]);

function districtNameCore(rawDistrictName: string): string {
  const preComma = normalizeMatcherText(rawDistrictName.split(",")[0] ?? "");
  const tokens = preComma.split(" ").filter((token) => token.length > 0);
  while (tokens.length > 0 && GENERIC_DISTRICT_SUFFIX_TOKENS.has(tokens[tokens.length - 1] ?? "")) {
    tokens.pop();
  }
  return tokens.join(" ");
}

function stripJurisdictionPrefixes(value: string, input: { districtName: string; state: string }): string {
  let next = value;

  const districtName = normalizeMatcherText(input.districtName);
  if (districtName.length > 0) {
    const districtPattern = new RegExp(`\\b${escapeRegExp(districtName)}\\b`, "g");
    next = next.replace(districtPattern, " ");
  }

  // District rows are stored as "Harris County, Texas" while ballot titles say
  // "Harris County Judge", so the full-name strip above never fires. Strip the
  // district's proper-noun core ("harris") first — deliberately keeping the
  // generic civic word ("county") — so the remaining title ("county judge")
  // still matches catalog names/aliases keyed on the bare office title.
  const core = districtNameCore(input.districtName);
  if (core.length >= 2) {
    const corePattern = new RegExp(`\\b${escapeRegExp(core)}\\b`, "g");
    next = next.replace(corePattern, " ");
  }

  const stateLower = input.state.trim().toLowerCase();
  if (stateLower.length > 0) {
    if (/^[a-z]{2}$/.test(stateLower)) {
      const stateName = getStateNameByAbbreviation(stateLower);
      if (stateName) {
        const normalizedStateName = normalizeMatcherText(stateName);
        const stateNamePattern = new RegExp(`\\b${escapeRegExp(normalizedStateName)}\\b`, "g");
        next = next.replace(stateNamePattern, " ");
      }
      const stateAbbrevPattern = new RegExp(`\\b${escapeRegExp(stateLower)}\\b`, "g");
      next = next.replace(stateAbbrevPattern, " ");
    } else {
      const normalizedStateName = normalizeMatcherText(stateLower);
      if (normalizedStateName.length > 0) {
        const stateNamePattern = new RegExp(`\\b${escapeRegExp(normalizedStateName)}\\b`, "g");
        next = next.replace(stateNamePattern, " ");
      }
    }
  }

  next = next
    .replace(/\bstate of\b/g, " ")
    .replace(/\bcounty of\b/g, " ")
    .replace(/\bcity of\b/g, " ");

  return next.replace(/\s+/g, " ").trim();
}

// A LEADERSHIP-of-the-body contest elects a different office than a member
// seat: "President of the Cook County Board of Commissioners" is the county
// executive, and comma forms ("President, Middlesex County Commissioners",
// "Chair, County Board of Commissioners") lose their connectors in
// normalization, so no fixed lookbehind can see them. Singularizing such a
// title scores it ~0.92 into the member office — a confidently wrong,
// alias-persisted match; with the rewrites skipped it under-scores (~0.4) and
// falls safely to no-match. Position matters: only a leadership word BEFORE
// the body phrase names the leadership post. A TRAILING one is a seat
// descriptor on a member seat ("JACKSON COUNTY BOARD OF COMMISSIONERS
// CHAIRMAN", NC live: the chairman is elected to the board) and must keep the
// rewrite.
const LEADERSHIP_BEFORE_COMMISSION_BODY_PATTERN =
  /\b(?:president|chair(?:man|woman|person)?|director)\b[a-z ]*\bcommission/;

function singularizeCommissionerBodyForms(value: string): string {
  if (LEADERSHIP_BEFORE_COMMISSION_BODY_PATTERN.test(value)) {
    return value;
  }
  return (
    value
      // North Carolina certifications title every county-commission seat by its
      // governing body ("ALAMANCE COUNTY BOARD OF COMMISSIONERS DISTRICT 02",
      // ~150 live rows); the catalog keys on "County Commissioner" and the body
      // form's plural tokenizes into near-zero overlap ("commissioners" ≠
      // "commissioner"). The "county"-anchored phrase leaves city boards of
      // commissioners (place scope) untouched. (Lives here, after the
      // jurisdiction strip, so the county's own name never sits inside the
      // body phrase.)
      .replace(/\bcounty board of commissioners\b/g, "county commissioner")
      // Bare plural body form without "board of" ("Middlesex County
      // Commissioners", official NJ title, live: wrote a NULL-office shell).
      // The "of"-lookbehind leaves every "board of commissioners" phrase
      // alone — the county form was already rewritten above, and city board
      // phrases must keep their plural so they do not over-match the member
      // office. Not before "court": the Texas "Commissioners Court" governing
      // body keeps its official plural so its key stays faithful for aliasing.
      .replace(/(?<!\bof )\bcommissioners\b(?! court\b)/g, "commissioner")
      // Utah titles the county-commission seat by its governing body plus a
      // seat letter ("Utah County Commission Seat A", live: four NULL-office
      // shells); the catalog keys on the member office. The seat letter itself
      // is stripped by the seat-designator rule below.
      .replace(/\bcounty commission\b/g, "county commissioner")
  );
}

// An independent fire district titles each board seat by the district's own
// name ("Holley-Navarre Fire District Seat 3", "Navarre Beach Fire Rescue
// District, Seat 5" — Santa Rosa County FL live, 13 seats across five
// districts on the Nov 2026 ballot). The district is its own taxing body, not
// the county, so the jurisdiction strip never removes its proper noun: the
// name both dilutes the token overlap against the catalog office (Fire
// Control District Commissioner) and differs per district, so no fixed alias
// can cover the family. The live titles scored 0.40-0.57 against the 0.56
// floor and stranded NULL-office shells. Map the named body form onto the
// office it elects, consuming the name (up to four tokens — "Avalon
// Beach-Mulat" is the longest live one) so what remains is the catalog's own
// key. Applied to canonical names too, where it is a no-op by construction.
//
// A fire district elects more than its board, so the fold has to be exact. New
// York's Town Law §174 seats an elected district treasurer alongside the board
// of fire commissioners, and the district phrase is common to both. The fold
// therefore runs LAST — after the seat strip — and is anchored to the WHOLE
// remaining key, so a title that still names another role cannot match it. An
// unanchored substring fold scored "Smithtown Fire District Treasurer" 1.009
// into this office and persisted the alias.
const FIRE_DISTRICT_SEAT_KEY_PATTERN =
  /^(?:[a-z0-9]+ ){0,4}fire (?:(?:control|rescue|protection|suppression|and rescue) )?district(?: (?:board member|board|commission|commissioner))?$/;
const FIRE_DISTRICT_OFFICE_KEY = "fire control district commissioner";
// Roles a fire district elects or appoints that are NOT its board seat. The
// anchor above already excludes them when they trail the district phrase; this
// also covers the comma form ("Treasurer, Smithtown Fire District"), whose
// leading role word the anchor's name prefix would otherwise absorb.
const FIRE_DISTRICT_NON_BOARD_ROLE_PATTERN =
  /\b(?:treasurer|secretary|clerk|chief|marshal|collector|assessor|auditor|attorney)\b/;

function mapFireDistrictBodyForms(value: string): string {
  if (FIRE_DISTRICT_NON_BOARD_ROLE_PATTERN.test(value)) {
    return value;
  }
  return FIRE_DISTRICT_SEAT_KEY_PATTERN.test(value) ? FIRE_DISTRICT_OFFICE_KEY : value;
}

// What a numbered/lettered seat designator can look like once normalized:
// "5", "5a", "II", "A". Letter-only seats are as common as numbers on live
// ballots — New Orleans and Shreveport council districts run A-E, Utah
// commission seats A-D — and before this alternation carried a bare letter the
// letter survived the strip: "Council Member, District A" kept a stray "a"
// token and scored 0.571 (ambiguous, no office) where "District 1" resolved at
// 1.000. The Roman-numeral alternative already accepted i/v/x/l as bare
// letters, so accepting the rest is the consistent reading, not a widening of
// what counts as a seat: a single letter standing alone after ward/district/
// seat/zone/part/precinct is never an office word.
const SEAT_DESIGNATOR = String.raw`(?:\d+[a-z]{0,2}|[ivxl]+|[a-z])`;

function stripSeatSuffixes(value: string): string {
  const withoutSeat = singularizeCommissionerBodyForms(value)
    // Louisiana numbers its justice-of-the-peace COURTS and titles both the JP
    // seat and its constable seat by the court ("Justice of the Peace 2nd
    // Justice Court" / "Constable 2nd Justice Court", Jefferson Parish live:
    // the constable form failed the matcher outright at 0.520 and took the
    // whole payload down). The numbered court is the seat; the office is the
    // bare justice of the peace or constable.
    .replace(/\b(?:\d+[a-z]{0,2}|[ivxl]+) justice court\b/g, " ")
    // Orleans Parish titles its constable seat by the municipal court it serves
    // ("Constable 1st City Court"). Anchored on the constable word so a JUDGE
    // title keeps its court words — there the court names the office, not the
    // seat.
    .replace(/(?<=\bconstable )(?:\d+[a-z]{0,2}|[ivxl]+) (?:city|parish) court\b/g, " ")
    // Caddo Parish names the constable's seat by the JP court's ward
    // ("Constable Justice of the Peace Ward 7"). The court phrase is the seat
    // designator on a LAW-ENFORCEMENT office; only the leading constable word
    // names it. The ward number goes with the generic ward rule below.
    .replace(/(?<=\bconstable )justice of the peace\b/g, " ")
    .replace(/\boffice (?:no )?\d+\b/g, " ")
    .replace(/\bposition (?:no )?\d+\b/g, " ")
    // "Council District No. 5" (Seattle live) titles the council-member SEAT
    // by its district; a plain seat strip would leave the bare token
    // "council", which under-tokenizes against "City Council Member". Map
    // the phrase to the office the seat belongs to before the generic strip.
    // Not when "member of" already governs it ("For Member of County Council
    // (District 1)", Howard County MD live — one qualifier word may sit in
    // between): the office word is already present and the mapping would
    // duplicate it ("member of county council member"); the generic district
    // strip below handles that seat form.
    .replace(
      new RegExp(String.raw`(?<!\bmember of (?:[a-z]+ )?)\bcouncil (?:district|dist) (?:no )?${SEAT_DESIGNATOR}\b`, "g"),
      "council member"
    )
    // "for" is removed with the seat it introduces: "Council Member for
    // District 2" (Fort Worth, live) must reduce to "council member", not
    // "council member for" — the dangling connector misses the alias table
    // and left ten council elections office-less.
    .replace(new RegExp(String.raw`\bfor (?:district|dist) (?:no )?${SEAT_DESIGNATOR}\b`, "g"), " ")
    // Interposed "No." ("District No. 5", San Diego/Seattle live) and
    // Honolulu's abbreviated Roman form ("Dist II") are the same seat suffix;
    // both previously survived the strip and produced zero-overlap keys.
    .replace(new RegExp(String.raw`\b(?:district|dist) (?:no )?${SEAT_DESIGNATOR}\b`, "g"), " ")
    .replace(/\b\d+(st|nd|rd|th)\s+district\b/g, " ")
    // Caddo Parish LA subdivides a ward's JP court and names the piece after
    // the community it covers ("Justice of the Peace Ward 2, Vivian Dist.",
    // "... Ward 2, Oil City Dist."). The community name is part of the seat, so
    // it has to go with the ward number — left behind it would sit in the
    // learned alias key as if it were an office word. Bounded to the one or two
    // words a community name takes, and anchored on both the ward number in
    // front and the "dist(rict)" marker behind, so no office word can be caught.
    .replace(
      /\bward (?:no )?(?:\d+[a-z]{0,2}|[ivxl]+) [a-z]+(?: [a-z]+)? dist(?:rict)?\b/g,
      " "
    )
    // Ward/Zone/Seat/Part and (justice) precinct forms are the same numbered
    // seat suffix as District/Position, all hit live: Florida city commissions
    // ("City Commission Ward 1" / "Zone 3" / "Seat 4"), Carson City NV wards,
    // Tennessee judicial parts ("Chancellor Part II"), and Arizona constable
    // precincts ("Constable, Justice Prec. 2" — the seat is the justice-court
    // precinct, so the "justice" that introduces it goes with the number;
    // "Justice of the Peace, Prec. 2" keeps its office words because there
    // the number follows bare "prec"). Lettered seats ("Commission Seat A",
    // Utah live) are the same designator.
    .replace(
      new RegExp(
        String.raw`\b(?:ward|zone|seat|part|(?:justice )?(?:precinct|prec)) (?:no )?${SEAT_DESIGNATOR}\b`,
        "g"
      ),
      " "
    )
    // At-large is a seat designator, not an office word ("County Council At
    // Large", "Sarasota City Commission At-Large", NC "AT-LARGE" commission
    // seats, all live), optionally lettered ("County Council At-Large A").
    .replace(/\bat large(?: [a-z])?\b/g, " ")
    // A bare trailing "seat" left behind once its qualifier is stripped
    // ("BOARD OF COMMISSIONERS DISTRICT 01 SEAT", "AT-LARGE SEAT", NC live).
    .replace(/\bseat\b/g, " ")
    // Vacancy descriptors qualify the term being filled, never the office
    // ("(UNEXPIRED)" NC commission seats, "Chancellor ... Unexpired Term" TN).
    .replace(/\b(?:unexpired|vacancy)(?: term)?\b/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    // Ballot-heading form "For <office>" ("For Member of County Council",
    // Howard County MD live) — leading connector only, so office names that
    // merely contain "for" are untouched.
    .replace(/^for /, "");
  return mapFireDistrictBodyForms(withoutSeat);
}

// The jurisdiction strip deliberately keeps the generic civic word so
// bare-office aliases like "county judge" still match — but some states alias
// the office WITHOUT it: "Snohomish County Prosecuting Attorney" strips to
// "county prosecuting attorney" while the seeded county alias is "prosecuting
// attorney" (→ District Attorney; Pierce and Snohomish both wrote NULL-office
// shells live). This yields the civic-word-free form for a last-chance alias
// lookup. Callers only use multi-word remainders — single-word offices
// ("County Sheriff" → "sheriff") already resolve through the token scorer, and
// a one-token alias hit ("judge") would be too generic to trust.
function stripLeadingGenericCivicWords(value: string): string {
  const tokens = value.split(" ").filter((token) => token.length > 0);
  let start = 0;
  while (start < tokens.length && GENERIC_DISTRICT_SUFFIX_TOKENS.has(tokens[start] ?? "")) {
    start += 1;
  }
  return tokens.slice(start).join(" ");
}

function toMatcherTokens(value: string): string[] {
  return value
    .split(" ")
    .map((token) => token.trim())
    .filter((token) => token.length > 0 && !STOPWORDS.has(token));
}

function toMatcherKeyFromBallotTitle(input: OfficeMatchInput): string {
  const normalized = normalizeMatcherText(input.officialBallotTitle);
  const withoutJurisdiction = stripJurisdictionPrefixes(normalized, {
    districtName: input.districtName,
    state: input.state,
  });
  const withoutSeat = stripSeatSuffixes(withoutJurisdiction);
  return withoutSeat.length > 0 ? withoutSeat : normalized;
}

function toMatcherKeyFromCanonicalName(canonicalName: string): string {
  const normalized = normalizeMatcherText(canonicalName);
  const withoutSeat = stripSeatSuffixes(normalized);
  return withoutSeat.length > 0 ? withoutSeat : normalized;
}

function isSchoolDistrictScope(scope: ElectionDistrictType): boolean {
  return SCHOOL_DISTRICT_SCOPES.has(scope);
}

function findSingleScopeOffice(offices: OfficeCandidate[], canonicalName: string): OfficeCandidate | undefined {
  return offices.find((office) => office.canonicalName === canonicalName);
}

function toSingleScopeOfficeMatch(
  office: OfficeCandidate | undefined,
  normalizedAlias: string,
  titleMatcherKey: string
): OfficeMatchResult | null {
  if (!office) {
    return null;
  }
  return {
    officeId: office.id,
    method: "deterministic_fallback",
    confidence: 1,
    normalizedAlias,
    aliasMemoryKey: titleMatcherKey,
    shouldPersistAlias: titleMatcherKey.length > 0,
  };
}

function isUsSenateCompatibleTitle(titleMatcherKey: string): boolean {
  if (!/\bsenate\b|\bsenator\b/.test(titleMatcherKey)) {
    return false;
  }
  return !NON_US_SENATE_OFFICE_MARKERS.some((pattern) => pattern.test(titleMatcherKey));
}

function isJudicialCompatibleTitle(titleMatcherKey: string): boolean {
  if (!JUDICIAL_TITLE_ALLOW_MARKERS.some((pattern) => pattern.test(titleMatcherKey))) {
    return false;
  }
  return !NON_JUDICIAL_TITLE_MARKERS.some((pattern) => pattern.test(titleMatcherKey));
}

// A justice of the peace is its own elected office, not a seat on the county
// trial court: JP courts hear small claims, evictions, and (state by state)
// traffic and Class C misdemeanors. Two things would otherwise bury it. The
// judicial-family fallback below hands EVERY judicial county title to the
// generic County Level Judge before scoring runs, and the token scorer cannot
// reach the JP office on its own — "justice"/"peace" share no token with
// "county level judge", and "of"/"the" are stopwords. So the JP office needs
// its own branch ahead of the fallback.
//
// Constable titles are excluded: Louisiana names a constable's seat after the
// JP court it serves ("Constable Justice of the Peace Ward 7", Caddo Parish
// live), and that is a law-enforcement office. The seat strip already reduces
// those to the bare constable word; this is the belt-and-braces check for a
// form the strip has not seen.
function isJusticeOfThePeaceTitle(titleMatcherKey: string): boolean {
  if (/\bconstable\b/.test(titleMatcherKey)) {
    return false;
  }
  return hasPhrase(titleMatcherKey, "justice of the peace");
}

function judgeCanonicalNameForScope(scope: ElectionDistrictType): string | null {
  if (scope === "statewide") {
    return STATE_LEVEL_JUDGE_CANONICAL_NAME;
  }
  if (scope === "county") {
    return COUNTY_LEVEL_JUDGE_CANONICAL_NAME;
  }
  if (scope === "place") {
    return PLACE_LEVEL_JUDGE_CANONICAL_NAME;
  }
  return null;
}

// "<County> Clerk of the District Court" (Nebraska) and "<County> Clerk of
// Circuit Court" / "Clerk of Courts" (Wisconsin) elect the clerk of court, a
// distinct office from the county's own clerk. Those titles put the county
// name FIRST, so the jurisdiction strip leaves "county clerk of ... court":
// the short generic "county clerk" key sits inside it as a prefix and takes
// the phrase-containment boost, while the specific "clerk of court" key —
// split apart by the interposed court name, or pluralized to "courts" —
// scores lower and loses. Every Wisconsin county with an elected clerk of
// circuit court and every Nebraska county uses this title form, so the wrong
// office was systemic rather than one-off. Naming a COURT is what marks the
// seat: Nebraska's own county-clerk title ("Clerk Register of Deeds") names
// none and stays with County Clerk.
const COURT_CLERK_TITLE_PATTERNS = [/\bclerk of (?:the )?(?:[a-z]+ )?courts?\b/, /\bcourts? clerk\b/];

function isCourtClerkTitle(titleMatcherKey: string): boolean {
  return COURT_CLERK_TITLE_PATTERNS.some((pattern) => pattern.test(titleMatcherKey));
}

// The clerk offices that name no court of their own — County Clerk, County
// Clerk and Recorder, City Clerk — are exactly the wrong targets for such a
// title.
function isNonCourtClerkOfficeKey(canonicalMatcherKey: string): boolean {
  return /\bclerk\b/.test(canonicalMatcherKey) && !/\bcourts?\b/.test(canonicalMatcherKey);
}

function isWashingtonState(state: string): boolean {
  const normalized = state.trim().toLowerCase();
  return normalized === "wa" || normalized === "washington";
}

function hasPhrase(text: string, phrase: string): boolean {
  if (!text || !phrase) {
    return false;
  }
  return new RegExp(`\\b${escapeRegExp(phrase)}\\b`).test(text);
}

// Nouns that NAME THE JOB rather than describe it. The token scorer treats
// every token alike, so a title whose function noun the catalog does not carry
// still lands on whatever office shares its generic words, at a confidence high
// enough to look authoritative and to be persisted as a learned alias:
// "County Revenue Commissioner" (Alabama's combined tax assessor and collector,
// uncatalogued) scored 0.800 into County Commissioner on two of three tokens.
// That is the worst outcome in the pipeline — `method: none` aborts the payload
// loudly and the researcher defers, while a confident wrong match writes
// successfully and hands the contest another office's research areas, which
// every later stage (roster, profile, records, labels) then inherits.
//
// So: a function noun present in the title and absent from the matched office's
// canonical key vetoes that office. The list is deliberately short and holds
// only nouns whose absence is decisive. Words that legitimately differ across a
// state's synonym for the SAME job stay out of it — "prosecuting"/"prosecutor"
// (District Attorney), "auditor"/"recorder" (Idaho and Utah fold them into the
// county clerk's office, "Clerk, Auditor and Recorder" -> County Clerk and
// Recorder at 0.667), "register" ("Clerk Register of Deeds"), "judge" and
// "justice" (the contest-family filters already decide those) — because vetoing
// them would turn correct matches into no-matches. Each candidate noun was
// A/B-run over every distinct stored ballot title before being kept, and two
// were dropped on the evidence: "constable" (Louisiana's combined
// "Constable(s) Justice of the Peace Ward N" rows, which name both offices,
// fell from Justice of the Peace to no-match) and "treasurer" (a combined
// "County Auditor-Treasurer" merely FLIPPED to the other half of its own
// office — a coin-flip, not the silent mis-file this guards against).
//
// Seeded aliases outrank this: an alias hit returns before any scoring, so a
// state's own wording for a catalogued office is still expressed the same way
// it always was, by adding the alias.
const DISTINCT_FUNCTION_NOUNS = [
  "marshal",
  "coroner",
  "sheriff",
  "surveyor",
  "assessor",
  "engineer",
  "revenue",
  "moderator",
  "defender",
  "mayor",
  "superintendent",
];

function hasUncoveredFunctionNoun(titleMatcherKey: string, office: OfficeCandidate): boolean {
  return DISTINCT_FUNCTION_NOUNS.some(
    (noun) => hasPhrase(titleMatcherKey, noun) && !hasPhrase(office.canonicalMatcherKey, noun)
  );
}

function scoreOfficeMatch(titleMatcherKey: string, titleTokens: string[], office: OfficeCandidate): number {
  if (titleTokens.length === 0 || office.canonicalTokens.length === 0) {
    return 0;
  }

  if (
    hasPhrase(titleMatcherKey, "township supervisor") &&
    office.canonicalMatcherKey === "county supervisor"
  ) {
    return 0;
  }

  // Backstop for a catalog with no Clerk of Court office in scope: without it
  // the containment boost still hands a court-clerk title to the plain clerk
  // office and persists that as a learned alias. No-match is the honest
  // outcome there.
  if (isCourtClerkTitle(titleMatcherKey) && isNonCourtClerkOfficeKey(office.canonicalMatcherKey)) {
    return 0;
  }

  // The fold above refuses to rewrite a non-board fire-district role, but bare
  // token overlap can still carry one in on its own: "Fire District Clerk"
  // shares two of three tokens with this office and scores 0.571, just over the
  // floor. A district's treasurer/clerk/secretary is a different job, and the
  // catalog has no office for it — no match is the honest answer.
  if (
    office.canonicalMatcherKey === FIRE_DISTRICT_OFFICE_KEY &&
    FIRE_DISTRICT_NON_BOARD_ROLE_PATTERN.test(titleMatcherKey)
  ) {
    return 0;
  }

  if (hasUncoveredFunctionNoun(titleMatcherKey, office)) {
    return 0;
  }

  const titleSet = new Set(titleTokens);
  let intersectionCount = 0;
  for (const token of office.canonicalTokens) {
    if (titleSet.has(token)) {
      intersectionCount += 1;
    }
  }

  if (intersectionCount === 0) {
    return 0;
  }

  const precision = intersectionCount / titleTokens.length;
  const recall = intersectionCount / office.canonicalTokens.length;
  const f1 = (2 * precision * recall) / (precision + recall);

  let score = f1;

  if (titleMatcherKey === office.canonicalMatcherKey) {
    score += 0.25;
  } else if (hasPhrase(titleMatcherKey, office.canonicalMatcherKey)) {
    score += 0.12;
  }

  if (hasPhrase(titleMatcherKey, "lieutenant governor")) {
    // A joint ticket ("Governor and Lieutenant Governor") is the governor's
    // race — the lieutenant governor runs on the governor's ticket — so the
    // bias flips there. Without the flip, the penalty below handed joint
    // tickets to the Lieutenant Governor office.
    const isJointGovernorTicket = hasPhrase(
      titleMatcherKey.replace(/\blieutenant governor\b/g, " ").replace(/\s+/g, " ").trim(),
      "governor"
    );
    if (hasPhrase(office.canonicalMatcherKey, "lieutenant governor")) {
      score += isJointGovernorTicket ? -0.35 : 0.2;
    } else if (hasPhrase(office.canonicalMatcherKey, "governor")) {
      score += isJointGovernorTicket ? 0.2 : -0.35;
    }
  }

  if (hasPhrase(titleMatcherKey, "united states senator")) {
    if (hasPhrase(office.canonicalMatcherKey, "state senator")) {
      score -= 0.4;
    }
  }

  if (hasPhrase(titleMatcherKey, "state senator")) {
    if (hasPhrase(office.canonicalMatcherKey, "united states senator")) {
      score -= 0.4;
    }
  }

  if (hasPhrase(titleMatcherKey, "united states representative")) {
    if (hasPhrase(office.canonicalMatcherKey, "state representative")) {
      score -= 0.4;
    }
  }

  if (hasPhrase(titleMatcherKey, "state representative")) {
    if (hasPhrase(office.canonicalMatcherKey, "united states representative")) {
      score -= 0.4;
    }
  }

  return score;
}

export class OfficeMatcher {
  private readonly aliasByScope = new Map<ElectionDistrictType, Map<string, string>>();
  private readonly officesByScope = new Map<ElectionDistrictType, OfficeCandidate[]>();

  constructor(private readonly client: Pick<PoolClient, "query">) {}

  private async loadAliases(scope: ElectionDistrictType): Promise<Map<string, string>> {
    const cached = this.aliasByScope.get(scope);
    if (cached) {
      return cached;
    }

    const result = await this.client.query<OfficeAliasRow>(
      `
        SELECT office_id, normalized_alias
        FROM public.office_title_aliases
        WHERE scope = $1
      `,
      [scope]
    );

    const aliasMap = new Map<string, string>();
    for (const row of result.rows ?? []) {
      aliasMap.set(row.normalized_alias, row.office_id);
    }
    this.aliasByScope.set(scope, aliasMap);
    return aliasMap;
  }

  private async loadOffices(scope: ElectionDistrictType): Promise<OfficeCandidate[]> {
    const cached = this.officesByScope.get(scope);
    if (cached) {
      return cached;
    }

    const result = await this.client.query<OfficeRow>(
      `
        SELECT id, canonical_name
        FROM public.offices
        WHERE scope = $1
      `,
      [scope]
    );

    const offices = (result.rows ?? []).map((row) => {
      const matcherKey = toMatcherKeyFromCanonicalName(row.canonical_name);
      return {
        id: row.id,
        canonicalName: row.canonical_name,
        canonicalMatcherKey: matcherKey,
        canonicalTokens: toMatcherTokens(matcherKey),
      };
    });

    this.officesByScope.set(scope, offices);
    return offices;
  }

  rememberAlias(scope: ElectionDistrictType, normalizedAlias: string, officeId: string): void {
    if (!normalizedAlias) {
      return;
    }
    const existing = this.aliasByScope.get(scope);
    if (!existing) {
      return;
    }
    existing.set(normalizedAlias, officeId);
  }

  async resolve(input: OfficeMatchInput): Promise<OfficeMatchResult> {
    const normalizedAlias = normalizeElectionTitleKey(input.officialBallotTitle);
    if (normalizedAlias.length === 0) {
      return {
        officeId: null,
        method: "none",
        confidence: 0,
        normalizedAlias,
        aliasMemoryKey: "",
        shouldPersistAlias: false,
      };
    }

    const aliases = await this.loadAliases(input.scope);
    const titleMatcherKey = toMatcherKeyFromBallotTitle(input);
    let exactOfficeId = aliases.get(normalizedAlias);
    if (!exactOfficeId && titleMatcherKey.length > 0 && titleMatcherKey !== normalizedAlias) {
      exactOfficeId = aliases.get(titleMatcherKey);
    }
    if (!exactOfficeId && titleMatcherKey.length > 0) {
      const civicWordFreeKey = stripLeadingGenericCivicWords(titleMatcherKey);
      if (civicWordFreeKey !== titleMatcherKey && civicWordFreeKey.includes(" ")) {
        exactOfficeId = aliases.get(civicWordFreeKey);
      }
    }
    if (exactOfficeId && input.discoveryContestFamily === "non_judicial_office") {
      // A learned alias may point at a judge office (e.g. a Texas "County Judge",
      // the county executive, once mis-scored into County Level Judge). The entry's
      // own contest family is authoritative: ignore judge-office aliases here.
      const aliasTarget = (await this.loadOffices(input.scope)).find((office) => office.id === exactOfficeId);
      if (aliasTarget && isJudicialOfficeCanonicalName(aliasTarget.canonicalName)) {
        exactOfficeId = undefined;
      }
    }
    if (exactOfficeId && isCourtClerkTitle(titleMatcherKey)) {
      // Runs already learned the mis-scored alias ("county clerk of the
      // district court" -> County Clerk), and an exact alias hit outranks the
      // deterministic rule below. The title names the court itself, so it is
      // authoritative over a stored clerk office that names none.
      const aliasTarget = (await this.loadOffices(input.scope)).find((office) => office.id === exactOfficeId);
      if (aliasTarget && isNonCourtClerkOfficeKey(aliasTarget.canonicalMatcherKey)) {
        exactOfficeId = undefined;
      }
    }
    if (exactOfficeId) {
      return {
        officeId: exactOfficeId,
        method: "alias_exact",
        confidence: 1,
        normalizedAlias,
        aliasMemoryKey: titleMatcherKey.length > 0 ? titleMatcherKey : normalizedAlias,
        shouldPersistAlias: false,
      };
    }

    const offices = await this.loadOffices(input.scope);
    if (offices.length === 0) {
      return {
        officeId: null,
        method: "none",
        confidence: 0,
        normalizedAlias,
        aliasMemoryKey: titleMatcherKey,
        shouldPersistAlias: false,
      };
    }

    if (isSchoolDistrictScope(input.scope)) {
      const match = toSingleScopeOfficeMatch(
        findSingleScopeOffice(offices, SCHOOL_BOARD_CANONICAL_NAME),
        normalizedAlias,
        titleMatcherKey
      );
      if (match) {
        return match;
      }
    }

    if (input.scope === "us_house") {
      const match = toSingleScopeOfficeMatch(
        findSingleScopeOffice(offices, US_HOUSE_CANONICAL_NAME),
        normalizedAlias,
        titleMatcherKey
      );
      if (match) {
        return match;
      }
    }

    if (input.scope === "state_upper") {
      const match = toSingleScopeOfficeMatch(
        findSingleScopeOffice(offices, STATE_UPPER_CANONICAL_NAME),
        normalizedAlias,
        titleMatcherKey
      );
      if (match) {
        return match;
      }
    }

    if (input.scope === "state_lower") {
      const match = toSingleScopeOfficeMatch(
        findSingleScopeOffice(offices, STATE_LOWER_CANONICAL_NAME),
        normalizedAlias,
        titleMatcherKey
      );
      if (match) {
        return match;
      }
    }

    if (
      input.scope === "statewide" &&
      (isUsSenateOfficeTitle(input.officialBallotTitle) ||
        (input.discoveryContestFamily === "us_senate" && isUsSenateCompatibleTitle(titleMatcherKey)))
    ) {
      const match = toSingleScopeOfficeMatch(
        findSingleScopeOffice(offices, US_SENATE_CANONICAL_NAME),
        normalizedAlias,
        titleMatcherKey
      );
      if (match) {
        return match;
      }
    }

    // Washington's constitution makes each county's elected "Clerk" the
    // clerk of superior court. The bare word is otherwise genuinely
    // ambiguous with County Clerk, so keep this exact and state-scoped and do
    // not persist a global county alias that would affect other states.
    if (
      input.scope === "county" &&
      isWashingtonState(input.state) &&
      titleMatcherKey === "clerk"
    ) {
      const office = findSingleScopeOffice(offices, CLERK_OF_COURT_CANONICAL_NAME);
      if (office) {
        return {
          officeId: office.id,
          method: "deterministic_fallback",
          confidence: 1,
          normalizedAlias,
          aliasMemoryKey: titleMatcherKey,
          shouldPersistAlias: false,
        };
      }
    }

    // A county title that names a court's clerk is that court's clerk, however
    // the state words it ("Clerk of the District Court", "Clerk of Circuit
    // Court", "Clerk of Courts", "Circuit Court Clerk"). The token scorer
    // cannot see past the generic county-clerk prefix, so decide it here.
    if (input.scope === "county" && isCourtClerkTitle(titleMatcherKey)) {
      const office = findSingleScopeOffice(offices, CLERK_OF_COURT_CANONICAL_NAME);
      if (office) {
        return {
          officeId: office.id,
          method: "deterministic_fallback",
          confidence: 1,
          normalizedAlias,
          aliasMemoryKey: titleMatcherKey,
          shouldPersistAlias: false,
        };
      }
    }

    // Ahead of the generic judge fallback: a justice-of-the-peace seat is the
    // JP office, not the county trial court. Gated on the entry's contest
    // family the same way every other judge-office route is — a non-judicial
    // entry falls through to scoring, where the JP office is filtered out and
    // the title honestly fails to match rather than being guessed.
    if (
      input.scope === "county" &&
      input.discoveryContestFamily !== "non_judicial_office" &&
      isJusticeOfThePeaceTitle(titleMatcherKey)
    ) {
      const match = toSingleScopeOfficeMatch(
        findSingleScopeOffice(offices, JUSTICE_OF_THE_PEACE_CANONICAL_NAME),
        normalizedAlias,
        titleMatcherKey
      );
      if (match) {
        return match;
      }
    }

    if (
      input.discoveryContestFamily === "judicial_office" &&
      isJudicialCompatibleTitle(titleMatcherKey)
    ) {
      const judgeCanonicalName = judgeCanonicalNameForScope(input.scope);
      if (judgeCanonicalName) {
        const match = toSingleScopeOfficeMatch(
          findSingleScopeOffice(offices, judgeCanonicalName),
          normalizedAlias,
          titleMatcherKey
        );
        if (match) {
          return match;
        }
      }
    }

    const titleTokens = toMatcherTokens(titleMatcherKey);
    // The token scorer can pull judicial-sounding executive titles (Texas "County
    // Judge") into a judge office; the entry's non-judicial contest family wins.
    const scoreableOffices =
      input.discoveryContestFamily === "non_judicial_office"
        ? offices.filter((office) => !isJudicialOfficeCanonicalName(office.canonicalName))
        : offices;
    const scored = scoreableOffices
      .map((office) => ({
        officeId: office.id,
        score: scoreOfficeMatch(titleMatcherKey, titleTokens, office),
      }))
      .sort((a, b) => b.score - a.score);

    const top = scored[0];
    if (!top || top.score < MIN_CONFIDENCE) {
      return {
        officeId: null,
        method: "none",
        confidence: top?.score ?? 0,
        normalizedAlias,
        aliasMemoryKey: titleMatcherKey,
        shouldPersistAlias: false,
      };
    }

    const second = scored[1];
    if (second && top.score - second.score < MIN_MARGIN) {
      return {
        officeId: null,
        method: "ambiguous",
        confidence: top.score,
        normalizedAlias,
        aliasMemoryKey: titleMatcherKey,
        shouldPersistAlias: false,
      };
    }

    return {
      officeId: top.officeId,
      method: "deterministic_fallback",
      confidence: top.score,
      normalizedAlias,
      aliasMemoryKey: titleMatcherKey,
      shouldPersistAlias: true,
    };
  }
}
