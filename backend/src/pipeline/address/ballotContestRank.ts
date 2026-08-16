import type { BallotLookupElectionSummary } from "./ballotLookup.js";

// ---------------------------------------------------------------------------
// State-baseline contest ranking for the `state_baseline` ballot sort: the
// generic order contests appear on US general-election ballots — federal,
// then statewide, then state legislature, then county, then municipal, then
// school districts, then ALL judicial races as a late block (highest court
// first), and ballot measures last.
//
// Judicial-at-the-end is the majority pattern, not a simplification: every
// retention state prints retention questions as a section after the other
// offices, and the nonpartisan-section states do the same for contested
// judicial races (MN Rule 8250.1810 subp 5 puts Judicial Offices dead last;
// Ohio RC 3505.04 and Michigan's ballot standards put judges on the
// nonpartisan ballot after every partisan office). Placing judges inside
// their level (TX Elec. Code 52.092, NC GS 163-165.6 style) is the minority.
//
// This is an APPROXIMATION of each state's ballot-arrangement law, good
// enough to render a ballot-shaped preview; it is not a per-state statute
// encoding. When a state's real order diverges enough to matter, add a
// per-state override here (keyed by district.state_fips) rather than
// widening the generic tiers — and validate it against that state's current
// official sample ballots, not statute text alone (several states delegate
// detail to secretary-of-state or election-board rules).
// ---------------------------------------------------------------------------

// Sparse tiers so a future per-state override can slot a contest between two
// generic levels without renumbering.
const MEASURE_RANK = 100;
const UNKNOWN_RANK = 95;

// Rank of a summary election under the generic baseline. Lower = earlier on
// the ballot.
export function stateBaselineContestRank(
  election: Pick<BallotLookupElectionSummary, "race_type" | "discovery_contest_family" | "district" | "office">
): number {
  if (election.race_type === "ballot_measure") {
    return MEASURE_RANK;
  }
  // The office scope names WHAT is elected; the district type only says where
  // the row is attached (a county row carries municipal ward seats too), so
  // scope wins and district_type is the fallback for unresolved offices.
  const scope = election.office?.scope ?? election.district.district_type;
  // The judicial block: 82-90, after school (80), before unknown (95).
  // Within the block, higher courts come first — the internal order every
  // judicial-section state uses (MN: Chief Justice -> Court of Appeals ->
  // District Court; same shape in MI and CA).
  const judicial = election.discovery_contest_family === "judicial_office";
  switch (scope) {
    case "presidential":
      return 0;
    case "statewide":
      // US Senate is a statewide-scope office but federal on the ballot.
      if (election.discovery_contest_family === "us_senate") {
        return 10;
      }
      return judicial ? 82 : 30;
    case "us_house":
      return 20;
    case "state_upper":
      return judicial ? 84 : 40;
    case "state_lower":
      return judicial ? 85 : 50;
    case "county":
      return judicial ? 86 : 60;
    case "place":
      return judicial ? 88 : 70;
    case "school_elementary":
    case "school_secondary":
    case "school_unified":
      return 80;
    default:
      // Open union guard: an unmodeled scope sorts just above measures
      // instead of throwing the whole ballot render away.
      return judicial ? 90 : UNKNOWN_RANK;
  }
}
