import type { BallotLookupElectionSummary } from "./ballotLookup.js";

// ---------------------------------------------------------------------------
// State-baseline contest ranking for the `state_baseline` ballot sort: the
// generic order contests appear on US general-election ballots — federal,
// then statewide, then state legislature, then county, then municipal, then
// school districts, with judicial races after the non-judicial races of the
// same level and ballot measures last.
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
  const judicialBump = election.discovery_contest_family === "judicial_office" ? 5 : 0;
  switch (scope) {
    case "presidential":
      return 0;
    case "statewide":
      // US Senate is a statewide-scope office but federal on the ballot.
      return election.discovery_contest_family === "us_senate" ? 10 : 30 + judicialBump;
    case "us_house":
      return 20;
    case "state_upper":
      return 40 + judicialBump;
    case "state_lower":
      return 50 + judicialBump;
    case "county":
      return 60 + judicialBump;
    case "place":
      return 70 + judicialBump;
    case "school_elementary":
    case "school_secondary":
    case "school_unified":
      return 80;
    default:
      // Open union guard: an unmodeled scope sorts just above measures
      // instead of throwing the whole ballot render away.
      return UNKNOWN_RANK;
  }
}
