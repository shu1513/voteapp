import { describe, expect, it } from "vitest";

import { stateBaselineContestRank } from "../../../src/pipeline/address/ballotContestRank.js";
import type { BallotLookupElectionSummary } from "../../../src/pipeline/address/ballotLookup.js";

type RankInput = Pick<
  BallotLookupElectionSummary,
  "race_type" | "discovery_contest_family" | "district" | "office"
>;

function input(overrides: {
  race_type?: string;
  contest_family?: string | null;
  office_scope?: string | null;
  district_type?: string;
}): RankInput {
  return {
    race_type: (overrides.race_type ?? "office") as RankInput["race_type"],
    discovery_contest_family: (overrides.contest_family ??
      "non_judicial_office") as RankInput["discovery_contest_family"],
    district: {
      id: "11111111-1111-4111-8111-111111111111",
      district_type: (overrides.district_type ?? "county") as RankInput["district"]["district_type"],
      geoid_compact: "06037",
      name: "Los Angeles County",
      state: "CA",
      state_fips: "06",
      representation_power_score: null,
      population: null,
    },
    office: overrides.office_scope
      ? {
          id: "12121212-1212-4212-8212-121212121212",
          scope: overrides.office_scope as NonNullable<RankInput["office"]>["scope"],
          canonical_name: "Office",
          summary: "",
        }
      : null,
  };
}

describe("stateBaselineContestRank", () => {
  it("ranks the generic ballot order: federal, statewide, legislature, county, municipal, school, measures", () => {
    const ranks = [
      input({ office_scope: "presidential" }),
      input({ office_scope: "statewide", contest_family: "us_senate" }),
      input({ office_scope: "us_house" }),
      input({ office_scope: "statewide" }),
      input({ office_scope: "state_upper" }),
      input({ office_scope: "state_lower" }),
      input({ office_scope: "county" }),
      input({ office_scope: "place" }),
      input({ office_scope: "school_unified" }),
      input({ race_type: "ballot_measure", contest_family: "ballot_measure", office_scope: null }),
    ].map(stateBaselineContestRank);

    const sorted = [...ranks].sort((a, b) => a - b);
    expect(ranks).toEqual(sorted);
    // Strictly increasing: every tier is distinct.
    expect(new Set(ranks).size).toBe(ranks.length);
  });

  it("ranks judicial races after non-judicial races of the same level, before the next level", () => {
    const statewideJudge = stateBaselineContestRank(
      input({ office_scope: "statewide", contest_family: "judicial_office" })
    );
    expect(statewideJudge).toBeGreaterThan(stateBaselineContestRank(input({ office_scope: "statewide" })));
    expect(statewideJudge).toBeLessThan(stateBaselineContestRank(input({ office_scope: "state_upper" })));

    const countyJudge = stateBaselineContestRank(
      input({ office_scope: "county", contest_family: "judicial_office" })
    );
    expect(countyJudge).toBeGreaterThan(stateBaselineContestRank(input({ office_scope: "county" })));
    expect(countyJudge).toBeLessThan(stateBaselineContestRank(input({ office_scope: "place" })));
  });

  it("falls back to the district type when the office is unresolved", () => {
    expect(stateBaselineContestRank(input({ office_scope: null, district_type: "us_house" }))).toBe(
      stateBaselineContestRank(input({ office_scope: "us_house" }))
    );
  });

  it("sorts unknown scopes just above measures instead of throwing", () => {
    const unknown = stateBaselineContestRank(input({ office_scope: "something_new" }));
    expect(unknown).toBeGreaterThan(stateBaselineContestRank(input({ office_scope: "school_unified" })));
    expect(unknown).toBeLessThan(
      stateBaselineContestRank(input({ race_type: "ballot_measure", office_scope: null }))
    );
  });
});
