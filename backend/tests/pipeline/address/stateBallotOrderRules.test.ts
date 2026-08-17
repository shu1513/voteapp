import { describe, expect, it } from "vitest";

import { stateBaselineContestRank } from "../../../src/pipeline/address/ballotContestRank.js";
import {
  OVERRIDDEN_STATE_FIPS,
  stateBallotContestRank,
  type StateRankableElection,
} from "../../../src/pipeline/address/stateBallotOrderRules.js";

type InputOverrides = {
  state_fips?: string;
  election_stage?: string | null;
  election_date?: string;
  race_type?: string;
  contest_family?: string | null;
  office_scope?: string | null;
  district_type?: string;
  title?: string;
};

function input(overrides: InputOverrides): StateRankableElection {
  return {
    official_ballot_title: overrides.title ?? "Office Title",
    race_type: (overrides.race_type ?? "office") as StateRankableElection["race_type"],
    election_stage: (overrides.election_stage === undefined
      ? "general"
      : overrides.election_stage) as StateRankableElection["election_stage"],
    election_date: overrides.election_date ?? "2026-11-03",
    discovery_contest_family: (overrides.contest_family ??
      (overrides.race_type === "ballot_measure"
        ? "ballot_measure"
        : "non_judicial_office")) as StateRankableElection["discovery_contest_family"],
    district: {
      id: "11111111-1111-4111-8111-111111111111",
      district_type: (overrides.district_type ??
        "county") as StateRankableElection["district"]["district_type"],
      geoid_compact: "99999",
      name: "Test District",
      state: "XX",
      state_fips: overrides.state_fips ?? "42",
      representation_power_score: null,
      population: null,
    },
    office:
      overrides.office_scope === null || overrides.race_type === "ballot_measure"
        ? null
        : {
            id: "12121212-1212-4212-8212-121212121212",
            scope: (overrides.office_scope ??
              "county") as NonNullable<StateRankableElection["office"]>["scope"],
            canonical_name: "Office",
            summary: "",
          },
  };
}

// Rank under the given state's general-election rules.
function rank(state_fips: string, overrides: InputOverrides): number {
  return stateBallotContestRank(input({ ...overrides, state_fips }));
}

// Common probe contests, one per baseline tier plus judicial/measure
// variants — used by the gate and no-row sweeps to compare full profiles.
const PROBES: InputOverrides[] = [
  { office_scope: "presidential", title: "President of the United States" },
  { office_scope: "statewide", contest_family: "us_senate", title: "United States Senator" },
  { office_scope: "us_house", title: "Representative in Congress" },
  { office_scope: "statewide", title: "Governor" },
  { office_scope: "statewide", title: "Secretary of State" },
  { office_scope: "state_upper", title: "State Senator" },
  { office_scope: "state_lower", title: "State Representative" },
  { office_scope: "county", title: "County Commissioner" },
  { office_scope: "place", title: "Mayor" },
  { office_scope: "school_unified", title: "School Board Member" },
  { office_scope: "statewide", contest_family: "judicial_office", title: "Justice of the Supreme Court" },
  { office_scope: "statewide", contest_family: "judicial_office", title: "Judge of the Court of Appeals" },
  { office_scope: "county", contest_family: "judicial_office", title: "Circuit Court Judge" },
  { office_scope: "place", contest_family: "judicial_office", title: "Municipal Court Judge" },
  { race_type: "ballot_measure", office_scope: null, district_type: "statewide", title: "Amendment 1" },
  { race_type: "ballot_measure", office_scope: null, district_type: "county", title: "County Question 1" },
  { race_type: "ballot_measure", office_scope: null, district_type: "place", title: "City Question 1" },
];

describe("stateBallotContestRank gating", () => {
  it("applies overrides only when election_stage is 'general'", () => {
    for (const fips of OVERRIDDEN_STATE_FIPS) {
      for (const probe of PROBES) {
        const baseline = stateBaselineContestRank(input(probe));
        for (const stage of ["primary", "runoff", "special", null]) {
          expect(rank(fips, { ...probe, election_stage: stage })).toBe(baseline);
        }
      }
    }
  });

  it("every override entry moves at least one probe contest on a general", () => {
    // Guards against dead entries (a rule that never fires is either a typo'd
    // FIPS key or an encoding mistake). NM is cycle-gated to presidential
    // years, so the sweep probes a presidential-year date.
    for (const fips of OVERRIDDEN_STATE_FIPS) {
      const moved = PROBES.some((probe) => {
        const generalRank = rank(fips, { ...probe, election_date: "2028-11-07" });
        return generalRank !== stateBaselineContestRank(input({ ...probe, election_date: "2028-11-07" }));
      });
      expect(moved, `override for FIPS ${fips} never fires`).toBe(true);
    }
  });

  it("falls back to the baseline for states without an entry", () => {
    // Grade-B/C states (PA, ID, MO, AR) and the grade-A states whose
    // verified order matches the baseline (NY, GA, DE, RI, CO, MS, WV, ND,
    // AK, KY) — plus a FIPS with no entry at all.
    const NO_ROW_FIPS = ["42", "16", "29", "05", "36", "13", "10", "44", "08", "28", "54", "38", "02", "21", "72"];
    for (const fips of NO_ROW_FIPS) {
      expect(OVERRIDDEN_STATE_FIPS).not.toContain(fips);
      for (const probe of PROBES) {
        expect(rank(fips, probe)).toBe(stateBaselineContestRank(input(probe)));
      }
    }
  });
});

describe("per-state deviations", () => {
  it("AL: Governor/LtGov above US Senate; appellate after legislature; trial before county", () => {
    expect(rank("01", { office_scope: "statewide", title: "Governor" })).toBeLessThan(
      rank("01", { office_scope: "statewide", contest_family: "us_senate", title: "United States Senator" })
    );
    expect(rank("01", { office_scope: "statewide", title: "Lieutenant Governor" })).toBeLessThan(
      rank("01", { office_scope: "statewide", contest_family: "us_senate", title: "United States Senator" })
    );
    // Other executives are NOT moved (split second run — below granularity).
    expect(rank("01", { office_scope: "statewide", title: "Secretary of State" })).toBe(
      stateBaselineContestRank(input({ office_scope: "statewide", title: "Secretary of State" }))
    );
    const supreme = rank("01", {
      office_scope: "statewide",
      contest_family: "judicial_office",
      title: "Chief Justice of the Supreme Court",
    });
    expect(supreme).toBeGreaterThan(rank("01", { office_scope: "state_lower", title: "State Representative" }));
    const trial = rank("01", { office_scope: "county", contest_family: "judicial_office", title: "Circuit Judge" });
    expect(trial).toBeGreaterThan(supreme);
    expect(trial).toBeLessThan(rank("01", { office_scope: "county", title: "County Commission" }));
  });

  it("AZ: only Governor precedes the legislature; retention opens the nonpartisan tail; municipal last", () => {
    const governor = rank("04", { office_scope: "statewide", title: "Governor" });
    const sos = rank("04", { office_scope: "statewide", title: "Secretary of State" });
    expect(governor).toBeLessThan(rank("04", { office_scope: "state_upper", title: "State Senator" }));
    expect(sos).toBeGreaterThan(rank("04", { office_scope: "state_lower", title: "State Representative" }));
    const retention = rank("04", {
      office_scope: "statewide",
      contest_family: "judicial_office",
      title: "Justice of the Supreme Court",
    });
    expect(retention).toBeGreaterThan(rank("04", { office_scope: "county", title: "County Recorder" }));
    expect(retention).toBeLessThan(rank("04", { office_scope: "school_unified", title: "School Board" }));
    // Municipal after school, before measures.
    const city = rank("04", { office_scope: "place", title: "City Councilmember" });
    expect(city).toBeGreaterThan(rank("04", { office_scope: "school_unified", title: "School Board" }));
    expect(city).toBeLessThan(rank("04", { race_type: "ballot_measure", office_scope: null, district_type: "statewide" }));
    // Contested Superior Court placement is A-excluded: county judicial stays baseline.
    expect(rank("04", { office_scope: "county", contest_family: "judicial_office", title: "Superior Court Judge" })).toBe(
      stateBaselineContestRank(
        input({ office_scope: "county", contest_family: "judicial_office", title: "Superior Court Judge" })
      )
    );
  });

  it("CA: executives before US Senate; judicial after legislature; school before county and city", () => {
    expect(rank("06", { office_scope: "statewide", title: "Governor" })).toBeLessThan(
      rank("06", { office_scope: "statewide", contest_family: "us_senate", title: "United States Senator" })
    );
    const judge = rank("06", { office_scope: "county", contest_family: "judicial_office", title: "Superior Court Judge" });
    expect(judge).toBeGreaterThan(rank("06", { office_scope: "state_lower", title: "Member of the State Assembly" }));
    // Superintendent of Public Instruction (statewide-scoped office) heads
    // the SCHOOL block instead of joining the executive run (§ 13109(j)).
    const superintendent = rank("06", { office_scope: "statewide", title: "Superintendent of Public Instruction" });
    expect(superintendent).toBeGreaterThan(
      rank("06", { office_scope: "county", contest_family: "judicial_office", title: "Superior Court Judge" })
    );
    const schoolBoard = rank("06", { office_scope: "school_unified", title: "Governing Board Member" });
    expect(superintendent).toBeLessThan(schoolBoard);
    expect(judge).toBeLessThan(schoolBoard);
    expect(schoolBoard).toBeLessThan(rank("06", { office_scope: "county", title: "Board of Supervisors" }));
    expect(schoolBoard).toBeLessThan(rank("06", { office_scope: "place", title: "City Council" }));
  });

  it("CT: Gov/LtGov above US Senate; remaining executives below the legislature; probate judge last office", () => {
    expect(rank("09", { office_scope: "statewide", title: "Governor and Lieutenant Governor" })).toBeLessThan(
      rank("09", { office_scope: "statewide", contest_family: "us_senate", title: "United States Senator" })
    );
    const treasurer = rank("09", { office_scope: "statewide", title: "Treasurer" });
    expect(treasurer).toBeGreaterThan(rank("09", { office_scope: "state_lower", title: "State Representative" }));
    const probate = rank("09", { office_scope: "place", contest_family: "judicial_office", title: "Judge of Probate" });
    expect(probate).toBeGreaterThan(treasurer);
    expect(probate).toBeLessThan(rank("09", { race_type: "ballot_measure", office_scope: null, district_type: "statewide" }));
  });

  it("DC: shadow US Senator/Representative print late; the Delegate keeps the federal slot", () => {
    // Real scopes from the data: the Delegate is titled "United States
    // Representative, DC At-Large" on the us_house district; Mayor rides
    // the place-scoped city district; AG is statewide.
    const delegate = rank("11", { office_scope: "us_house", title: "United States Representative, DC At-Large" });
    const mayor = rank("11", { office_scope: "place", title: "Mayor of the District of Columbia" });
    const atLargeCouncil = rank("11", { office_scope: "statewide", title: "At-Large Member of the Council" });
    const wardCouncil = rank("11", { office_scope: "place", title: "Member of the Council Ward 3" });
    const attorneyGeneral = rank("11", { office_scope: "statewide", title: "Attorney General" });
    const shadowSenator = rank("11", {
      office_scope: "statewide",
      contest_family: "us_senate",
      title: "United States Senator",
    });
    const shadowRep = rank("11", { office_scope: "statewide", title: "United States Representative" });
    // § 1202.1 (b) -> (c) -> (e) -> (f) -> (g) -> (h) -> (i)
    expect(delegate).toBeLessThan(mayor);
    expect(mayor).toBeLessThan(atLargeCouncil);
    expect(atLargeCouncil).toBeLessThan(wardCouncil);
    expect(wardCouncil).toBeLessThan(attorneyGeneral);
    expect(attorneyGeneral).toBeLessThan(shadowSenator);
    expect(shadowSenator).toBeLessThan(shadowRep);
    // (j)/(k) SBOE after the shadow offices, regardless of modeled scope,
    // then (l) ANC as the final office block before measures.
    const sboe = rank("11", { office_scope: "statewide", title: "At-Large Member of the State Board of Education" });
    const anc = rank("11", { office_scope: "place", title: "Advisory Neighborhood Commissioner 3B01" });
    expect(sboe).toBeGreaterThan(shadowRep);
    expect(anc).toBeGreaterThan(sboe);
    expect(anc).toBeLessThan(
      rank("11", { race_type: "ballot_measure", office_scope: null, district_type: "statewide", title: "Initiative 83" })
    );
  });

  it("FL: the judicial section prints before school board", () => {
    const judge = rank("12", { office_scope: "county", contest_family: "judicial_office", title: "Circuit Judge" });
    expect(judge).toBeGreaterThan(rank("12", { office_scope: "place", title: "City Council" }));
    expect(judge).toBeLessThan(rank("12", { office_scope: "school_unified", title: "School Board" }));
  });

  it("HI: OHA trustees between state house and county; county charter questions after state amendments", () => {
    const oha = rank("15", { office_scope: "statewide", title: "Office of Hawaiian Affairs Trustee, At-Large" });
    expect(oha).toBeGreaterThan(rank("15", { office_scope: "state_lower", title: "State Representative" }));
    expect(oha).toBeLessThan(rank("15", { office_scope: "county", title: "Councilmember" }));
    // Non-OHA statewide contests keep the baseline slot.
    expect(rank("15", { office_scope: "statewide", title: "Governor" })).toBe(
      stateBaselineContestRank(input({ office_scope: "statewide", title: "Governor" }))
    );
    expect(rank("15", { race_type: "ballot_measure", office_scope: null, district_type: "statewide" })).toBeLessThan(
      rank("15", { race_type: "ballot_measure", office_scope: null, district_type: "county" })
    );
  });

  it("IL: statewide measures first; executives before US House; school after judicial", () => {
    expect(rank("17", { race_type: "ballot_measure", office_scope: null, district_type: "statewide" })).toBeLessThan(
      rank("17", { office_scope: "presidential", title: "President" })
    );
    // Local referenda still last.
    expect(rank("17", { race_type: "ballot_measure", office_scope: null, district_type: "place" })).toBeGreaterThan(
      rank("17", { office_scope: "school_unified", title: "Board of Education" })
    );
    const governor = rank("17", { office_scope: "statewide", title: "Governor" });
    expect(governor).toBeGreaterThan(
      rank("17", { office_scope: "statewide", contest_family: "us_senate", title: "United States Senator" })
    );
    expect(governor).toBeLessThan(rank("17", { office_scope: "us_house", title: "Representative in Congress" }));
    expect(rank("17", { office_scope: "school_unified", title: "Board of Education" })).toBeGreaterThan(
      rank("17", { office_scope: "county", contest_family: "judicial_office", title: "Judge of the Circuit Court" })
    );
  });

  it("IN: questions first; executives before US House; trial courts early, retention dead last", () => {
    expect(rank("18", { race_type: "ballot_measure", office_scope: null, district_type: "statewide" })).toBeLessThan(
      rank("18", { race_type: "ballot_measure", office_scope: null, district_type: "county" })
    );
    expect(rank("18", { race_type: "ballot_measure", office_scope: null, district_type: "county" })).toBeLessThan(
      rank("18", { office_scope: "presidential", title: "President" })
    );
    const governor = rank("18", { office_scope: "statewide", title: "Governor" });
    expect(governor).toBeLessThan(rank("18", { office_scope: "us_house", title: "Representative in Congress" }));
    const trial = rank("18", { office_scope: "county", contest_family: "judicial_office", title: "Judge of the Circuit Court" });
    expect(trial).toBeGreaterThan(rank("18", { office_scope: "state_lower", title: "State Representative" }));
    expect(trial).toBeLessThan(rank("18", { office_scope: "county", title: "County Auditor" }));
    const retention = rank("18", {
      office_scope: "statewide",
      contest_family: "judicial_office",
      title: "Justice of the Supreme Court",
    });
    expect(retention).toBeGreaterThan(rank("18", { office_scope: "school_unified", title: "School Board" }));
  });

  it("IA: retention after the township/special tier; measures state -> county -> city", () => {
    const retention = rank("19", {
      office_scope: "statewide",
      contest_family: "judicial_office",
      title: "Justice of the Supreme Court",
    });
    expect(retention).toBeGreaterThan(stateBaselineContestRank(input({ office_scope: "something_new" })));
    expect(retention).toBeLessThan(rank("19", { race_type: "ballot_measure", office_scope: null, district_type: "statewide" }));
    expect(rank("19", { race_type: "ballot_measure", office_scope: null, district_type: "statewide" })).toBeLessThan(
      rank("19", { race_type: "ballot_measure", office_scope: null, district_type: "county" })
    );
    expect(rank("19", { race_type: "ballot_measure", office_scope: null, district_type: "county" })).toBeLessThan(
      rank("19", { race_type: "ballot_measure", office_scope: null, district_type: "place" })
    );
  });

  it("KS: partisan district judges between state house and county; retention stays late", () => {
    const districtJudge = rank("20", {
      office_scope: "county",
      contest_family: "judicial_office",
      title: "District Court Judge",
    });
    expect(districtJudge).toBeGreaterThan(rank("20", { office_scope: "state_lower", title: "State Representative" }));
    expect(districtJudge).toBeLessThan(rank("20", { office_scope: "county", title: "County Clerk" }));
    expect(
      rank("20", { office_scope: "statewide", contest_family: "judicial_office", title: "Justice of the Supreme Court" })
    ).toBe(
      stateBaselineContestRank(
        input({ office_scope: "statewide", contest_family: "judicial_office", title: "Justice of the Supreme Court" })
      )
    );
  });

  it("LA: executives above US Senate; appellate in the state block; trial atop the parish block; school before municipal", () => {
    expect(rank("22", { office_scope: "statewide", title: "Governor" })).toBeLessThan(
      rank("22", { office_scope: "statewide", contest_family: "us_senate", title: "United States Senator" })
    );
    const appellate = rank("22", {
      office_scope: "statewide",
      contest_family: "judicial_office",
      title: "Judge, Court of Appeal",
    });
    expect(appellate).toBeGreaterThan(rank("22", { office_scope: "us_house", title: "United States Representative" }));
    expect(appellate).toBeLessThan(rank("22", { office_scope: "state_upper", title: "State Senator" }));
    const trial = rank("22", { office_scope: "county", contest_family: "judicial_office", title: "District Judge" });
    expect(trial).toBeLessThan(rank("22", { office_scope: "county", title: "Sheriff" }));
    const schoolBoard = rank("22", { office_scope: "school_unified", title: "School Board Member" });
    expect(schoolBoard).toBeGreaterThan(rank("22", { office_scope: "county", title: "Sheriff" }));
    expect(schoolBoard).toBeLessThan(rank("22", { office_scope: "place", title: "Mayor" }));
  });

  it("ME: Governor between US Senate and US House; probate judge heads the county block", () => {
    const governor = rank("23", { office_scope: "statewide", title: "Governor" });
    expect(governor).toBeGreaterThan(
      rank("23", { office_scope: "statewide", contest_family: "us_senate", title: "United States Senator" })
    );
    expect(governor).toBeLessThan(rank("23", { office_scope: "us_house", title: "Representative to Congress" }));
    const probate = rank("23", { office_scope: "county", contest_family: "judicial_office", title: "Judge of Probate" });
    expect(probate).toBeGreaterThan(rank("23", { office_scope: "state_lower", title: "State Representative" }));
    expect(probate).toBeLessThan(rank("23", { office_scope: "county", title: "Sheriff" }));
  });

  it("MD: executives above US Senate; judicial mid-ballot with trial before Supreme before Appellate", () => {
    expect(rank("24", { office_scope: "statewide", title: "Governor" })).toBeLessThan(
      rank("24", { office_scope: "statewide", contest_family: "us_senate", title: "United States Senator" })
    );
    const circuit = rank("24", { office_scope: "county", contest_family: "judicial_office", title: "Judge of the Circuit Court" });
    const supreme = rank("24", {
      office_scope: "statewide",
      contest_family: "judicial_office",
      title: "Justice of the Supreme Court of Maryland",
    });
    const appellate = rank("24", {
      office_scope: "statewide",
      contest_family: "judicial_office",
      title: "Judge of the Appellate Court of Maryland",
    });
    expect(circuit).toBeLessThan(supreme);
    expect(supreme).toBeLessThan(appellate);
    expect(appellate).toBeLessThan(rank("24", { office_scope: "county", title: "County Executive" }));
    expect(circuit).toBeGreaterThan(rank("24", { office_scope: "state_lower", title: "State Delegate" }));
  });

  it("MA: executives between US Senate and US House", () => {
    const governor = rank("25", { office_scope: "statewide", title: "Governor" });
    expect(governor).toBeGreaterThan(
      rank("25", { office_scope: "statewide", contest_family: "us_senate", title: "Senator in Congress" })
    );
    expect(governor).toBeLessThan(rank("25", { office_scope: "us_house", title: "Representative in Congress" }));
  });

  it("MI: Gov/SOS/AG before US Senate, education boards after the legislature; judicial before school", () => {
    expect(rank("26", { office_scope: "statewide", title: "Governor" })).toBeLessThan(
      rank("26", { office_scope: "statewide", contest_family: "us_senate", title: "United States Senator" })
    );
    const regent = rank("26", { office_scope: "statewide", title: "Regent of the University of Michigan" });
    expect(regent).toBeGreaterThan(rank("26", { office_scope: "state_lower", title: "State Representative" }));
    expect(regent).toBeLessThan(rank("26", { office_scope: "county", title: "County Commissioner" }));
    const wsuGovernor = rank("26", { office_scope: "statewide", title: "Governor of Wayne State University" });
    expect(wsuGovernor).toBe(regent);
    const judge = rank("26", { office_scope: "county", contest_family: "judicial_office", title: "Judge of Circuit Court" });
    expect(judge).toBeGreaterThan(rank("26", { office_scope: "place", title: "City Council" }));
    expect(judge).toBeLessThan(rank("26", { office_scope: "school_unified", title: "Board of Education" }));
  });

  it("MN: legislature before executives; statewide amendments before county; judicial dead last", () => {
    const governor = rank("27", { office_scope: "statewide", title: "Governor" });
    expect(governor).toBeGreaterThan(rank("27", { office_scope: "state_lower", title: "State Representative" }));
    const amendment = rank("27", { race_type: "ballot_measure", office_scope: null, district_type: "statewide" });
    expect(amendment).toBeGreaterThan(governor);
    expect(amendment).toBeLessThan(rank("27", { office_scope: "county", title: "County Commissioner" }));
    const judge = rank("27", { office_scope: "county", contest_family: "judicial_office", title: "Judge, District Court" });
    expect(judge).toBeGreaterThan(rank("27", { race_type: "ballot_measure", office_scope: null, district_type: "place" }));
  });

  it("MT: judicial between executives and legislature; JP is the last county office", () => {
    const supreme = rank("30", {
      office_scope: "statewide",
      contest_family: "judicial_office",
      title: "Justice of the Supreme Court",
    });
    expect(supreme).toBeGreaterThan(rank("30", { office_scope: "statewide", title: "Governor" }));
    expect(supreme).toBeLessThan(rank("30", { office_scope: "state_upper", title: "State Senator" }));
    const jp = rank("30", { office_scope: "county", contest_family: "judicial_office", title: "Justice of the Peace" });
    expect(jp).toBeGreaterThan(rank("30", { office_scope: "county", title: "County Commissioner" }));
    expect(jp).toBeLessThan(rank("30", { office_scope: "place", title: "Mayor" }));
  });

  it("NE: the statewide-measure ballot comes last, after local measures", () => {
    expect(rank("31", { race_type: "ballot_measure", office_scope: null, district_type: "statewide" })).toBeGreaterThan(
      rank("31", { race_type: "ballot_measure", office_scope: null, district_type: "place" })
    );
  });

  it("NV: judicial early, school after judicial before municipal, JPs last among offices", () => {
    const district = rank("32", { office_scope: "county", contest_family: "judicial_office", title: "District Court Judge" });
    expect(district).toBeGreaterThan(rank("32", { office_scope: "county", title: "County Commissioner" }));
    const schoolBoard = rank("32", { office_scope: "school_unified", title: "School Board Trustee" });
    expect(schoolBoard).toBeGreaterThan(district);
    expect(schoolBoard).toBeLessThan(rank("32", { office_scope: "place", title: "City Council" }));
    const jp = rank("32", { office_scope: "place", contest_family: "judicial_office", title: "Justice of the Peace" });
    expect(jp).toBeGreaterThan(rank("32", { office_scope: "place", title: "City Council" }));
    expect(jp).toBeLessThan(rank("32", { race_type: "ballot_measure", office_scope: null, district_type: "statewide" }));
  });

  it("NH: Governor in slot 2, before US Senate", () => {
    const governor = rank("33", { office_scope: "statewide", title: "Governor" });
    expect(governor).toBeGreaterThan(rank("33", { office_scope: "presidential", title: "President" }));
    expect(governor).toBeLessThan(
      rank("33", { office_scope: "statewide", contest_family: "us_senate", title: "United States Senator" })
    );
  });

  it("NJ: measures run statewide -> municipal -> county", () => {
    const statewide = rank("34", { race_type: "ballot_measure", office_scope: null, district_type: "statewide" });
    const municipal = rank("34", { race_type: "ballot_measure", office_scope: null, district_type: "place" });
    const county = rank("34", { race_type: "ballot_measure", office_scope: null, district_type: "county" });
    expect(statewide).toBeLessThan(municipal);
    expect(municipal).toBeLessThan(county);
  });

  it("NM (presidential years): partisan judicial before county; retention leads the question block", () => {
    const presYear = { election_date: "2028-11-07" };
    const partisanJudge = rank("35", {
      ...presYear,
      office_scope: "county",
      contest_family: "judicial_office",
      title: "Judge of the District Court",
    });
    expect(partisanJudge).toBeGreaterThan(rank("35", { ...presYear, office_scope: "state_lower", title: "State Representative" }));
    expect(partisanJudge).toBeLessThan(rank("35", { ...presYear, office_scope: "county", title: "County Clerk" }));
    const retention = rank("35", {
      ...presYear,
      office_scope: "statewide",
      contest_family: "judicial_office",
      title: "Judicial Retention - Court of Appeals",
    });
    expect(retention).toBeGreaterThan(rank("35", { ...presYear, office_scope: "county", title: "County Clerk" }));
    expect(retention).toBeLessThan(
      rank("35", { ...presYear, race_type: "ballot_measure", office_scope: null, district_type: "statewide" })
    );
    // The standard question wording counts as retention too (shared matcher).
    const questionForm = rank("35", {
      ...presYear,
      office_scope: "statewide",
      contest_family: "judicial_office",
      title: "Shall J. Miles Hanisee be retained as a Judge of the Court of Appeals?",
    });
    expect(questionForm).toBe(retention);
    // Gubernatorial cycles are A-excluded: the whole entry defers to baseline.
    expect(rank("35", { election_date: "2026-11-03", office_scope: "county", contest_family: "judicial_office", title: "Judge of the District Court" })).toBe(
      stateBaselineContestRank(
        input({ election_date: "2026-11-03", office_scope: "county", contest_family: "judicial_office", title: "Judge of the District Court" })
      )
    );
  });

  it("NC: appellate courts before the legislature; trial courts between state house and county", () => {
    const appellate = rank("37", {
      office_scope: "statewide",
      contest_family: "judicial_office",
      title: "Judge of the Court of Appeals",
    });
    expect(appellate).toBeGreaterThan(rank("37", { office_scope: "statewide", title: "Commissioner of Agriculture" }));
    expect(appellate).toBeLessThan(rank("37", { office_scope: "state_upper", title: "State Senator" }));
    const trial = rank("37", { office_scope: "county", contest_family: "judicial_office", title: "District Court Judge" });
    expect(trial).toBeGreaterThan(rank("37", { office_scope: "state_lower", title: "State Representative" }));
    expect(trial).toBeLessThan(rank("37", { office_scope: "county", title: "Register of Deeds" }));
  });

  it("OH: executives then Supreme Court then US Senate; appeals after state house; trial courts before school", () => {
    const governor = rank("39", { office_scope: "statewide", title: "Governor and Lieutenant Governor" });
    const supreme = rank("39", {
      office_scope: "statewide",
      contest_family: "judicial_office",
      title: "Justice of the Supreme Court",
    });
    const senate = rank("39", { office_scope: "statewide", contest_family: "us_senate", title: "United States Senator" });
    expect(governor).toBeLessThan(supreme);
    expect(supreme).toBeLessThan(senate);
    const appeals = rank("39", {
      office_scope: "statewide",
      contest_family: "judicial_office",
      title: "Judge of the Court of Appeals",
    });
    expect(appeals).toBeGreaterThan(rank("39", { office_scope: "state_lower", title: "State Representative" }));
    expect(appeals).toBeLessThan(rank("39", { office_scope: "county", title: "County Auditor" }));
    const trial = rank("39", { office_scope: "county", contest_family: "judicial_office", title: "Judge of the Court of Common Pleas" });
    expect(trial).toBeGreaterThan(rank("39", { office_scope: "place", title: "City Council" }));
    expect(trial).toBeLessThan(rank("39", { office_scope: "school_unified", title: "Board of Education" }));
  });

  it("OK: executives before US Senate; trial then appellate retention after county, before State Questions", () => {
    expect(rank("40", { office_scope: "statewide", title: "Governor" })).toBeLessThan(
      rank("40", { office_scope: "statewide", contest_family: "us_senate", title: "United States Senator" })
    );
    const trial = rank("40", { office_scope: "county", contest_family: "judicial_office", title: "District Judge" });
    const retention = rank("40", {
      office_scope: "statewide",
      contest_family: "judicial_office",
      title: "Justice of the Supreme Court",
    });
    expect(trial).toBeGreaterThan(rank("40", { office_scope: "county", title: "County Commissioner" }));
    expect(retention).toBeGreaterThan(trial);
    expect(retention).toBeLessThan(rank("40", { race_type: "ballot_measure", office_scope: null, district_type: "statewide" }));
  });

  it("OR: judicial before county and city contests", () => {
    const judge = rank("41", { office_scope: "county", contest_family: "judicial_office", title: "Judge of the Circuit Court" });
    expect(judge).toBeGreaterThan(rank("41", { office_scope: "state_lower", title: "State Representative" }));
    expect(judge).toBeLessThan(rank("41", { office_scope: "county", title: "County Commissioner" }));
  });

  it("SC: executives above US Senate; nothing below the executive move is encoded", () => {
    expect(rank("45", { office_scope: "statewide", title: "Governor" })).toBeLessThan(
      rank("45", { office_scope: "statewide", contest_family: "us_senate", title: "United States Senator" })
    );
    // County-and-below is A-excluded (county-arranged, grade B).
    expect(rank("45", { office_scope: "county", contest_family: "judicial_office", title: "Probate Judge" })).toBe(
      stateBaselineContestRank(
        input({ office_scope: "county", contest_family: "judicial_office", title: "Probate Judge" })
      )
    );
  });

  it("SD: county questions after statewide measures", () => {
    expect(rank("46", { race_type: "ballot_measure", office_scope: null, district_type: "county" })).toBeGreaterThan(
      rank("46", { race_type: "ballot_measure", office_scope: null, district_type: "statewide" })
    );
  });

  it("TN: Governor slot 2; state amendments right behind; judicial after state house; school inside the county block", () => {
    const governor = rank("47", { office_scope: "statewide", title: "Governor" });
    expect(governor).toBeLessThan(
      rank("47", { office_scope: "statewide", contest_family: "us_senate", title: "United States Senator" })
    );
    const amendment = rank("47", { race_type: "ballot_measure", office_scope: null, district_type: "statewide" });
    expect(amendment).toBeGreaterThan(governor);
    expect(amendment).toBeLessThan(
      rank("47", { office_scope: "statewide", contest_family: "us_senate", title: "United States Senator" })
    );
    // Local questions still trail.
    expect(rank("47", { race_type: "ballot_measure", office_scope: null, district_type: "county" })).toBeGreaterThan(
      rank("47", { office_scope: "place", title: "City Council" })
    );
    const supreme = rank("47", {
      office_scope: "statewide",
      contest_family: "judicial_office",
      title: "Justice of the Supreme Court",
    });
    expect(supreme).toBeGreaterThan(rank("47", { office_scope: "state_lower", title: "State Representative" }));
    expect(supreme).toBeLessThan(rank("47", { office_scope: "county", title: "County Commission" }));
    // County-scoped judicial splits by class: circuit/chancery/criminal
    // courts join the early block after the state house; general sessions
    // and juvenile judges ride behind the county line.
    const circuit = rank("47", { office_scope: "county", contest_family: "judicial_office", title: "Circuit Court Judge, Division 2" });
    expect(circuit).toBeGreaterThan(rank("47", { office_scope: "state_lower", title: "State Representative" }));
    expect(circuit).toBeLessThan(rank("47", { office_scope: "county", title: "County Commission" }));
    const generalSessions = rank("47", {
      office_scope: "county",
      contest_family: "judicial_office",
      title: "General Sessions Court Judge, Division 2",
    });
    expect(generalSessions).toBeGreaterThan(rank("47", { office_scope: "county", title: "County Commission" }));
    expect(generalSessions).toBeLessThan(rank("47", { office_scope: "place", title: "City Council" }));
    const schoolBoard = rank("47", { office_scope: "school_unified", title: "School Board Member" });
    expect(schoolBoard).toBeGreaterThan(rank("47", { office_scope: "county", title: "County Commission" }));
    expect(schoolBoard).toBeLessThan(rank("47", { office_scope: "place", title: "City Council" }));
  });

  it("TX: judicial within level at every tier", () => {
    const supreme = rank("48", {
      office_scope: "statewide",
      contest_family: "judicial_office",
      title: "Justice, Supreme Court",
    });
    expect(supreme).toBeGreaterThan(rank("48", { office_scope: "statewide", title: "Railroad Commissioner" }));
    expect(supreme).toBeLessThan(rank("48", { office_scope: "state_upper", title: "State Senator" }));
    // District judges arrive county-scoped (no judicial-district scope
    // exists); the title split sends them to the after-state-house slot.
    const district = rank("48", {
      office_scope: "county",
      contest_family: "judicial_office",
      title: "District Judge, 218th Judicial District",
    });
    expect(district).toBeGreaterThan(rank("48", { office_scope: "state_lower", title: "State Representative" }));
    expect(district).toBeLessThan(rank("48", { office_scope: "county", title: "County Clerk" }));
    // County courts lead the county block — and stay behind district courts.
    const countyCourt = rank("48", { office_scope: "county", contest_family: "judicial_office", title: "Judge, County Court at Law" });
    expect(countyCourt).toBeGreaterThan(district);
    expect(countyCourt).toBeLessThan(rank("48", { office_scope: "county", title: "County Clerk" }));
    // JPs are county-scoped precinct offices: after every county office,
    // before municipal.
    const jp = rank("48", { office_scope: "county", contest_family: "judicial_office", title: "Justice of the Peace, Precinct 1" });
    expect(jp).toBeGreaterThan(rank("48", { office_scope: "county", title: "County Clerk" }));
    expect(jp).toBeLessThan(rank("48", { office_scope: "place", title: "Mayor" }));
  });

  it("UT: school board at the end of the county block; retention after every candidate contest", () => {
    const schoolBoard = rank("49", { office_scope: "school_unified", title: "Local School Board" });
    expect(schoolBoard).toBeGreaterThan(rank("49", { office_scope: "county", title: "County Council" }));
    expect(schoolBoard).toBeLessThan(rank("49", { office_scope: "place", title: "City Council" }));
    const retention = rank("49", {
      office_scope: "statewide",
      contest_family: "judicial_office",
      title: "Justice of the Supreme Court",
    });
    expect(retention).toBeGreaterThan(stateBaselineContestRank(input({ office_scope: "something_new" })));
    expect(retention).toBeLessThan(rank("49", { race_type: "ballot_measure", office_scope: null, district_type: "statewide" }));
  });

  it("VT: statewide measures first; probate and assistant judges lead the county block; JP stays baseline", () => {
    expect(rank("50", { race_type: "ballot_measure", office_scope: null, district_type: "statewide" })).toBeLessThan(
      rank("50", { office_scope: "presidential", title: "President" })
    );
    const probate = rank("50", { office_scope: "county", contest_family: "judicial_office", title: "Judge of Probate" });
    expect(probate).toBeGreaterThan(rank("50", { office_scope: "state_lower", title: "State Representative" }));
    expect(probate).toBeLessThan(rank("50", { office_scope: "county", title: "High Bailiff" }));
    // JP-last-among-offices is A-excluded; the baseline place-judicial slot stands.
    expect(rank("50", { office_scope: "place", contest_family: "judicial_office", title: "Justice of the Peace" })).toBe(
      stateBaselineContestRank(
        input({ office_scope: "place", contest_family: "judicial_office", title: "Justice of the Peace" })
      )
    );
  });

  it("VA: school board inside the locality blocks; statewide measures before local", () => {
    const schoolBoard = rank("51", { office_scope: "school_unified", title: "School Board At Large" });
    expect(schoolBoard).toBeGreaterThan(rank("51", { office_scope: "place", title: "City Council" }));
    expect(schoolBoard).toBeLessThan(stateBaselineContestRank(input({ office_scope: "school_unified" })));
    expect(rank("51", { race_type: "ballot_measure", office_scope: null, district_type: "statewide" })).toBeLessThan(
      rank("51", { race_type: "ballot_measure", office_scope: null, district_type: "county" })
    );
  });

  it("WA: measures first (state then local); judicial after county before municipal", () => {
    const stateMeasure = rank("53", { race_type: "ballot_measure", office_scope: null, district_type: "statewide" });
    const countyMeasure = rank("53", { race_type: "ballot_measure", office_scope: null, district_type: "county" });
    expect(stateMeasure).toBeLessThan(countyMeasure);
    expect(countyMeasure).toBeLessThan(rank("53", { office_scope: "presidential", title: "President" }));
    const judge = rank("53", { office_scope: "county", contest_family: "judicial_office", title: "Judge of the Superior Court" });
    expect(judge).toBeGreaterThan(rank("53", { office_scope: "county", title: "County Assessor" }));
    expect(judge).toBeLessThan(rank("53", { office_scope: "place", title: "City Council" }));
  });

  it("WI: executives above US Senate", () => {
    expect(rank("55", { office_scope: "statewide", title: "Governor" })).toBeLessThan(
      rank("55", { office_scope: "statewide", contest_family: "us_senate", title: "United States Senator" })
    );
  });

  it("WY: judicial retention after county, before municipal and school", () => {
    const retention = rank("56", {
      office_scope: "statewide",
      contest_family: "judicial_office",
      title: "Justice of the Supreme Court",
    });
    expect(retention).toBeGreaterThan(rank("56", { office_scope: "county", title: "County Commissioner" }));
    expect(retention).toBeLessThan(rank("56", { office_scope: "place", title: "Mayor" }));
    expect(retention).toBeLessThan(rank("56", { office_scope: "school_unified", title: "School Board" }));
  });
});
