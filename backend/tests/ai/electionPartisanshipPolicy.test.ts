import { describe, expect, it } from "vitest";

import {
  isJudicialOfficeTitle,
  isJudicialRetentionTitle,
  resolveCandidateContestPartisanshipByPolicy,
  resolveElectionIsPartisan,
  shouldIncludeCandidatePartyByPolicy,
  shouldAskIsPartisanInPrompt,
} from "../../src/ai/electionPartisanshipPolicy.js";

describe("electionPartisanshipPolicy", () => {
  it("never asks AI for ballot_measure family", () => {
    expect(
      shouldAskIsPartisanInPrompt({
        draft: {
          district_id: "d-1",
          district_name: "Los Angeles County, California",
          district_type: "county",
          state: "CA",
        },
        contestFamily: "ballot_measure",
      })
    ).toBe(false);
  });

  it("asks AI for mixed school states and not for fixed school states", () => {
    expect(
      shouldAskIsPartisanInPrompt({
        draft: {
          district_id: "d-1",
          district_name: "Demo Unified School District",
          district_type: "school_unified",
          state: "NC",
        },
        contestFamily: "all",
      })
    ).toBe(true);

    expect(
      shouldAskIsPartisanInPrompt({
        draft: {
          district_id: "d-1",
          district_name: "Demo Unified School District",
          district_type: "school_unified",
          state: "CA",
        },
        contestFamily: "all",
      })
    ).toBe(false);
  });

  it("never asks AI for judicial_office family", () => {
    expect(
      shouldAskIsPartisanInPrompt({
        draft: {
          district_id: "d-1",
          district_name: "Los Angeles County, California",
          district_type: "county",
          state: "CA",
        },
        contestFamily: "judicial_office",
      })
    ).toBe(false);
  });

  it("never asks AI for us_senate family and forces partisan true", () => {
    expect(
      shouldAskIsPartisanInPrompt({
        draft: {
          district_id: "d-1",
          district_name: "California",
          district_type: "statewide",
          state: "CA",
        },
        contestFamily: "us_senate",
      })
    ).toBe(false);

    const resolved = resolveElectionIsPartisan({
      draft: {
        district_id: "d-1",
        district_name: "California",
        district_type: "statewide",
        state: "CA",
      },
      contestFamily: "us_senate",
      raceType: "office",
      officialBallotTitle: "United States Senator",
      aiValue: undefined,
    });
    expect(resolved).toBe(true);
  });

  it("forces ballot_measure to nonpartisan", () => {
    const resolved = resolveElectionIsPartisan({
      draft: {
        district_id: "d-1",
        district_name: "California",
        district_type: "statewide",
        state: "CA",
      },
      contestFamily: "ballot_measure",
      raceType: "ballot_measure",
      officialBallotTitle: "Proposition 1",
      aiValue: true,
    });
    expect(resolved).toBe(false);
  });

  it("forces judicial contests by state policy", () => {
    const tx = resolveElectionIsPartisan({
      draft: {
        district_id: "d-1",
        district_name: "Texas",
        district_type: "statewide",
        state: "TX",
      },
      contestFamily: "judicial_office",
      raceType: "office",
      officialBallotTitle: "Judge of the Supreme Court",
      aiValue: undefined,
    });
    expect(tx).toBe(true);

    const ca = resolveElectionIsPartisan({
      draft: {
        district_id: "d-1",
        district_name: "California",
        district_type: "statewide",
        state: "CA",
      },
      contestFamily: "judicial_office",
      raceType: "office",
      officialBallotTitle: "Judge of the Superior Court",
      aiValue: true,
    });
    expect(ca).toBe(false);
  });

  it("uses AI value for mixed school states and leaves undefined when missing", () => {
    const fromAi = resolveElectionIsPartisan({
      draft: {
        district_id: "d-1",
        district_name: "Demo Unified School District",
        district_type: "school_unified",
        state: "NC",
      },
      contestFamily: "all",
      raceType: "office",
      officialBallotTitle: "School Board Member",
      aiValue: true,
    });
    expect(fromAi).toBe(true);

    const missingAi = resolveElectionIsPartisan({
      draft: {
        district_id: "d-1",
        district_name: "Demo Unified School District",
        district_type: "school_unified",
        state: "NC",
      },
      contestFamily: "all",
      raceType: "office",
      officialBallotTitle: "School Board Member",
      aiValue: undefined,
    });
    expect(missingAi).toBeUndefined();
  });

  it("supports California Court of Appeal title detection", () => {
    expect(isJudicialOfficeTitle("Justice of the Court of Appeal, District 2")).toBe(true);
  });

  it("treats judicial retention contests as nonpartisan in partisan-judicial states", () => {
    expect(isJudicialRetentionTitle("Justice of the Supreme Court (Retention)")).toBe(true);

    const resolved = resolveElectionIsPartisan({
      draft: {
        district_id: "d-1",
        district_name: "Kansas",
        district_type: "statewide",
        state: "KS",
      },
      contestFamily: "judicial_office",
      raceType: "office",
      officialBallotTitle: "Justice of the Supreme Court (Retention)",
      aiValue: true,
    });
    expect(resolved).toBe(false);
  });

  it("keeps non-retention judicial contests in partisan-judicial states as partisan", () => {
    const resolved = resolveElectionIsPartisan({
      draft: {
        district_id: "d-1",
        district_name: "Kansas",
        district_type: "statewide",
        state: "KS",
      },
      contestFamily: "judicial_office",
      raceType: "office",
      officialBallotTitle: "Judge of the District Court",
      aiValue: undefined,
    });
    expect(resolved).toBe(true);
  });

  it("keeps South Carolina probate judges partisan", () => {
    const resolved = resolveElectionIsPartisan({
      draft: {
        district_id: "d-sc",
        district_name: "Spartanburg County, South Carolina",
        district_type: "county",
        state: "SC",
      },
      contestFamily: "judicial_office",
      raceType: "office",
      officialBallotTitle: "Probate Judge",
      aiValue: false,
    });
    expect(resolved).toBe(true);
  });

  it("candidate policy helper excludes party for retention judicial contests", () => {
    const includeParty = shouldIncludeCandidatePartyByPolicy({
      districtType: "statewide",
      state: "KS",
      officialBallotTitle: "Shall Justice Jane Doe be retained in office?",
    });
    expect(includeParty).toBe(false);
  });

  it("forces Washington state-legislative contests to partisan", () => {
    for (const districtType of ["state_upper", "state_lower"] as const) {
      expect(
        shouldAskIsPartisanInPrompt({
          draft: {
            district_id: "d-wa",
            district_name: "Washington legislative district",
            district_type: districtType,
            state: "WA",
          },
          contestFamily: "all",
        })
      ).toBe(false);

      expect(
        resolveElectionIsPartisan({
          draft: {
            district_id: "d-wa",
            district_name: "Washington legislative district",
            district_type: districtType,
            state: "WA",
          },
          contestFamily: "all",
          raceType: "office",
          officialBallotTitle: "State Representative Position 1",
          aiValue: false,
        })
      ).toBe(true);

      expect(
        shouldIncludeCandidatePartyByPolicy({
          districtType,
          state: "WA",
          officialBallotTitle: "State Representative Position 1",
        })
      ).toBe(true);
    }
  });

  it("treats elected Arizona county judgeships as partisan", () => {
    for (const officialBallotTitle of [
      "Judge of Superior Court, Division 2",
      "Justice of the Peace, Precinct 2",
    ]) {
      expect(
        resolveCandidateContestPartisanshipByPolicy({
          districtType: "county",
          state: "AZ",
          officialBallotTitle,
        }),
        officialBallotTitle
      ).toBe(true);
    }
  });

  it("keeps Arizona merit-selection retention questions nonpartisan", () => {
    expect(
      resolveCandidateContestPartisanshipByPolicy({
        districtType: "county",
        state: "AZ",
        officialBallotTitle: "Judge of the Superior Court, Division 4 (Retention)",
      })
    ).toBe(false);
  });

  it("does not treat court clerks and prosecutors as judicial offices", () => {
    for (const officialBallotTitle of [
      "Clerk of Superior Court",
      "Elkhart County Circuit Court Clerk",
      "Prosecuting Attorney of Elkhart County, 34th Judicial Circuit",
      "State Attorney, 4th Judicial Circuit",
      "Constable, Justice Precinct 2",
    ]) {
      expect(isJudicialOfficeTitle(officialBallotTitle), officialBallotTitle).toBe(false);
    }

    // The judicial rule forced these either way (false in Arizona, true in
    // North Carolina); with the office off the judicial path the payload's own
    // researched value decides.
    expect(
      resolveCandidateContestPartisanshipByPolicy({
        districtType: "county",
        state: "AZ",
        officialBallotTitle: "Clerk of Superior Court",
      })
    ).toBeUndefined();
    expect(
      shouldIncludeCandidatePartyByPolicy({
        districtType: "county",
        state: "AZ",
        officialBallotTitle: "Clerk of Superior Court",
      })
    ).toBe(true);
  });
});
