import { describe, expect, it } from "vitest";

import {
  isJudicialOfficeTitle,
  isJudicialRetentionTitle,
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

  it("candidate policy helper excludes party for retention judicial contests", () => {
    const includeParty = shouldIncludeCandidatePartyByPolicy({
      districtType: "statewide",
      state: "KS",
      officialBallotTitle: "Shall Justice Jane Doe be retained in office?",
    });
    expect(includeParty).toBe(false);
  });
});
