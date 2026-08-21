import { beforeEach, describe, expect, it, vi } from "vitest";

const { lookupHistoricalContestMarginRowsMock, calculateWeightedHistoricalContestMarginMock } = vi.hoisted(() => ({
  lookupHistoricalContestMarginRowsMock: vi.fn(),
  calculateWeightedHistoricalContestMarginMock: vi.fn(),
}));

vi.mock("../../../src/pipeline/competitiveness/historicalContestMarginLookup.js", () => ({
  lookupHistoricalContestMarginRows: lookupHistoricalContestMarginRowsMock,
  calculateWeightedHistoricalContestMargin: calculateWeightedHistoricalContestMarginMock,
}));

import {
  isDcDelegateDistrict,
  loadCurrentRaceRatingContexts,
} from "../../../src/pipeline/competitiveness/currentRaceRatingContextLoader.js";

const SENATE_ID = "11111111-1111-4111-8111-111111111111";
const DC_HOUSE_ID = "22222222-2222-4222-8222-222222222222";

function senateElectionRow() {
  return {
    election_id: SENATE_ID,
    district_name: "Georgia",
    district_type: "statewide",
    state: "GA",
    geoid_compact: "13",
    state_fips: "13",
    official_ballot_title: "United States Senator",
    election_date: "2026-11-03",
    election_stage: "general",
    is_partisan: true,
    office_canonical_name: "United States Senator",
  };
}

function dcHouseElectionRow() {
  return {
    election_id: DC_HOUSE_ID,
    district_name: "District of Columbia At-Large",
    district_type: "us_house",
    state: "DC",
    geoid_compact: "1198",
    state_fips: "11",
    official_ballot_title: "United States Representative, DC At-Large",
    election_date: "2026-11-03",
    election_stage: "general",
    is_partisan: true,
    office_canonical_name: null,
  };
}

describe("isDcDelegateDistrict", () => {
  it("flags only the DC us_house district", () => {
    expect(isDcDelegateDistrict("us_house", "DC")).toBe(true);
    expect(isDcDelegateDistrict("us_house", "dc")).toBe(true);
    expect(isDcDelegateDistrict("us_house", "GA")).toBe(false);
    expect(isDcDelegateDistrict("statewide", "DC")).toBe(false);
  });
});

describe("loadCurrentRaceRatingContexts", () => {
  beforeEach(() => {
    lookupHistoricalContestMarginRowsMock.mockReset();
    calculateWeightedHistoricalContestMarginMock.mockReset();
  });

  it("skips all queries for an empty id list", async () => {
    const query = vi.fn();
    await expect(loadCurrentRaceRatingContexts({ query } as never, ["", "  "])).resolves.toEqual([]);
    expect(query).not.toHaveBeenCalled();
    expect(lookupHistoricalContestMarginRowsMock).not.toHaveBeenCalled();
  });

  it("builds contexts with roster, DC flag, and historic label", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [senateElectionRow(), dcHouseElectionRow()] })
      .mockResolvedValueOnce({
        rows: [
          {
            election_id: SENATE_ID,
            display_name: "Alex Example",
            party: "Democratic",
            is_incumbent: true,
            status: "active",
          },
          {
            election_id: SENATE_ID,
            display_name: "Bo Sample",
            party: "Republican",
            is_incumbent: false,
            status: "active",
          },
        ],
      });
    lookupHistoricalContestMarginRowsMock.mockResolvedValue(
      new Map([[SENATE_ID, [{ election_year: 2020 }]]])
    );
    calculateWeightedHistoricalContestMarginMock.mockImplementation((rows: unknown[]) =>
      rows.length > 0
        ? { competitiveness_label: "very_competitive", margin_percent: 3.2, election_years: [2020] }
        : null
    );

    const contexts = await loadCurrentRaceRatingContexts({ query } as never, [
      SENATE_ID,
      DC_HOUSE_ID,
      SENATE_ID,
    ]);

    // Only office elections are eligible for a race rating.
    expect(query.mock.calls[0]?.[0]).toContain("e.race_type = 'office'");
    expect(query.mock.calls[0]?.[1]).toEqual([[SENATE_ID, DC_HOUSE_ID]]);

    expect(contexts).toHaveLength(2);
    const senate = contexts[0]!;
    expect(senate.electionId).toBe(SENATE_ID);
    expect(senate.isDcDelegate).toBe(false);
    expect(senate.candidates.map((candidate) => candidate.displayName)).toEqual(["Alex Example", "Bo Sample"]);
    expect(senate.historical).toEqual({
      competitivenessLabel: "very_competitive",
      marginPercent: 3.2,
      electionYears: [2020],
    });

    const dcHouse = contexts[1]!;
    expect(dcHouse.isDcDelegate).toBe(true);
    expect(dcHouse.candidates).toEqual([]);
    expect(dcHouse.historical).toBeNull();

    // Historic lookup keys mirror ballotLookup's derivation: prior years only.
    const lookupInputs = lookupHistoricalContestMarginRowsMock.mock.calls[0]?.[1];
    expect(lookupInputs).toEqual([
      expect.objectContaining({
        lookupId: SENATE_ID,
        officeCanonicalName: "United States Senator",
        districtType: "statewide",
        geoidCompact: "13",
        stateFips: "13",
        currentElectionYear: 2026,
        maxElectionYear: 2025,
      }),
      expect.objectContaining({ lookupId: DC_HOUSE_ID }),
    ]);
  });

  it("drops ids that resolve to no office election", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [senateElectionRow()] })
      .mockResolvedValueOnce({ rows: [] });
    lookupHistoricalContestMarginRowsMock.mockResolvedValue(new Map());
    calculateWeightedHistoricalContestMarginMock.mockReturnValue(null);

    const contexts = await loadCurrentRaceRatingContexts({ query } as never, [SENATE_ID, DC_HOUSE_ID]);
    expect(contexts.map((context) => context.electionId)).toEqual([SENATE_ID]);
  });
});
