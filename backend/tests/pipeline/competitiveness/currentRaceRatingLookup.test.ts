import { describe, expect, it, vi } from "vitest";

import {
  currentRaceRatingBlocksResearch,
  currentRaceRatingOverridesHistory,
  loadCurrentRaceRatings,
  loadOverridingCurrentRaceRatings,
  type CurrentRaceRatingLookupRecord,
} from "../../../src/pipeline/competitiveness/currentRaceRatingLookup.js";

const ELECTION_ID = "11111111-1111-4111-8111-111111111111";
// Fixed clock: 2026-08-20. as_of 2026-06-21 is exactly 60 days old,
// 2026-06-20 is 61.
const TODAY = new Date("2026-08-20T15:30:00.000Z");

function ratingRecord(
  overrides: Partial<CurrentRaceRatingLookupRecord> = {}
): CurrentRaceRatingLookupRecord {
  return {
    election_id: ELECTION_ID,
    election_date: "2026-11-03",
    competitiveness_label: "toss_up",
    method: "outlet_consensus",
    confidence: "high",
    evidence_status: "rated",
    as_of: "2026-08-06",
    decisive_round: null,
    evidence: { observations: [] },
    source_url: "https://insideelections.com/ratings/senate",
    researched_on: "2026-08-06",
    ...overrides,
  };
}

describe("currentRaceRatingOverridesHistory", () => {
  it("accepts a fresh, confident rating for an upcoming election", () => {
    expect(currentRaceRatingOverridesHistory(ratingRecord(), TODAY)).toBe(true);
    expect(currentRaceRatingOverridesHistory(ratingRecord({ confidence: "medium" }), TODAY)).toBe(true);
  });

  it("is fresh at exactly 60 days old and stale at 61", () => {
    expect(currentRaceRatingOverridesHistory(ratingRecord({ as_of: "2026-06-21" }), TODAY)).toBe(true);
    expect(currentRaceRatingOverridesHistory(ratingRecord({ as_of: "2026-06-20" }), TODAY)).toBe(false);
  });

  it("treats a future as_of as not fresh instead of eternally fresh", () => {
    expect(currentRaceRatingOverridesHistory(ratingRecord({ as_of: "2099-01-01" }), TODAY)).toBe(false);
    expect(currentRaceRatingBlocksResearch(ratingRecord({ as_of: "2099-01-01" }), TODAY)).toBe(false);
  });

  it("never overrides with low confidence or none_found", () => {
    expect(currentRaceRatingOverridesHistory(ratingRecord({ confidence: "low" }), TODAY)).toBe(false);
    expect(
      currentRaceRatingOverridesHistory(
        ratingRecord({
          evidence_status: "none_found",
          competitiveness_label: null,
          confidence: null,
          as_of: null,
        }),
        TODAY
      )
    ).toBe(false);
  });

  it("excludes past elections but keeps election day itself", () => {
    expect(
      currentRaceRatingOverridesHistory(ratingRecord({ election_date: "2026-08-19" }), TODAY)
    ).toBe(false);
    expect(
      currentRaceRatingOverridesHistory(ratingRecord({ election_date: "2026-08-20" }), TODAY)
    ).toBe(true);
  });
});

describe("currentRaceRatingBlocksResearch", () => {
  it("blocks re-research while a rated row is fresh, regardless of confidence", () => {
    expect(currentRaceRatingBlocksResearch(ratingRecord({ confidence: "low" }), TODAY)).toBe(true);
    expect(currentRaceRatingBlocksResearch(ratingRecord({ as_of: "2026-06-21" }), TODAY)).toBe(true);
    expect(currentRaceRatingBlocksResearch(ratingRecord({ as_of: "2026-06-20" }), TODAY)).toBe(false);
  });

  it("blocks a none_found retry for 30 days from researched_on", () => {
    const noneFound = (researchedOn: string) =>
      ratingRecord({
        evidence_status: "none_found",
        competitiveness_label: null,
        confidence: null,
        as_of: null,
        researched_on: researchedOn,
      });
    expect(currentRaceRatingBlocksResearch(noneFound("2026-07-21"), TODAY)).toBe(true);
    expect(currentRaceRatingBlocksResearch(noneFound("2026-07-20"), TODAY)).toBe(false);
  });
});

describe("loadCurrentRaceRatings", () => {
  it("skips the query for an empty id list", async () => {
    const query = vi.fn();
    await expect(loadCurrentRaceRatings({ query } as never, ["", "  "])).resolves.toEqual(new Map());
    expect(query).not.toHaveBeenCalled();
  });

  it("queries by deduplicated election ids and maps rows", async () => {
    const row = ratingRecord();
    const query = vi.fn().mockResolvedValue({ rows: [row] });

    const result = await loadCurrentRaceRatings({ query } as never, [ELECTION_ID, ELECTION_ID]);
    expect(result.get(ELECTION_ID)).toEqual(row);
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[0]).toContain("FROM public.current_race_ratings");
    expect(query.mock.calls[0]?.[0]).toContain("JOIN public.elections");
    // researched_on must not depend on the DB session time zone.
    expect(query.mock.calls[0]?.[0]).toContain(
      "(crr.researched_at AT TIME ZONE 'UTC')::date::text AS researched_on"
    );
    expect(query.mock.calls[0]?.[1]).toEqual([[ELECTION_ID]]);
  });
});

describe("loadOverridingCurrentRaceRatings", () => {
  it("drops rows that do not override history", async () => {
    const fresh = ratingRecord();
    const stale = ratingRecord({
      election_id: "22222222-2222-4222-8222-222222222222",
      as_of: "2026-06-20",
    });
    const query = vi.fn().mockResolvedValue({ rows: [fresh, stale] });

    const result = await loadOverridingCurrentRaceRatings(
      { query } as never,
      [fresh.election_id, stale.election_id],
      { today: TODAY }
    );
    expect([...result.keys()]).toEqual([fresh.election_id]);
  });
});
