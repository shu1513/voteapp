import { describe, expect, it } from "vitest";

import type { CurrentRaceRatingLookupRecord } from "../../src/pipeline/competitiveness/currentRaceRatingLookup.js";
import {
  parseElectionIdsFlag,
  parseManifestElectionIds,
  partitionDueRows,
} from "../../src/scripts/manualCurrentRaceRatings.js";

// Fixed clock: 2026-08-20. as_of 2026-06-21 is exactly 60 days old (fresh),
// 2026-06-20 is 61 (stale).
const NOW = new Date("2026-08-20T15:30:00.000Z");

describe("parseElectionIdsFlag", () => {
  it("splits, trims, and deduplicates", () => {
    expect(parseElectionIdsFlag(" a , b ,a,")).toEqual(["a", "b"]);
  });

  it("rejects an empty list", () => {
    expect(() => parseElectionIdsFlag(" , ")).toThrow("at least one election id");
  });
});

describe("parseManifestElectionIds", () => {
  it("accepts a bare array and an election_ids object", () => {
    expect(parseManifestElectionIds(["a", "b", "a"])).toEqual(["a", "b"]);
    expect(parseManifestElectionIds({ election_ids: ["a"] })).toEqual(["a"]);
  });

  it("rejects malformed manifests instead of treating them as empty", () => {
    expect(() => parseManifestElectionIds({ ids: ["a"] })).toThrow("Manifest must be");
    expect(() => parseManifestElectionIds([1])).toThrow("non-empty strings");
    expect(() => parseManifestElectionIds([])).toThrow("no election ids");
  });
});

function dueRow(overrides: Record<string, unknown> = {}) {
  return {
    election_id: "11111111-1111-4111-8111-111111111111",
    state: "GA",
    district_name: "Georgia",
    district_type: "statewide",
    official_ballot_title: "United States Senator",
    election_date: "2026-11-03",
    election_stage: "general",
    ...overrides,
  } as never;
}

function ratingRecord(overrides: Partial<CurrentRaceRatingLookupRecord> = {}): CurrentRaceRatingLookupRecord {
  return {
    election_id: "11111111-1111-4111-8111-111111111111",
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

describe("partitionDueRows", () => {
  it("splits rows into due, blocked, and DC-delegate excluded", () => {
    const freshId = "11111111-1111-4111-8111-111111111111";
    const staleId = "22222222-2222-4222-8222-222222222222";
    const unratedId = "33333333-3333-4333-8333-333333333333";
    const dcId = "44444444-4444-4444-8444-444444444444";
    const rows = [
      dueRow({ election_id: freshId }),
      dueRow({ election_id: staleId }),
      dueRow({ election_id: unratedId }),
      dueRow({
        election_id: dcId,
        state: "DC",
        district_type: "us_house",
        district_name: "District of Columbia At-Large",
        official_ballot_title: "United States Representative, DC At-Large",
      }),
    ];
    const ratings = new Map([
      [freshId, ratingRecord({ election_id: freshId, as_of: "2026-06-21" })],
      [staleId, ratingRecord({ election_id: staleId, as_of: "2026-06-20" })],
    ]);

    const partition = partitionDueRows(rows, ratings, NOW);
    expect(partition.due.map((row) => row.election_id)).toEqual([staleId, unratedId]);
    expect(partition.blocked.map((row) => row.election_id)).toEqual([freshId]);
    expect(partition.excluded).toHaveLength(1);
    expect(partition.excluded[0]).toMatchObject({
      election_id: dcId,
      is_dc_delegate: true,
      reason: expect.stringContaining("DC delegate"),
    });
    // The stale row keeps its stored rating in the annotation so the
    // researcher sees what a rewrite would replace.
    expect(partition.due[0]?.existing_rating).toMatchObject({ as_of: "2026-06-20" });
    expect(partition.due[1]?.existing_rating).toBeNull();
  });

  it("blocks a recent none_found row for its retry window", () => {
    const rows = [dueRow()];
    const ratings = new Map([
      [
        "11111111-1111-4111-8111-111111111111",
        ratingRecord({
          evidence_status: "none_found",
          competitiveness_label: null,
          confidence: null,
          as_of: null,
          researched_on: "2026-07-21",
        }),
      ],
    ]);
    const partition = partitionDueRows(rows, ratings, NOW);
    expect(partition.due).toHaveLength(0);
    expect(partition.blocked).toHaveLength(1);
  });
});
