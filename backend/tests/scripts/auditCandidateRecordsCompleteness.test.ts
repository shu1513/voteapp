import { describe, expect, it } from "vitest";

import {
  buildAuditTargetConditions,
  isConfirmedNull,
} from "../../src/scripts/auditCandidateRecordsCompleteness.js";

describe("buildAuditTargetConditions", () => {
  it("returns no conditions when no filters are set", () => {
    const result = buildAuditTargetConditions({
      candidateId: null,
      electionId: null,
      districtId: null,
    });
    expect(result.conditions).toEqual([]);
    expect(result.values).toEqual([]);
  });

  it("builds a direct candidate condition", () => {
    const result = buildAuditTargetConditions({
      candidateId: "11111111-1111-1111-1111-111111111111",
      electionId: null,
      districtId: null,
    });
    expect(result.conditions).toEqual(["c.id = $1::uuid"]);
    expect(result.values).toEqual(["11111111-1111-1111-1111-111111111111"]);
  });

  it("builds EXISTS conditions for election and district so aggregation rows stay unfiltered", () => {
    const result = buildAuditTargetConditions({
      candidateId: null,
      electionId: "22222222-2222-2222-2222-222222222222",
      districtId: "33333333-3333-3333-3333-333333333333",
    });
    expect(result.conditions).toHaveLength(2);
    expect(result.conditions[0]).toContain("EXISTS");
    expect(result.conditions[0]).toContain("cef.election_id = $1::uuid");
    expect(result.conditions[1]).toContain("EXISTS");
    expect(result.conditions[1]).toContain("ef.district_id = $2::uuid");
    expect(result.values).toEqual([
      "22222222-2222-2222-2222-222222222222",
      "33333333-3333-3333-3333-333333333333",
    ]);
  });

  it("accepts joint-ticket running mates through either side of the election link", () => {
    const result = buildAuditTargetConditions({
      candidateId: null,
      electionId: "22222222-2222-2222-2222-222222222222",
      districtId: "33333333-3333-3333-3333-333333333333",
    });
    for (const condition of result.conditions) {
      expect(condition).toContain("cef.candidate_id = c.id OR cef.running_mate_candidate_id = c.id");
    }
  });

  it("combines all three filters with sequential placeholders", () => {
    const result = buildAuditTargetConditions({
      candidateId: "11111111-1111-1111-1111-111111111111",
      electionId: "22222222-2222-2222-2222-222222222222",
      districtId: "33333333-3333-3333-3333-333333333333",
    });
    expect(result.conditions[0]).toContain("$1");
    expect(result.conditions[1]).toContain("$2");
    expect(result.conditions[2]).toContain("$3");
    expect(result.values).toHaveLength(3);
  });
});

describe("isConfirmedNull", () => {
  it("requires a covering confirmation with the no_records_found gap id", () => {
    expect(
      isConfirmedNull({
        confirmed_gap_ids: ["candidate_records.no_records_found"],
        confirmation_covers_latest_search: true,
      })
    ).toBe(true);
    expect(
      isConfirmedNull({
        confirmed_gap_ids: ["candidate_records.no_records_found"],
        confirmation_covers_latest_search: false,
      })
    ).toBe(false);
    expect(
      isConfirmedNull({
        confirmed_gap_ids: [],
        confirmation_covers_latest_search: true,
      })
    ).toBe(false);
  });
});
