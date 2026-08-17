import { describe, expect, it } from "vitest";

import { findSameCommitteeAmendmentPairs } from "../../../src/scripts/probeMissouriCandidateFinance.js";

describe("probeMissouriCandidateFinance", () => {
  it("does not pair an amendment with a base report from another committee", () => {
    const reports = [
      { mecid: "A222073", report: "AMENDED April Quarterly" },
      { mecid: "C263985", report: "April Quarterly" },
    ];

    expect(findSameCommitteeAmendmentPairs(reports)).toEqual([]);
  });

  it("pairs normalized amendment names within the same committee", () => {
    const reports = [
      { mecid: "c263985", report: "  amended   April Quarterly  " },
      { mecid: "C263985", report: "April Quarterly" },
    ];

    expect(findSameCommitteeAmendmentPairs(reports)).toEqual([reports[0]]);
  });
});
