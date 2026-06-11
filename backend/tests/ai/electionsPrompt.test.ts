import { describe, expect, it } from "vitest";

import { buildElectionsPrompt } from "../../src/ai/providers/electionsPrompt.js";

describe("buildElectionsPrompt partisanship field gating", () => {
  it("does not request is_partisan for ballot_measure family", () => {
    const prompt = buildElectionsPrompt({
      draft: {
        district_id: "d-1",
        district_name: "Los Angeles County, California",
        district_type: "county",
        state: "CA",
      },
      softRetryCount: 0,
      reviewFeedbackLines: [],
      contestFamily: "ballot_measure",
    });

    expect(prompt).not.toContain("\"is_partisan\":");
  });

  it("does not request is_partisan for fixed nonpartisan school states", () => {
    const prompt = buildElectionsPrompt({
      draft: {
        district_id: "d-1",
        district_name: "Demo Unified School District",
        district_type: "school_unified",
        state: "CA",
      },
      softRetryCount: 0,
      reviewFeedbackLines: [],
      contestFamily: "all",
    });

    expect(prompt).not.toContain("\"is_partisan\":");
  });

  it("requests is_partisan for mixed school states", () => {
    const prompt = buildElectionsPrompt({
      draft: {
        district_id: "d-1",
        district_name: "Demo Unified School District",
        district_type: "school_unified",
        state: "NC",
      },
      softRetryCount: 0,
      reviewFeedbackLines: [],
      contestFamily: "all",
    });

    expect(prompt).toContain("\"is_partisan\": true");
    expect(prompt).toContain("is_partisan");
  });

  it("renders U.S. Senate-only family instructions", () => {
    const prompt = buildElectionsPrompt({
      draft: {
        district_id: "d-1",
        district_name: "California",
        district_type: "statewide",
        state: "CA",
      },
      softRetryCount: 0,
      reviewFeedbackLines: [],
      contestFamily: "us_senate",
    });

    expect(prompt).toContain("Contest family for this call: us_senate");
    expect(prompt).toContain("Return only U.S. Senate contests for this statewide scope.");
    expect(prompt).toContain("Exclude all ballot measures and all non-U.S.-Senate office contests.");
    expect(prompt).toContain("\"senate_class\": \"class_i | class_ii | class_iii");
    expect(prompt).toContain("\"term_end_year\": \"YYYY");
    expect(prompt).not.toContain("\"is_partisan\":");
    expect(prompt).not.toContain("is_partisan:");
    expect(prompt).not.toContain(
      "For ballot measures, official_ballot_title must be the actual official measure label/title"
    );
  });

  it("excludes presidential contests from statewide non-judicial discovery", () => {
    const prompt = buildElectionsPrompt({
      draft: {
        district_id: "d-1",
        district_name: "California",
        district_type: "statewide",
        state: "CA",
      },
      softRetryCount: 0,
      reviewFeedbackLines: [],
      contestFamily: "non_judicial_office",
    });

    expect(prompt).toContain(
      "Exclude all ballot measures, all judicial contests, all federal contests (including President, Vice President, presidential electors, U.S. Senate, and U.S. House)."
    );
  });
});
