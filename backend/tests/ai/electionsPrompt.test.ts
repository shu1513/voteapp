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
});
