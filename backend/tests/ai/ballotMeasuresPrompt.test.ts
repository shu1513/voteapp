import { describe, expect, it } from "vitest";

import { buildBallotMeasuresPrompt } from "../../src/ai/providers/ballotMeasuresPrompt.js";

describe("buildBallotMeasuresPrompt", () => {
  it("requests binary YES-outcome research-area tags from allowed slugs", () => {
    const prompt = buildBallotMeasuresPrompt({
      districtName: "Los Angeles County, California",
      districtType: "county",
      state: "CA",
      electionDate: "2026-06-02",
      officialBallotTitle: "Measure H",
      seedUrls: [],
      allowedResearchAreaSlugs: ["healthcare_affordability", "cost_of_living_reduction"],
    });

    expect(prompt).toContain('"research_area_tags"');
    expect(prompt).toContain("research_area_tags describes the likely policy effect if YES wins / the measure passes.");
    expect(prompt).toContain('"for" means the YES outcome advances');
    expect(prompt).toContain('"against" means the YES outcome cuts against');
    expect(prompt).toContain("Do not tag an area if the effect is mixed, indirect, unclear, or not meaningfully directional.");
    expect(prompt).toContain("best supporting URLs used for this research, up to 20 unique URLs");
    expect(prompt).toContain('- "healthcare_affordability"');
    expect(prompt).toContain('- "cost_of_living_reduction"');
  });
});
