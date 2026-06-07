import { describe, expect, it, vi } from "vitest";

import {
  BALLOT_MEASURE_RESEARCH_AREA_SLUGS,
  loadAllowedBallotMeasureResearchAreas,
  upsertBallotMeasureResearchAreaTags,
} from "../../src/pipeline/ballotMeasures/ballotMeasureResearchAreaTags.js";

describe("loadAllowedBallotMeasureResearchAreas", () => {
  it("loads the explicit ballot-measure research-area policy list", async () => {
    const query = vi.fn().mockResolvedValueOnce({
      rows: [
        { id: "ra-health", slug: "healthcare_affordability" },
        { id: "ra-tax", slug: "cost_of_living_reduction" },
      ],
    });

    const result = await loadAllowedBallotMeasureResearchAreas({ query });

    expect(result).toEqual([
      { id: "ra-health", slug: "healthcare_affordability" },
      { id: "ra-tax", slug: "cost_of_living_reduction" },
    ]);
    expect(query).toHaveBeenCalledWith(expect.stringContaining("FROM public.research_areas"), [
      BALLOT_MEASURE_RESEARCH_AREA_SLUGS,
    ]);
  });
});

describe("upsertBallotMeasureResearchAreaTags", () => {
  it("upserts binary for/against tags and prunes stale tags", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1 });
    const result = await upsertBallotMeasureResearchAreaTags(
      { query },
      "bm-1",
      [
        { researchAreaSlug: "healthcare_affordability", stance: "for" },
        { researchAreaSlug: "cost_of_living_reduction", stance: "against" },
      ],
      new Map([
        ["healthcare_affordability", "ra-health"],
        ["cost_of_living_reduction", "ra-cost"],
      ])
    );

    expect(result).toEqual({ processed: 2 });
    expect(query).toHaveBeenCalledTimes(3);
    expect(query.mock.calls[0]?.[0]).toContain("INSERT INTO public.ballot_measure_research_area_tags");
    expect(query.mock.calls[0]?.[1]).toEqual(["bm-1", "ra-health", "for"]);
    expect(query.mock.calls[1]?.[1]).toEqual(["bm-1", "ra-cost", "against"]);
    expect(query.mock.calls[2]?.[0]).toContain("DELETE FROM public.ballot_measure_research_area_tags");
    expect(query.mock.calls[2]?.[1]).toEqual(["bm-1", ["ra-health", "ra-cost"]]);
  });

  it("deletes every stale tag when the latest classification is empty", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1 });
    const result = await upsertBallotMeasureResearchAreaTags({ query }, "bm-1", [], new Map());

    expect(result).toEqual({ processed: 0 });
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[0]).toContain("DELETE FROM public.ballot_measure_research_area_tags");
    expect(query.mock.calls[0]?.[1]).toEqual(["bm-1", []]);
  });

  it("throws when a tag slug has no loaded research area id", async () => {
    const query = vi.fn();

    await expect(
      upsertBallotMeasureResearchAreaTags(
        { query },
        "bm-1",
        [{ researchAreaSlug: "healthcare_affordability", stance: "for" }],
        new Map()
      )
    ).rejects.toThrow("missing research area id");
  });
});
