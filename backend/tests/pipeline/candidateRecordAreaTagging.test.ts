import { describe, expect, it, vi } from "vitest";

import {
  loadAllowedResearchAreasForOfficeId,
  loadAllowedResearchAreasForElection,
  upsertCandidateRecordAreaTags,
  validateCandidateRecordAreaLabels,
} from "../../src/pipeline/candidates/candidateRecordAreaTagging.js";

describe("loadAllowedResearchAreasForElection", () => {
  it("returns office-bound areas plus general", async () => {
    const query = vi.fn().mockResolvedValueOnce({
      rows: [
        { id: "ra1", slug: "general" },
        { id: "ra2", slug: "government_efficiency" },
      ],
    });

    const result = await loadAllowedResearchAreasForElection({ query }, "election-1");

    expect(result).toEqual([
      { id: "ra1", slug: "general" },
      { id: "ra2", slug: "government_efficiency" },
    ]);
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[0]).toContain("office_research_areas");
    expect(query.mock.calls[0]?.[0]).toContain("ra.slug = 'general'");
  });
});

describe("loadAllowedResearchAreasForOfficeId", () => {
  it("returns office-bound areas plus general by office id", async () => {
    const query = vi.fn().mockResolvedValueOnce({
      rows: [
        { id: "ra1", slug: "general" },
        { id: "ra2", slug: "government_efficiency" },
      ],
    });

    const result = await loadAllowedResearchAreasForOfficeId({ query }, "office-1");

    expect(result).toEqual([
      { id: "ra1", slug: "general" },
      { id: "ra2", slug: "government_efficiency" },
    ]);
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[0]).toContain("WHERE ora.office_id = $1::uuid");
    expect(query.mock.calls[0]?.[1]).toEqual(["office-1"]);
  });
});

describe("validateCandidateRecordAreaLabels", () => {
  it("accepts office-area label with stance and general label without stance", () => {
    const allowed = new Set(["general", "government_efficiency"]);
    const result = validateCandidateRecordAreaLabels(
      [
        {
          candidateRecordId: "rec-1",
          researchAreaSlug: "government_efficiency",
          stance: "for",
        },
        {
          candidateRecordId: "rec-2",
          researchAreaSlug: "general",
          stance: null,
        },
      ],
      allowed
    );

    expect(result).toEqual({
      ok: true,
      normalized: [
        {
          candidateRecordId: "rec-1",
          researchAreaSlug: "government_efficiency",
          stance: "for",
        },
        {
          candidateRecordId: "rec-2",
          researchAreaSlug: "general",
          stance: null,
        },
      ],
    });
  });

  it("rejects label when area slug is outside office-allowed set", () => {
    const allowed = new Set(["general", "government_efficiency"]);
    const result = validateCandidateRecordAreaLabels(
      [{ candidateRecordId: "rec-1", researchAreaSlug: "immigration", stance: "for" }],
      allowed
    );

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.failures[0]?.reason).toContain("not allowed for this office");
    }
  });

  it("rejects general labels when stance is provided", () => {
    const allowed = new Set(["general"]);
    const result = validateCandidateRecordAreaLabels(
      [{ candidateRecordId: "rec-1", researchAreaSlug: "general", stance: "neutral" }],
      allowed
    );

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.failures[0]?.reason).toContain("must not include stance");
    }
  });

  it("rejects non-general labels when stance is missing", () => {
    const allowed = new Set(["general", "government_efficiency"]);
    const result = validateCandidateRecordAreaLabels(
      [{ candidateRecordId: "rec-1", researchAreaSlug: "government_efficiency", stance: null }],
      allowed
    );

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.failures[0]?.reason).toContain("requires stance");
    }
  });
});

describe("upsertCandidateRecordAreaTags", () => {
  it("upserts tags using research area id map and nullable stance", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1 });
    const labels = [
      { candidateRecordId: "rec-1", researchAreaSlug: "government_efficiency", stance: "against" as const },
      { candidateRecordId: "rec-2", researchAreaSlug: "general", stance: null },
    ];
    const map = new Map<string, string>([
      ["government_efficiency", "ra-eff"],
      ["general", "ra-general"],
    ]);

    const result = await upsertCandidateRecordAreaTags({ query }, labels, map);

    expect(result).toEqual({ processed: 2 });
    expect(query).toHaveBeenCalledTimes(2);
    expect(query.mock.calls[0]?.[0]).toContain("INSERT INTO public.candidate_record_area_tags");
    expect(query.mock.calls[1]?.[1]).toEqual(["rec-2", "ra-general", null]);
  });
});
