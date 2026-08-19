import { describe, expect, it, vi } from "vitest";

import {
  loadAllResearchAreas,
  loadAllowedResearchAreasForOfficeId,
  upsertCandidateRecordAreaTags,
  validateCandidateRecordAreaLabels,
} from "../../src/pipeline/candidates/candidateRecordAreaTagging.js";

describe("loadAllowedResearchAreasForOfficeId", () => {
  it("returns office-bound areas plus universal non-stance areas by office id", async () => {
    const query = vi.fn().mockResolvedValueOnce({
      rows: [
        { id: "ra1", slug: "general" },
        { id: "ra3", slug: "integrity_and_ethics" },
        { id: "ra2", slug: "government_efficiency" },
      ],
    });

    const result = await loadAllowedResearchAreasForOfficeId({ query }, "office-1");

    expect(result).toEqual([
      { id: "ra1", slug: "general" },
      { id: "ra3", slug: "integrity_and_ethics" },
      { id: "ra2", slug: "government_efficiency" },
    ]);
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[0]).toContain("WHERE ora.office_id = $1::uuid");
    expect(query.mock.calls[0]?.[0]).toContain("SELECT DISTINCT id, slug, description");
    expect(query.mock.calls[0]?.[1]).toEqual([
      "office-1",
      ["general", "integrity_and_ethics"],
    ]);
  });
});

describe("loadAllResearchAreas", () => {
  it("returns every research area for unmatched-office fallback", async () => {
    const query = vi.fn().mockResolvedValueOnce({
      rows: [
        { id: "ra1", slug: "general" },
        { id: "ra2", slug: "government_efficiency" },
        { id: "ra3", slug: "integrity_and_ethics" },
      ],
    });

    const result = await loadAllResearchAreas({ query });

    expect(result).toEqual([
      { id: "ra1", slug: "general" },
      { id: "ra2", slug: "government_efficiency" },
      { id: "ra3", slug: "integrity_and_ethics" },
    ]);
    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[0]).toContain("FROM public.research_areas");
    expect(query.mock.calls[0]?.[0]).toContain("SELECT id, slug, description");
    expect(query.mock.calls[0]?.[0]).not.toContain("office_research_areas");
  });
});

describe("validateCandidateRecordAreaLabels", () => {
  it("accepts office-area label with stance and universal labels without stance", () => {
    const allowed = new Set(["general", "integrity_and_ethics", "government_efficiency"]);
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
        {
          candidateRecordId: "rec-3",
          researchAreaSlug: "integrity_and_ethics",
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
        {
          candidateRecordId: "rec-3",
          researchAreaSlug: "integrity_and_ethics",
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
      expect(result.failures[0]?.reason).toContain("not allowed for this candidate/election context");
    }
  });

  it("rejects general labels when stance is provided", () => {
    const allowed = new Set(["general"]);
    const result = validateCandidateRecordAreaLabels(
      [{ candidateRecordId: "rec-1", researchAreaSlug: "general", stance: "neutral" as never }],
      allowed
    );

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.failures[0]?.reason).toContain("must not include stance");
    }
  });

  it("rejects integrity_and_ethics labels when stance is provided", () => {
    const allowed = new Set(["integrity_and_ethics"]);
    const result = validateCandidateRecordAreaLabels(
      [{ candidateRecordId: "rec-1", researchAreaSlug: "integrity_and_ethics", stance: "neutral" as never }],
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

  it("rejects neutral stance for specific research areas", () => {
    const allowed = new Set(["government_efficiency"]);
    const result = validateCandidateRecordAreaLabels(
      [{ candidateRecordId: "rec-1", researchAreaSlug: "government_efficiency", stance: "neutral" as never }],
      allowed
    );

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.failures[0]?.reason).toContain("requires stance (for|against)");
    }
  });
});

describe("upsertCandidateRecordAreaTags", () => {
  it("upserts tags using research area id map and nullable stance", async () => {
    const query = vi.fn().mockResolvedValue({ rowCount: 1 });
    const labels = [
      { candidateRecordId: "rec-1", researchAreaSlug: "government_efficiency", stance: "against" as const },
      { candidateRecordId: "rec-2", researchAreaSlug: "general", stance: null },
      { candidateRecordId: "rec-3", researchAreaSlug: "integrity_and_ethics", stance: null },
    ];
    const map = new Map<string, string>([
      ["government_efficiency", "ra-eff"],
      ["general", "ra-general"],
      ["integrity_and_ethics", "ra-legal"],
    ]);

    const result = await upsertCandidateRecordAreaTags({ query }, labels, map);

    expect(result).toEqual({ processed: 3 });
    expect(query).toHaveBeenCalledTimes(3);
    expect(query.mock.calls[0]?.[0]).toContain("INSERT INTO public.candidate_record_area_tags");
    expect(query.mock.calls[1]?.[1]).toEqual(["rec-2", "ra-general", null]);
    expect(query.mock.calls[2]?.[1]).toEqual(["rec-3", "ra-legal", null]);
  });
});
