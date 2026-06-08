import { describe, expect, it } from "vitest";

import { parseCandidateRecordAreaLabelPayload } from "../../src/contracts/candidateRecordAreaLabelPayloadContract.js";

describe("parseCandidateRecordAreaLabelPayload", () => {
  it("parses valid labels with stance areas and non-stance universal areas", () => {
    const parsed = parseCandidateRecordAreaLabelPayload(
      {
        labels: [
          { record_index: 0, research_area_slug: "government_efficiency", stance: "for" },
          { record_index: 1, research_area_slug: "general" },
          { record_index: 2, research_area_slug: "integrity_and_ethics" },
        ],
      },
      {
        allowedResearchAreaSlugs: new Set(["general", "integrity_and_ethics", "government_efficiency"]),
        recordCount: 3,
        requireLabelForEveryRecord: true,
      }
    );

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }
    expect(parsed.payload.labels).toEqual([
      { record_index: 0, research_area_slug: "government_efficiency", stance: "for" },
      { record_index: 1, research_area_slug: "general" },
      { record_index: 2, research_area_slug: "integrity_and_ethics" },
    ]);
  });

  it("rejects out-of-scope research area slug", () => {
    const parsed = parseCandidateRecordAreaLabelPayload(
      {
        labels: [{ record_index: 0, research_area_slug: "immigration", stance: "for" }],
      },
      { allowedResearchAreaSlugs: new Set(["general", "government_efficiency"]) }
    );

    expect(parsed.ok).toBe(false);
  });

  it("rejects neutral stance on specific research areas", () => {
    const parsed = parseCandidateRecordAreaLabelPayload(
      {
        labels: [{ record_index: 0, research_area_slug: "government_efficiency", stance: "neutral" }],
      },
      { allowedResearchAreaSlugs: new Set(["government_efficiency"]) }
    );

    expect(parsed.ok).toBe(false);
  });

  it("rejects stance on general", () => {
    const parsed = parseCandidateRecordAreaLabelPayload({
      labels: [{ record_index: 0, research_area_slug: "general", stance: "neutral" }],
    });

    expect(parsed.ok).toBe(false);
  });

  it("rejects stance on integrity_and_ethics", () => {
    const parsed = parseCandidateRecordAreaLabelPayload({
      labels: [{ record_index: 0, research_area_slug: "integrity_and_ethics", stance: "neutral" }],
    });

    expect(parsed.ok).toBe(false);
  });

  it("rejects missing stance on non-general label", () => {
    const parsed = parseCandidateRecordAreaLabelPayload({
      labels: [{ record_index: 0, research_area_slug: "government_efficiency" }],
    });

    expect(parsed.ok).toBe(false);
  });

  it("rejects missing coverage when requireLabelForEveryRecord is true", () => {
    const parsed = parseCandidateRecordAreaLabelPayload(
      {
        labels: [{ record_index: 0, research_area_slug: "general" }],
      },
      { recordCount: 2, requireLabelForEveryRecord: true, allowedResearchAreaSlugs: new Set(["general"]) }
    );

    expect(parsed.ok).toBe(false);
    if (!parsed.ok) {
      expect(parsed.reason).toContain("missing at least one label for record_index=1");
    }
  });
});
