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

  it("rejects out-of-scope research area slug and names the slug in the reason", () => {
    const parsed = parseCandidateRecordAreaLabelPayload(
      {
        labels: [
          { record_index: 0, research_area_slug: "general" },
          { record_index: 1, research_area_slug: "immigration", stance: "for" },
        ],
      },
      { allowedResearchAreaSlugs: new Set(["general", "government_efficiency"]) }
    );

    expect(parsed.ok).toBe(false);
    if (!parsed.ok) {
      expect(parsed.reason).toContain("payload.labels contains invalid row");
      expect(parsed.reason).toContain("labels[1]");
      expect(parsed.reason).toContain("'immigration' is not in the allowed research areas");
    }
  });

  it("reports every invalid row in one pass and prints the office allowlist once", () => {
    // Fail-fast label validation forced serial repair — one defect per
    // dry-run (a live 17-record payload took three cycles for three label
    // problems). All rows must surface together, with the office's allowed
    // set attached so the repair needs no discovery dry-run.
    const parsed = parseCandidateRecordAreaLabelPayload(
      {
        labels: [
          { record_index: 0, research_area_slug: "general" },
          { record_index: 1, research_area_slug: "immigration", stance: "for" },
          { record_index: 2, research_area_slug: "government_efficiency" },
          { record_index: 3, research_area_slug: "integrity_and_ethics", stance: "for" },
        ],
      },
      { allowedResearchAreaSlugs: new Set(["general", "government_efficiency", "integrity_and_ethics"]) }
    );

    expect(parsed.ok).toBe(false);
    if (!parsed.ok) {
      expect(parsed.reason).toContain("payload.labels contains invalid row");
      expect(parsed.reason).toContain("labels[1]: research_area_slug 'immigration' is not in the allowed research areas");
      expect(parsed.reason).toContain("labels[2]: stance is required for research_area_slug 'government_efficiency'");
      expect(parsed.reason).toContain("labels[3]: stance is not allowed for non-stance area 'integrity_and_ethics'");
      expect(parsed.reason).toContain(
        "allowed research areas for this office: general, government_efficiency, integrity_and_ethics"
      );
    }
  });

  it("reports a duplicate pair alongside other invalid rows instead of failing fast", () => {
    // Failing fast on the duplicate discarded already-collected row errors
    // (and the allowlist hint), re-introducing serial repair.
    const parsed = parseCandidateRecordAreaLabelPayload(
      {
        labels: [
          { record_index: 0, research_area_slug: "immigration", stance: "for" },
          { record_index: 1, research_area_slug: "general" },
          { record_index: 1, research_area_slug: "general" },
        ],
      },
      { allowedResearchAreaSlugs: new Set(["general", "government_efficiency"]) }
    );

    expect(parsed.ok).toBe(false);
    if (!parsed.ok) {
      expect(parsed.reason).toContain("payload.labels contains invalid row");
      expect(parsed.reason).toContain("labels[0]: research_area_slug 'immigration' is not in the allowed research areas");
      expect(parsed.reason).toContain("labels[2]: duplicate (record_index, research_area_slug) pair");
      expect(parsed.reason).toContain("allowed research areas for this office: general, government_efficiency");
    }
  });

  it("names the cause when stance is missing on a stance-bearing area", () => {
    const parsed = parseCandidateRecordAreaLabelPayload(
      {
        labels: [{ record_index: 0, research_area_slug: "government_efficiency" }],
      },
      { allowedResearchAreaSlugs: new Set(["government_efficiency"]) }
    );

    expect(parsed.ok).toBe(false);
    if (!parsed.ok) {
      expect(parsed.reason).toContain("stance is required for research_area_slug 'government_efficiency'");
    }
  });

  it("names the cause when record_index is out of range", () => {
    const parsed = parseCandidateRecordAreaLabelPayload(
      {
        labels: [{ record_index: 5, research_area_slug: "general" }],
      },
      { allowedResearchAreaSlugs: new Set(["general"]), recordCount: 2 }
    );

    expect(parsed.ok).toBe(false);
    if (!parsed.ok) {
      expect(parsed.reason).toContain("record_index 5 is out of range (record count 2)");
    }
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
      labels: [{ record_index: 0, research_area_slug: "general", stance: "for" }],
    });

    expect(parsed.ok).toBe(false);
  });

  it("rejects stance on integrity_and_ethics", () => {
    const parsed = parseCandidateRecordAreaLabelPayload({
      labels: [{ record_index: 0, research_area_slug: "integrity_and_ethics", stance: "against" }],
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
