import { describe, expect, it } from "vitest";

import { financeClassificationKey } from "../../src/pipeline/finance/financeIndustryClassificationService.js";
import {
  checkRowsAgainstKnownLabels,
  parseIndustryLabelPayload,
} from "../../src/scripts/manualFinanceIndustryLabels.js";

function validRow() {
  return {
    label_type: "employer",
    raw_label: "Disney",
    industry_slug: "technology",
    confidence: "high",
  };
}

describe("parseIndustryLabelPayload", () => {
  it("accepts a valid payload, trims, and derives the normalized label", () => {
    const rows = parseIndustryLabelPayload({
      labels: [{ ...validRow(), raw_label: "  Belkin International Inc  " }],
    });
    expect(rows).toHaveLength(1);
    expect(rows[0].raw_label).toBe("Belkin International Inc");
    expect(rows[0].normalized_label).toBe("BELKIN INTERNATIONAL");
    expect(rows[0].industry_slug).toBe("technology");
  });

  it("accepts an explicit null industry_slug as a no-industry verdict", () => {
    const rows = parseIndustryLabelPayload({
      labels: [{ ...validRow(), raw_label: "City of Los Angeles", industry_slug: null }],
    });
    expect(rows[0].industry_slug).toBeNull();
  });

  it("rejects a payload without a labels array", () => {
    expect(() => parseIndustryLabelPayload({})).toThrow(/labels/);
    expect(() => parseIndustryLabelPayload({ labels: [] })).toThrow(/empty/);
  });

  it("collects every problem across rows in one error", () => {
    const bad = () =>
      parseIndustryLabelPayload({
        labels: [
          { ...validRow(), label_type: "occupation" },
          { ...validRow(), raw_label: "" },
          { ...validRow(), industry_slug: "entertainment" },
          { ...validRow(), confidence: "unknown" },
        ],
      });
    expect(bad).toThrow(/labels\[0\]: label_type must be "employer" or "donor"/);
    expect(bad).toThrow(/labels\[1\]: raw_label is required/);
    expect(bad).toThrow(/labels\[2\]: industry_slug must be null or one of/);
    expect(bad).toThrow(/labels\[3\]: confidence must be "high", "medium", or "low"/);
  });

  it("rejects a missing industry_slug key instead of treating it as null", () => {
    const { industry_slug: _slug, ...withoutSlug } = validRow();
    expect(() => parseIndustryLabelPayload({ labels: [withoutSlug] })).toThrow(
      /industry_slug is required \(use null/
    );
  });

  it("rejects raw labels that normalize to nothing", () => {
    expect(() =>
      parseIndustryLabelPayload({ labels: [{ ...validRow(), raw_label: "()" }] })
    ).toThrow(/normalizes to an empty label/);
  });

  it("rejects two raw labels that collide on the same normalized label", () => {
    expect(() =>
      parseIndustryLabelPayload({
        labels: [validRow(), { ...validRow(), raw_label: "DISNEY  Inc" }],
      })
    ).toThrow(/duplicate \(label_type, normalized label\)/);
  });

  it("allows the same raw label under both label types", () => {
    expect(
      parseIndustryLabelPayload({ labels: [validRow(), { ...validRow(), label_type: "donor" }] })
    ).toHaveLength(2);
  });
});

describe("checkRowsAgainstKnownLabels", () => {
  const rows = parseIndustryLabelPayload({ labels: [validRow()] });

  it("passes when the normalized label already has a classification row", () => {
    const known = new Set([financeClassificationKey("employer", "DISNEY")]);
    expect(checkRowsAgainstKnownLabels(rows, known)).toEqual([]);
  });

  it("rejects a label no finance sync has persisted, including a label_type mismatch", () => {
    const known = new Set([financeClassificationKey("donor", "DISNEY")]);
    const issues = checkRowsAgainstKnownLabels(rows, known);
    expect(issues).toHaveLength(1);
    expect(issues[0].kind).toBe("missing_label");
    expect(issues[0].reason).toMatch(/has no classification row — copy label_type and raw_label from the due list/);
  });
});
