import { describe, expect, it } from "vitest";

import { parseCommitteeLabelPayload } from "../../src/scripts/manualFinanceCommitteeLabels.js";

function validRow() {
  return {
    source: "LOS_ANGELES_CITY_ETHICS",
    committee_id: "1461461",
    committee_name: "Streets for All Los Angeles PAC",
    label: "Transportation-advocacy PAC focused on bike and bus infrastructure",
    source_urls: ["https://ethics.lacity.org/data/campaigns/"],
  };
}

describe("parseCommitteeLabelPayload", () => {
  it("accepts a valid payload and trims fields", () => {
    const rows = parseCommitteeLabelPayload({
      labels: [{ ...validRow(), label: `  ${validRow().label}  ` }],
    });
    expect(rows).toHaveLength(1);
    expect(rows[0].label).toBe(validRow().label);
  });

  it("rejects a payload without a labels array", () => {
    expect(() => parseCommitteeLabelPayload({})).toThrow(/labels/);
    expect(() => parseCommitteeLabelPayload({ labels: [] })).toThrow(/empty/);
  });

  it("collects every problem across rows in one error", () => {
    const bad = () =>
      parseCommitteeLabelPayload({
        labels: [
          { ...validRow(), source: "NOT_A_SOURCE" },
          { ...validRow(), label: "" },
          { ...validRow(), source_urls: ["ftp://example.gov"] },
          { ...validRow(), label: "line one\nline two" },
        ],
      });
    expect(bad).toThrow(/unknown source "NOT_A_SOURCE"/);
    expect(bad).toThrow(/labels\[1\]: label is required/);
    expect(bad).toThrow(/not a valid http\(s\) URL/);
    expect(bad).toThrow(/must be a single line/);
  });

  it("rejects duplicate (source, committee_id) pairs", () => {
    expect(() => parseCommitteeLabelPayload({ labels: [validRow(), validRow()] })).toThrow(/duplicate/);
  });

  it("rejects labels over the length cap", () => {
    expect(() =>
      parseCommitteeLabelPayload({ labels: [{ ...validRow(), label: "x".repeat(201) }] })
    ).toThrow(/exceeds 200 characters/);
  });
});
