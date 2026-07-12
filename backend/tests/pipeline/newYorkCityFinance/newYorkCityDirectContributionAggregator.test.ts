import { describe, expect, it } from "vitest";

import { aggregateNewYorkCityDirectContributions } from "../../../src/pipeline/newYorkCityFinance/newYorkCityDirectContributionAggregator.js";
import type { NewYorkCityCfbContributionRow } from "../../../src/pipeline/newYorkCityFinance/newYorkCityCfbCsv.js";

function row(overrides: Partial<NewYorkCityCfbContributionRow> = {}): NewYorkCityCfbContributionRow {
  return {
    electionYear: 2025, officeCode: "1", candidateId: "A1", candidateName: "DOE, JANE", filing: 10,
    schedule: "ABC", referenceNumber: "R1", contributorName: "Alex Smith", contributorType: "IND",
    occupation: "Teacher", employer: "NYC DOE", amount: 200, adjustmentType: null, ...overrides,
  };
}

describe("newYorkCityDirectContributionAggregator", () => {
  it("aggregates occupations, employers, and sizes with refunds", () => {
    const result = aggregateNewYorkCityDirectContributions({
      candidateId: "A1", electionYear: 2025, officeCode: "1",
      rows: [
        row(),
        row({ referenceNumber: "R2", schedule: "M", amount: -50 }),
        row({ candidateId: "OTHER" }),
        row({ officeCode: "2", referenceNumber: "R3", amount: 999 }),
      ],
    });
    expect(result.acceptedRowCount).toBe(2);
    expect(result.breakdowns).toEqual(expect.arrayContaining([
      expect.objectContaining({ categoryType: "occupation", categoryName: "Teacher", amount: 150, contributorCount: 1 }),
      expect.objectContaining({ categoryType: "employer", categoryName: "NYC DOE", amount: 150, contributorCount: 1 }),
      expect.objectContaining({ categoryType: "contribution_size", categoryName: "$100-$249", amount: 200 }),
    ]));
  });

  it("keeps only latest filing for duplicate transaction identity", () => {
    const result = aggregateNewYorkCityDirectContributions({
      candidateId: "A1", electionYear: 2025, officeCode: "1",
      rows: [row({ filing: 10, amount: 200 }), row({ filing: 12, amount: 300 })],
    });
    expect(result.acceptedRowCount).toBe(1);
    expect(result.breakdowns).toContainEqual(expect.objectContaining({ categoryType: "occupation", amount: 300 }));
  });

  it("ignores unsupported schedules and never emits non-positive categories", () => {
    const result = aggregateNewYorkCityDirectContributions({
      candidateId: "A1", electionYear: 2025, officeCode: "1",
      rows: [row({ schedule: "N" }), row({ schedule: "M", referenceNumber: "R2", amount: -500 })],
    });
    expect(result.ignoredRowCount).toBe(1);
    expect(result.breakdowns).toEqual([]);
  });
});
