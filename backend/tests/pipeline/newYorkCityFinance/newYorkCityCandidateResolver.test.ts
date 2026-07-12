import { describe, expect, it } from "vitest";

import { resolveNewYorkCityCandidate } from "../../../src/pipeline/newYorkCityFinance/newYorkCityCandidateResolver.js";
import type { NewYorkCityCfbFinancialAnalysisRow } from "../../../src/pipeline/newYorkCityFinance/newYorkCityCfbCsv.js";

function row(overrides: Partial<NewYorkCityCfbFinancialAnalysisRow> = {}): NewYorkCityCfbFinancialAnalysisRow {
  return {
    electionYear: 2025, fromStatement: 1, toStatement: 10, officeCode: "1", candidateName: "DOE, JANE",
    candidateId: "A1", boroughCode: null, privateContributions: 100, publicFunds: 0, netExpenditures: 50,
    outstandingBills: 0, ...overrides,
  };
}

describe("newYorkCityCandidateResolver", () => {
  it("matches exact normalized identity and takes latest statement", () => {
    const result = resolveNewYorkCityCandidate({
      candidateName: "Jane Q. Doe", electionYear: 2025, officeScope: "place", officeCanonicalName: "Mayor",
      districtGeoid: "3651000", analysisRows: [row(), row({ toStatement: 16, privateContributions: 200 })],
    });
    expect(result.status).toBe("matched");
    if (result.status === "matched") expect(result.summary.privateContributions).toBe(200);
  });

  it("requires exact office and borough", () => {
    expect(resolveNewYorkCityCandidate({
      candidateName: "Jane Doe", electionYear: 2025, officeScope: "county", officeCanonicalName: "Borough President",
      districtGeoid: "36047", analysisRows: [row({ officeCode: "4", boroughCode: "Q" })],
    })).toEqual({ status: "unmatched", reason: "no_exact_match" });
  });

  it("refuses ambiguous candidate IDs", () => {
    const result = resolveNewYorkCityCandidate({
      candidateName: "Jane Doe", electionYear: 2025, officeScope: "place", officeCanonicalName: "Mayor",
      districtGeoid: "3651000", analysisRows: [row(), row({ candidateId: "A2" })],
    });
    expect(result).toEqual({ status: "ambiguous", reason: "multiple_exact_matches", matches: ["A1", "A2"] });
  });
});
