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

  it("rejects a same-race row whose middle name contradicts the candidate", () => {
    // Same office, borough, and year — only the middle evidence differs.
    // Without the middle gate this row linked as an exact match and attached
    // the other Jane Doe's CFB filings.
    expect(resolveNewYorkCityCandidate({
      candidateName: "Jane Q. Doe", electionYear: 2025, officeScope: "place", officeCanonicalName: "Mayor",
      districtGeoid: "3651000", analysisRows: [row({ candidateName: "DOE, JANE R." })],
    })).toEqual({ status: "unmatched", reason: "no_exact_match" });
  });

  it("accepts an initial that corroborates the full middle name", () => {
    expect(resolveNewYorkCityCandidate({
      candidateName: "Jane Q. Doe", electionYear: 2025, officeScope: "place", officeCanonicalName: "Mayor",
      districtGeoid: "3651000", analysisRows: [row({ candidateName: "DOE, JANE QUINN" })],
    }).status).toBe("matched");
  });

  it("still falls back to first+last when a side lacks middle info", () => {
    expect(resolveNewYorkCityCandidate({
      candidateName: "Jane Doe", electionYear: 2025, officeScope: "place", officeCanonicalName: "Mayor",
      districtGeoid: "3651000", analysisRows: [row({ candidateName: "DOE, JANE R." })],
    }).status).toBe("matched");
  });

  it("reads a bare trailing V as a middle initial, not a generational suffix", () => {
    // GENERATIONAL_SUFFIX_RANK deliberately excludes "V": a trailing "V" is far
    // more often a middle initial than a fifth generation, so it has to survive
    // normalization as middle evidence on EITHER side of the comparison.
    const status = (candidateName: string, rowCandidateName: string): string =>
      resolveNewYorkCityCandidate({
        candidateName, electionYear: 2025, officeScope: "place", officeCanonicalName: "Mayor",
        districtGeoid: "3651000", analysisRows: [row({ candidateName: rowCandidateName })],
      }).status;

    expect(status("Jane V. Doe", "DOE, JANE R.")).toBe("unmatched");
    expect(status("Jane R. Doe", "DOE, JANE V")).toBe("unmatched");
    expect(status("Jane V. Doe", "DOE, JANE V")).toBe("matched");
    expect(status("Jane Doe", "DOE, JANE V")).toBe("matched");
  });

  it("refuses ambiguous candidate IDs", () => {
    const result = resolveNewYorkCityCandidate({
      candidateName: "Jane Doe", electionYear: 2025, officeScope: "place", officeCanonicalName: "Mayor",
      districtGeoid: "3651000", analysisRows: [row(), row({ candidateId: "A2" })],
    });
    expect(result).toEqual({ status: "ambiguous", reason: "multiple_exact_matches", matches: ["A1", "A2"] });
  });
});
