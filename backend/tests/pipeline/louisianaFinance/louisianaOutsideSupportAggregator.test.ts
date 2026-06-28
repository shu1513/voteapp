import { describe, expect, it } from "vitest";

import { aggregateLouisianaOutsideSupport } from "../../../src/pipeline/louisianaFinance/louisianaOutsideSupportAggregator.js";
import type { LouisianaCampaignFinanceCsvRow } from "../../../src/pipeline/louisianaFinance/louisianaCampaignFinanceArtifactReader.js";

function expenditure(overrides: Partial<LouisianaCampaignFinanceCsvRow> = {}): LouisianaCampaignFinanceCsvRow {
  return {
    FilerNumber: "PAC123",
    FilerLastName: "Better Louisiana PAC",
    FilerFirstName: "",
    ReportCode: "F202",
    ReportType: "F202",
    ReportNumber: "1",
    Schedule: "E-3",
    RecipientName: "John Bel Edwards",
    RecipientAddr1: "100 Main St",
    RecipientAddr2: "",
    RecipientCity: "Baton Rouge",
    RecipientState: "LA",
    RecipientZip: "70801",
    ExpenditureDescription: "Campaign contribution",
    CandidateBeneficiary: "John Bel Edwards",
    ExpenditureDate: "09/01/2027",
    ExpenditureAmt: "1,000.00",
    ...overrides,
  };
}

describe("louisianaOutsideSupportAggregator", () => {
  it("groups PAC campaign contributions to a candidate as support", () => {
    const result = aggregateLouisianaOutsideSupport({
      candidateName: "John Bel Edwards",
      electionYear: 2027,
      sourceUrl: "https://www.ethics.la.gov/",
      expenditureRows: [
        expenditure({ ExpenditureAmt: "1,000.00" }),
        expenditure({ ExpenditureAmt: "250.00", ReportNumber: "2" }),
      ],
    });

    expect(result).toEqual({
      matchedExpenditureRowCount: 2,
      includedExpenditureRowCount: 2,
      skippedExpenditureRowCount: 0,
      summary: {
        outsideSupportTotal: 1250,
        outsideOpposeTotal: 0,
        sourceUrl: "https://www.ethics.la.gov/",
        groups: [
          {
            filerNumber: "PAC123",
            filerName: "Better Louisiana PAC",
            supportOppose: "support",
            supportMechanism: "la_pac_contribution_to_candidate",
            amount: 1250,
            expenditureCount: 2,
            sourceUrl: "https://www.ethics.la.gov/",
          },
        ],
      },
    });
  });

  it("matches recipient name to a provided candidate committee alias", () => {
    const result = aggregateLouisianaOutsideSupport({
      candidateName: "John Bel Edwards",
      candidateCommitteeName: "John Bel Edwards Campaign",
      electionYear: 2027,
      expenditureRows: [
        expenditure({
          CandidateBeneficiary: "",
          RecipientName: "John Bel Edwards Campaign",
        }),
      ],
    });

    expect(result.includedExpenditureRowCount).toBe(1);
    expect(result.summary.groups).toEqual([
      expect.objectContaining({
        filerNumber: "PAC123",
        supportMechanism: "la_pac_contribution_to_candidate",
      }),
    ]);
  });

  it("skips non-PAC rows, ordinary expenses, wrong schedules, refunds, wrong cycle dates, and invalid amounts", () => {
    const result = aggregateLouisianaOutsideSupport({
      candidateName: "John Bel Edwards",
      electionYear: 2027,
      expenditureRows: [
        expenditure({ ExpenditureAmt: "500.00" }),
        expenditure({ ReportCode: "10-G", ReportType: "10-G", FilerLastName: "Edwards", FilerFirstName: "John Bel" }),
        expenditure({ ExpenditureDescription: "Media buy" }),
        expenditure({ Schedule: "E-1" }),
        expenditure({ ExpenditureDescription: "Refunded campaign contribution" }),
        expenditure({ ExpenditureDate: "09/01/2025" }),
        expenditure({ ExpenditureAmt: "0.00" }),
        expenditure({ CandidateBeneficiary: "Other Candidate", RecipientName: "Other Candidate" }),
      ],
    });

    expect(result.matchedExpenditureRowCount).toBe(7);
    expect(result.includedExpenditureRowCount).toBe(1);
    expect(result.skippedExpenditureRowCount).toBe(6);
    expect(result.summary.outsideSupportTotal).toBe(500);
  });

  it("limits groups and rejects invalid inputs", () => {
    const result = aggregateLouisianaOutsideSupport({
      candidateName: "John Bel Edwards",
      electionYear: 2027,
      maxGroups: 1,
      expenditureRows: [
        expenditure({ FilerNumber: "SMALLPAC", FilerLastName: "Small PAC", ExpenditureAmt: "100.00" }),
        expenditure({ FilerNumber: "BIGPAC", FilerLastName: "Big PAC", ExpenditureAmt: "1000.00" }),
      ],
    });

    expect(result.summary.groups).toEqual([expect.objectContaining({ filerNumber: "BIGPAC", amount: 1000 })]);
    expect(result.summary.outsideSupportTotal).toBe(1100);
    expect(() =>
      aggregateLouisianaOutsideSupport({ candidateName: "John Bel Edwards", electionYear: 1999, expenditureRows: [] })
    ).toThrow("Invalid Louisiana outside support aggregation election year");
    expect(() =>
      aggregateLouisianaOutsideSupport({ candidateName: " ", electionYear: 2027, expenditureRows: [] })
    ).toThrow("candidate name is required");
    expect(() =>
      aggregateLouisianaOutsideSupport({
        candidateName: "John Bel Edwards",
        electionYear: 2027,
        maxGroups: 0,
        expenditureRows: [],
      })
    ).toThrow("maxGroups");
  });
});
