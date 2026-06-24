import { describe, expect, it } from "vitest";

import {
  aggregateMassachusettsOutsideSpending,
  isMassachusettsIndependentExpenditure,
  supportOpposeFromMassachusettsOcpfIsSupported,
} from "../../../src/pipeline/massachusettsFinance/massachusettsOutsideSpendingAggregator.js";
import type {
  MassachusettsOcpfExpenditureItem,
  MassachusettsOcpfReportDetail,
} from "../../../src/pipeline/massachusettsFinance/massachusettsOcpfClient.js";

function expenditure(overrides: Partial<MassachusettsOcpfExpenditureItem> = {}): MassachusettsOcpfExpenditureItem {
  return {
    affectedCandidateName: "Maura T. Healey",
    relatedCpfId: "15710",
    isSupported: true,
    recordTypeDescription: "Independent Expenditure",
    ieInfo: "digital ads in support of Maura T. Healey",
    amount: 10_000,
    date: "10/15/2022",
    sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
    ...overrides,
  };
}

function report(overrides: Partial<MassachusettsOcpfReportDetail> = {}): MassachusettsOcpfReportDetail {
  return {
    reportId: 858575,
    cpfId: "16116",
    committeeName: "Local 103 International Brotherhood of Electrical Workers Independent Expenditure PAC",
    reportYear: 2022,
    reportType: "IEPAC Report",
    reportingPeriod: "2022 Pre-election",
    candidateListing: "Maura T. Healey",
    candidateSpendingBreakdown: "Maura T. Healey (Supported) $10,000.00",
    receiptsTotal: 200_000,
    expendituresTotal: 10_000,
    sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
    receipts: [],
    expenditures: [expenditure()],
    ...overrides,
  };
}

describe("massachusettsOutsideSpendingAggregator", () => {
  it("aggregates support and opposition by exact related CPF ID", () => {
    const sourceUrl = "https://api.ocpf.us/miscreports/iepacs/reports/2022";
    const result = aggregateMassachusettsOutsideSpending({
      candidateCpfId: "15710",
      electionYear: 2022,
      sourceUrl,
      reportDetails: [
        report({
          expenditures: [expenditure({ amount: 4_503 }), expenditure({ amount: 25_517, date: "2022-10-20" })],
        }),
        report({
          reportId: 866213,
          cpfId: "16901",
          committeeName: "Mass Freedom Independent Expenditure PAC",
          sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=866213",
          expenditures: [
            expenditure({ relatedCpfId: "14907", affectedCandidateName: "Geoffrey Diehl", amount: 70_000 }),
            expenditure({
              relatedCpfId: "15710",
              isSupported: false,
              amount: 70_000,
              ieInfo: "in opposition to Maura T. Healey",
            }),
          ],
        }),
      ],
    });

    expect(result).toEqual({
      summary: {
        supportTotal: 30020,
        opposeTotal: 70000,
        groups: [
          {
            iepacCpfId: "16901",
            iepacName: "Mass Freedom Independent Expenditure PAC",
            supportOppose: "oppose",
            amount: 70000,
            sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=866213",
          },
          {
            iepacCpfId: "16116",
            iepacName: "Local 103 International Brotherhood of Electrical Workers Independent Expenditure PAC",
            supportOppose: "support",
            amount: 30020,
            sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
          },
        ],
        sourceUrl,
      },
      matchedExpenditureRowCount: 3,
      includedExpenditureRowCount: 3,
      skippedExpenditureRowCount: 0,
    });
  });

  it("collapses the same IE PAC and stance across reports", () => {
    const result = aggregateMassachusettsOutsideSpending({
      candidateCpfId: "15710",
      electionYear: 2022,
      reportDetails: [
        report({ expenditures: [expenditure({ amount: 100 })] }),
        report({ reportId: 858576, expenditures: [expenditure({ amount: 200 })] }),
      ],
    });

    expect(result.summary).toEqual({
      supportTotal: 300,
      opposeTotal: 0,
      groups: [
        expect.objectContaining({
          iepacCpfId: "16116",
          supportOppose: "support",
          amount: 300,
        }),
      ],
      sourceUrl: null,
    });
    expect(result.matchedExpenditureRowCount).toBe(2);
    expect(result.includedExpenditureRowCount).toBe(2);
    expect(result.skippedExpenditureRowCount).toBe(0);
  });

  it("skips rows without strict structured proof", () => {
    const result = aggregateMassachusettsOutsideSpending({
      candidateCpfId: "15710",
      electionYear: 2022,
      reportDetails: [
        report({
          expenditures: [
            expenditure({ relatedCpfId: "99999", amount: 999_999 }),
            expenditure({ isSupported: null, amount: 100 }),
            expenditure({ recordTypeDescription: "Operating Expenditure", amount: 200 }),
            expenditure({ date: "12/31/2021", amount: 300 }),
            expenditure({ amount: 0 }),
            expenditure({ amount: Number.NaN }),
          ],
        }),
        report({ cpfId: "", expenditures: [expenditure({ amount: 400 })] }),
        report({ committeeName: " ", expenditures: [expenditure({ amount: 500 })] }),
      ],
    });

    expect(result).toEqual({
      summary: null,
      matchedExpenditureRowCount: 7,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 7,
    });
  });

  it("does not infer candidate matches from names or free text", () => {
    const result = aggregateMassachusettsOutsideSpending({
      candidateCpfId: "15710",
      electionYear: 2022,
      reportDetails: [
        report({
          expenditures: [
            expenditure({
              relatedCpfId: undefined,
              affectedCandidateName: "Maura T. Healey",
              ieInfo: "support Maura T. Healey for Governor",
              amount: 5_000,
            }),
          ],
        }),
      ],
    });

    expect(result).toEqual({
      summary: null,
      matchedExpenditureRowCount: 0,
      includedExpenditureRowCount: 0,
      skippedExpenditureRowCount: 0,
    });
  });

  it("limits groups after sorting by amount", () => {
    const result = aggregateMassachusettsOutsideSpending({
      candidateCpfId: "15710",
      electionYear: 2022,
      maxGroups: 1,
      reportDetails: [
        report({ cpfId: "1", committeeName: "Small PAC", expenditures: [expenditure({ amount: 100 })] }),
        report({ cpfId: "2", committeeName: "Large PAC", expenditures: [expenditure({ amount: 500 })] }),
      ],
    });

    expect(result.summary?.groups).toEqual([expect.objectContaining({ iepacCpfId: "2", iepacName: "Large PAC" })]);
    expect(result.summary?.supportTotal).toBe(600);
  });

  it("maps OCPF isSupported values to support and oppose labels", () => {
    expect(supportOpposeFromMassachusettsOcpfIsSupported(true)).toBe("support");
    expect(supportOpposeFromMassachusettsOcpfIsSupported(false)).toBe("oppose");
    expect(supportOpposeFromMassachusettsOcpfIsSupported(null)).toBeNull();
  });

  it("requires explicit independent-expenditure record type", () => {
    expect(isMassachusettsIndependentExpenditure(expenditure())).toBe(true);
    expect(isMassachusettsIndependentExpenditure(expenditure({ recordTypeDescription: " Independent Expenditure " }))).toBe(
      true
    );
    expect(isMassachusettsIndependentExpenditure(expenditure({ recordTypeDescription: "Operating Expenditure" }))).toBe(
      false
    );
    expect(isMassachusettsIndependentExpenditure(expenditure({ recordTypeDescription: undefined }))).toBe(false);
  });

  it("validates required aggregation inputs", () => {
    expect(() =>
      aggregateMassachusettsOutsideSpending({ candidateCpfId: " ", electionYear: 2022, reportDetails: [] })
    ).toThrow("Massachusetts candidate CPF ID is required");
    expect(() =>
      aggregateMassachusettsOutsideSpending({ candidateCpfId: "15710", electionYear: 1999, reportDetails: [] })
    ).toThrow("Invalid Massachusetts outside spending aggregation election year");
    expect(() =>
      aggregateMassachusettsOutsideSpending({ candidateCpfId: "15710", electionYear: 2022, reportDetails: [], maxGroups: 0 })
    ).toThrow("maxGroups");
  });
});
