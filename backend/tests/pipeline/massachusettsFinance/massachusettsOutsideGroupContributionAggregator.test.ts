import { describe, expect, it } from "vitest";

import { aggregateMassachusettsOutsideGroupContributions } from "../../../src/pipeline/massachusettsFinance/massachusettsOutsideGroupContributionAggregator.js";
import type {
  MassachusettsOcpfReceiptItem,
  MassachusettsOcpfReportDetail,
} from "../../../src/pipeline/massachusettsFinance/massachusettsOcpfClient.js";
import type { MassachusettsOutsideSpendingGroup } from "../../../src/pipeline/massachusettsFinance/massachusettsOutsideSpendingAggregator.js";

function receipt(overrides: Partial<MassachusettsOcpfReceiptItem> = {}): MassachusettsOcpfReceiptItem {
  return {
    contributorName: "IBEW Local 103",
    contributorType: "Union/Association",
    recordTypeDescription: "Contribution",
    amount: 25_000,
    date: "10/25/2022",
    sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
    ...overrides,
  };
}

function report(overrides: Partial<MassachusettsOcpfReportDetail> = {}): MassachusettsOcpfReportDetail {
  return {
    reportId: 858575,
    cpfId: "81068",
    committeeName: "Local 103 International Brotherhood of Electrical Workers Independent Expenditure PAC",
    reportYear: 2022,
    reportType: "IEPAC Report",
    reportingPeriod: "2022 Pre-election",
    candidateListing: "Maura T. Healey",
    candidateSpendingBreakdown: "Maura T. Healey (Supported) $32,420.00",
    receiptsTotal: 32_420,
    expendituresTotal: 32_420,
    sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
    receipts: [receipt()],
    expenditures: [],
    ...overrides,
  };
}

function outsideGroup(overrides: Partial<MassachusettsOutsideSpendingGroup> = {}): MassachusettsOutsideSpendingGroup {
  return {
    iepacCpfId: "81068",
    iepacName: "Local 103 International Brotherhood of Electrical Workers Independent Expenditure PAC",
    supportOppose: "support",
    amount: 32_420,
    sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
    ...overrides,
  };
}

describe("massachusettsOutsideGroupContributionAggregator", () => {
  it("backtraces IE PAC organization receipts into donor and industry breakdowns", () => {
    const result = aggregateMassachusettsOutsideGroupContributions({
      electionYear: 2022,
      outsideGroups: [outsideGroup()],
      reportDetails: [
        report({
          receipts: [
            receipt({ amount: 20_000 }),
            receipt({ amount: 15_000, contributorName: "IBEW Local 103" }),
            receipt({ amount: 30_000, contributorName: "Sierra Club", contributorType: "Association" }),
          ],
        }),
      ],
    });

    expect(result).toEqual({
      matchedReceiptRowCount: 3,
      includedReceiptRowCount: 3,
      skippedReceiptRowCount: 0,
      outsideGroupBreakdowns: [
        {
          iepacCpfId: "81068",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "IBEW Local 103",
          amount: 35_000,
          contributorCount: 1,
          sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
        },
        {
          iepacCpfId: "81068",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Sierra Club",
          amount: 30_000,
          contributorCount: 1,
          sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
        },
        {
          iepacCpfId: "81068",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "labor_unions",
          amount: 35_000,
          contributorCount: 1,
          sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
        },
        {
          iepacCpfId: "81068",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "environmental_group",
          amount: 30_000,
          contributorCount: 1,
          sourceUrl: "https://www.ocpf.us/Reports/DisplayReport?menuHidden=true&id=858575",
        },
      ],
    });
  });

  it("skips side-specific donor backtrace when the same IE PAC has support and oppose spending", () => {
    const result = aggregateMassachusettsOutsideGroupContributions({
      electionYear: 2022,
      outsideGroups: [outsideGroup({ supportOppose: "support" }), outsideGroup({ supportOppose: "oppose" })],
      reportDetails: [report({ receipts: [receipt({ amount: 50_000 })] })],
    });

    expect(result.matchedReceiptRowCount).toBe(1);
    expect(result.includedReceiptRowCount).toBe(0);
    expect(result.skippedReceiptRowCount).toBe(1);
    expect(result.outsideGroupBreakdowns).toEqual([]);
  });

  it("only emits deterministic industry rows above the state threshold", () => {
    const result = aggregateMassachusettsOutsideGroupContributions({
      electionYear: 2022,
      outsideGroups: [outsideGroup()],
      reportDetails: [
        report({
          receipts: [
            receipt({ amount: 24_999.99, contributorName: "IBEW Local 103" }),
            receipt({ amount: 50_000, contributorName: "Unknown Foundation", contributorType: "Organization" }),
            receipt({ amount: 50_000, contributorName: "Jane Person", contributorType: "Individual" }),
          ],
        }),
      ],
    });

    expect(result.matchedReceiptRowCount).toBe(3);
    expect(result.includedReceiptRowCount).toBe(2);
    expect(result.skippedReceiptRowCount).toBe(1);
    expect(result.outsideGroupBreakdowns).toEqual([
      expect.objectContaining({ categoryType: "donor", categoryName: "Unknown Foundation", amount: 50_000 }),
      expect.objectContaining({ categoryType: "donor", categoryName: "IBEW Local 103", amount: 24_999.99 }),
    ]);
    expect(result.outsideGroupBreakdowns.some((row) => row.categoryType === "industry")).toBe(false);
  });

  it("skips receipts for unrelated IE PACs and invalid donor rows", () => {
    const result = aggregateMassachusettsOutsideGroupContributions({
      electionYear: 2022,
      outsideGroups: [outsideGroup()],
      reportDetails: [
        report({ cpfId: "99999", receipts: [receipt({ amount: 100_000 })] }),
        report({
          receipts: [
            receipt({ amount: 0 }),
            receipt({ amount: -10 }),
            receipt({ amount: Number.NaN }),
            receipt({ contributorName: "" }),
            receipt({ date: "12/31/2021" }),
            receipt({ contributorType: "Individual", contributorName: "Person, Pat" }),
            receipt({ contributorType: undefined, recordTypeDescription: "Individual", contributorName: "Person, Pat" }),
            receipt({ contributorType: undefined, recordTypeDescription: "Refund" }),
            receipt({ amount: 25_000 }),
          ],
        }),
      ],
    });

    expect(result.matchedReceiptRowCount).toBe(9);
    expect(result.includedReceiptRowCount).toBe(1);
    expect(result.skippedReceiptRowCount).toBe(8);
    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ categoryType: "donor", categoryName: "IBEW Local 103", amount: 25_000 }),
        expect.objectContaining({ categoryType: "industry", categoryName: "labor_unions", amount: 25_000 }),
      ])
    );
  });

  it("accepts missing receipt type because OCPF report receipts are already receipt-scoped", () => {
    const result = aggregateMassachusettsOutsideGroupContributions({
      electionYear: 2022,
      outsideGroups: [outsideGroup()],
      reportDetails: [report({ receipts: [receipt({ recordTypeDescription: undefined, amount: 25_000 })] })],
    });

    expect(result.includedReceiptRowCount).toBe(1);
    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([expect.objectContaining({ categoryType: "industry", categoryName: "labor_unions" })])
    );
  });

  it("returns every donor and industry row uncapped, sorted by amount", () => {
    const result = aggregateMassachusettsOutsideGroupContributions({
      electionYear: 2022,
      outsideGroups: [outsideGroup()],
      reportDetails: [
        report({
          receipts: [
            receipt({ contributorName: "IBEW Local 103", amount: 25_000 }),
            receipt({ contributorName: "Sierra Club", contributorType: "Association", amount: 50_000 }),
          ],
        }),
      ],
    });

    expect(result.outsideGroupBreakdowns).toEqual([
      expect.objectContaining({ categoryType: "donor", categoryName: "Sierra Club", amount: 50_000 }),
      expect.objectContaining({ categoryType: "donor", categoryName: "IBEW Local 103", amount: 25_000 }),
      expect.objectContaining({ categoryType: "industry", categoryName: "environmental_group", amount: 50_000 }),
      expect.objectContaining({ categoryType: "industry", categoryName: "labor_unions", amount: 25_000 }),
    ]);
  });

  it("keeps every IE PAC support bucket's donor and industry rows", () => {
    const result = aggregateMassachusettsOutsideGroupContributions({
      electionYear: 2022,
      outsideGroups: [
        outsideGroup({ iepacCpfId: "81068", supportOppose: "support" }),
        outsideGroup({ iepacCpfId: "81069", supportOppose: "support" }),
      ],
      reportDetails: [
        report({
          cpfId: "81068",
          receipts: [
            receipt({ contributorName: "IBEW Local 103", amount: 25_000 }),
            receipt({ contributorName: "Sierra Club", contributorType: "Association", amount: 50_000 }),
          ],
        }),
        report({
          cpfId: "81069",
          receipts: [
            receipt({ contributorName: "IBEW Local 222", amount: 75_000 }),
            receipt({ contributorName: "Sierra Club", contributorType: "Association", amount: 30_000 }),
          ],
        }),
      ],
    });

    expect(result.outsideGroupBreakdowns).toHaveLength(8);
    expect(result.outsideGroupBreakdowns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ iepacCpfId: "81068", categoryType: "donor", categoryName: "Sierra Club", amount: 50_000 }),
        expect.objectContaining({ iepacCpfId: "81068", categoryType: "donor", categoryName: "IBEW Local 103", amount: 25_000 }),
        expect.objectContaining({ iepacCpfId: "81068", categoryType: "industry", categoryName: "environmental_group", amount: 50_000 }),
        expect.objectContaining({ iepacCpfId: "81068", categoryType: "industry", categoryName: "labor_unions", amount: 25_000 }),
        expect.objectContaining({ iepacCpfId: "81069", categoryType: "donor", categoryName: "IBEW Local 222", amount: 75_000 }),
        expect.objectContaining({ iepacCpfId: "81069", categoryType: "donor", categoryName: "Sierra Club", amount: 30_000 }),
        expect.objectContaining({ iepacCpfId: "81069", categoryType: "industry", categoryName: "labor_unions", amount: 75_000 }),
        expect.objectContaining({ iepacCpfId: "81069", categoryType: "industry", categoryName: "environmental_group", amount: 30_000 }),
      ])
    );
  });

  it("validates inputs", () => {
    expect(() =>
      aggregateMassachusettsOutsideGroupContributions({ electionYear: 1999, outsideGroups: [], reportDetails: [] })
    ).toThrow("Invalid Massachusetts outside group contribution election year");
    expect(() =>
      aggregateMassachusettsOutsideGroupContributions({
        electionYear: 2022,
        outsideGroups: [],
        reportDetails: [],
        minIndustryAmount: -1,
      })
    ).toThrow("minIndustryAmount");
  });
});
