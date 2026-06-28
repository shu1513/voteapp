import { describe, expect, it } from "vitest";

import { aggregateLouisianaOutsideGroupContributions } from "../../../src/pipeline/louisianaFinance/louisianaOutsideGroupContributionAggregator.js";
import type { LouisianaCampaignFinanceCsvRow } from "../../../src/pipeline/louisianaFinance/louisianaCampaignFinanceArtifactReader.js";
import type { LouisianaOutsideSupportGroup } from "../../../src/pipeline/louisianaFinance/louisianaOutsideSupportAggregator.js";

function contribution(overrides: Partial<LouisianaCampaignFinanceCsvRow> = {}): LouisianaCampaignFinanceCsvRow {
  return {
    FilerNumber: "PAC123",
    FilerLastName: "Better Louisiana PAC",
    FilerFirstName: "",
    ReportCode: "F202",
    ReportType: "F202",
    ReportNumber: "1",
    ContributorTypeCode: "BUS",
    ContributorName: "Google LLC",
    ContributorAddr1: "100 Main St",
    ContributorAddr2: "",
    ContributorCity: "Baton Rouge",
    ContributorrState: "LA",
    ContributorZip: "70801",
    ContributionType: "MONETARY",
    ContributionDescription: "",
    ContributionDate: "08/01/2027",
    ContributionAmt: "100.00",
    ContributionDesignatedElectionAdditionInfo: "",
    ...overrides,
  };
}

function outsideGroup(overrides: Partial<LouisianaOutsideSupportGroup> = {}): LouisianaOutsideSupportGroup {
  return {
    filerNumber: "PAC123",
    filerName: "Better Louisiana PAC",
    supportOppose: "support",
    supportMechanism: "la_pac_contribution_to_candidate",
    amount: 1000,
    expenditureCount: 1,
    sourceUrl: "https://www.ethics.la.gov/",
    ...overrides,
  };
}

describe("louisianaOutsideGroupContributionAggregator", () => {
  it("backtraces supporting PAC receipts into organization donors and industries", () => {
    const result = aggregateLouisianaOutsideGroupContributions({
      electionYear: 2027,
      sourceUrl: "https://www.ethics.la.gov/",
      outsideGroups: [outsideGroup()],
      contributionRows: [
        contribution({ ContributionAmt: "20,000.00", ContributorName: "Google LLC", ContributorTypeCode: "BUS" }),
        contribution({ ContributionAmt: "15,000.00", ContributorName: "Google LLC", ContributorTypeCode: "BUS" }),
        contribution({ ContributionAmt: "30,000.00", ContributorName: "IBEW Local 300", ContributorTypeCode: "PAC" }),
        contribution({ ContributionAmt: "50,000.00", ContributorName: "Doe, Jane", ContributorTypeCode: "IND" }),
      ],
    });

    expect(result).toEqual({
      matchedContributionRowCount: 4,
      includedContributionRowCount: 3,
      skippedContributionRowCount: 1,
      classifications: [
        {
          rawLabel: "Google LLC",
          labelType: "donor",
          normalizedLabel: "GOOGLE",
          industrySlug: "technology",
          confidence: "high",
          classificationSource: "rule",
          matchedRule: "organization_exact_google",
        },
        {
          rawLabel: "IBEW Local 300",
          labelType: "donor",
          normalizedLabel: "IBEW LOCAL 300",
          industrySlug: "labor_unions",
          confidence: "medium",
          classificationSource: "rule",
          matchedRule: "organization_pattern_labor_unions",
        },
      ],
      outsideGroupBreakdowns: [
        {
          filerNumber: "PAC123",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "Google LLC",
          amount: 35000,
          contributorCount: 1,
          sourceUrl: "https://www.ethics.la.gov/",
        },
        {
          filerNumber: "PAC123",
          supportOppose: "support",
          categoryType: "donor",
          categoryName: "IBEW Local 300",
          amount: 30000,
          contributorCount: 1,
          sourceUrl: "https://www.ethics.la.gov/",
        },
        {
          filerNumber: "PAC123",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "technology",
          amount: 35000,
          contributorCount: 1,
          sourceUrl: "https://www.ethics.la.gov/",
        },
        {
          filerNumber: "PAC123",
          supportOppose: "support",
          categoryType: "industry",
          categoryName: "labor_unions",
          amount: 30000,
          contributorCount: 1,
          sourceUrl: "https://www.ethics.la.gov/",
        },
      ],
    });
  });

  it("skips ambiguous support side when one PAC has support and oppose groups", () => {
    const result = aggregateLouisianaOutsideGroupContributions({
      electionYear: 2027,
      outsideGroups: [outsideGroup({ supportOppose: "support" }), outsideGroup({ supportOppose: "oppose" })],
      contributionRows: [contribution()],
    });

    expect(result.matchedContributionRowCount).toBe(1);
    expect(result.includedContributionRowCount).toBe(0);
    expect(result.skippedContributionRowCount).toBe(1);
    expect(result.outsideGroupBreakdowns).toEqual([]);
    expect(result.classifications).toEqual([]);
  });

  it("skips contributions outside the election cycle", () => {
    const result = aggregateLouisianaOutsideGroupContributions({
      electionYear: 2027,
      outsideGroups: [outsideGroup()],
      contributionRows: [
        contribution({
          ContributionDate: "09/01/2025",
          ContributorName: "Google LLC",
          ContributorTypeCode: "BUS",
        }),
      ],
    });

    expect(result.matchedContributionRowCount).toBe(1);
    expect(result.includedContributionRowCount).toBe(0);
    expect(result.skippedContributionRowCount).toBe(1);
    expect(result.outsideGroupBreakdowns).toEqual([]);
    expect(result.classifications).toEqual([]);
  });

  it("keeps donor evidence for selected industries even when another donor is larger", () => {
    const result = aggregateLouisianaOutsideGroupContributions({
      electionYear: 2027,
      maxBreakdownsPerCategory: 1,
      outsideGroups: [outsideGroup()],
      contributionRows: [
        contribution({
          ContributionAmt: "50,000.00",
          ContributorName: "Generic Holdings LLC",
          ContributorTypeCode: "BUS",
        }),
        contribution({
          ContributionAmt: "10,000.00",
          ContributorName: "Google LLC",
          ContributorTypeCode: "BUS",
        }),
      ],
    });

    expect(result.outsideGroupBreakdowns).toEqual([
      expect.objectContaining({
        categoryType: "donor",
        categoryName: "Generic Holdings LLC",
        amount: 50000,
      }),
      expect.objectContaining({
        categoryType: "donor",
        categoryName: "Google LLC",
        amount: 10000,
      }),
      expect.objectContaining({
        categoryType: "industry",
        categoryName: "technology",
        amount: 10000,
      }),
    ]);
  });

  it("validates inputs", () => {
    expect(() =>
      aggregateLouisianaOutsideGroupContributions({ electionYear: 1999, outsideGroups: [], contributionRows: [] })
    ).toThrow("Invalid Louisiana outside group contribution election year");
    expect(() =>
      aggregateLouisianaOutsideGroupContributions({
        electionYear: 2027,
        outsideGroups: [],
        contributionRows: [],
        maxBreakdownsPerCategory: 0,
      })
    ).toThrow("maxBreakdownsPerCategory");
  });
});
