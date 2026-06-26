import { describe, expect, it } from "vitest";

import {
  aggregateMinnesotaOutsideGroupContributions,
} from "../../../src/pipeline/minnesotaFinance/minnesotaOutsideGroupContributionAggregator.js";
import type { MinnesotaCampaignFinanceCsvRow } from "../../../src/pipeline/minnesotaFinance/minnesotaCampaignFinanceArtifactReader.js";

function contributionRow(overrides: Partial<MinnesotaCampaignFinanceCsvRow> = {}): MinnesotaCampaignFinanceCsvRow {
  return {
    "Recipient reg num": "SP123",
    Recipient: "Better Minnesota",
    Amount: "100.00",
    Contributor: "Google LLC",
    "Contrib type": "Business",
    "Receipt date": "2026-09-01",
    Year: "2026",
    ...overrides,
  };
}

describe("Minnesota outside group contribution aggregator", () => {
  it("aggregates organization donors and classifies industries while skipping individual donors", () => {
    const result = aggregateMinnesotaOutsideGroupContributions({
      electionYear: 2026,
      sourceUrl: "https://example.test/minnesota",
      outsideGroups: [
        {
          committeeId: "SP123",
          committeeName: "Better Minnesota",
          supportOppose: "support",
          amount: 70_000,
          sourceUrl: "https://example.test/outside/support",
        },
        {
          committeeId: "SP999",
          committeeName: "Wrong Way PAC",
          supportOppose: "oppose",
          amount: 10_000,
          sourceUrl: "https://example.test/outside/oppose",
        },
      ],
      contributionRows: [
        contributionRow({
          Contributor: "Google LLC",
          "Contrib type": "Business",
          Amount: "100.00",
        }),
        contributionRow({
          Contributor: "AFL CIO",
          "Contrib type": "Committee",
          Amount: "50.00",
          "Contrib reg num": "CIO-1",
        }),
        contributionRow({
          "Recipient reg num": "SP999",
          Recipient: "Wrong Way PAC",
          Contributor: "Microsoft",
          "Contrib type": "PAC",
          Amount: "25.00",
          Year: "2026",
        }),
        contributionRow({
          Contributor: "Jane Doe",
          "Contrib type": "Individual",
          Amount: "999.00",
          "Contrib Employer name": "Google",
        }),
      ],
    });

    expect(result.matchedContributionRowCount).toBe(4);
    expect(result.includedContributionRowCount).toBe(3);
    expect(result.skippedContributionRowCount).toBe(1);
    expect(result.outsideGroupBreakdowns).toEqual([
      expect.objectContaining({
        committeeId: "SP123",
        supportOppose: "support",
        categoryType: "donor",
        categoryName: "Google LLC",
        amount: 100,
        contributorCount: 1,
        sourceUrl: "https://example.test/minnesota",
      }),
      expect.objectContaining({
        committeeId: "SP123",
        supportOppose: "support",
        categoryType: "donor",
        categoryName: "AFL CIO",
        amount: 50,
        contributorCount: 1,
        sourceUrl: "https://example.test/minnesota",
      }),
      expect.objectContaining({
        committeeId: "SP123",
        supportOppose: "support",
        categoryType: "industry",
        categoryName: "technology",
        amount: 100,
        contributorCount: 1,
        sourceUrl: "https://example.test/minnesota",
      }),
      expect.objectContaining({
        committeeId: "SP123",
        supportOppose: "support",
        categoryType: "industry",
        categoryName: "labor_unions",
        amount: 50,
        contributorCount: 1,
        sourceUrl: "https://example.test/minnesota",
      }),
      expect.objectContaining({
        committeeId: "SP999",
        supportOppose: "oppose",
        categoryType: "donor",
        categoryName: "Microsoft",
        amount: 25,
        contributorCount: 1,
        sourceUrl: "https://example.test/minnesota",
      }),
      expect.objectContaining({
        committeeId: "SP999",
        supportOppose: "oppose",
        categoryType: "industry",
        categoryName: "technology",
        amount: 25,
        contributorCount: 1,
        sourceUrl: "https://example.test/minnesota",
      }),
    ]);
  });

  it("skips contributions outside the election cycle", () => {
    const result = aggregateMinnesotaOutsideGroupContributions({
      electionYear: 2026,
      outsideGroups: [
        {
          committeeId: "SP123",
          committeeName: "Better Minnesota",
          supportOppose: "support",
          amount: 70_000,
        },
      ],
      contributionRows: [
        contributionRow({
          Year: "2024",
          Contributor: "Google LLC",
          "Contrib type": "Business",
        }),
      ],
    });

    expect(result.matchedContributionRowCount).toBe(1);
    expect(result.includedContributionRowCount).toBe(0);
    expect(result.skippedContributionRowCount).toBe(1);
    expect(result.outsideGroupBreakdowns).toEqual([]);
  });
});
