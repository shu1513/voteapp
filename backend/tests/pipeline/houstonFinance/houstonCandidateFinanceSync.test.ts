import { describe, expect, it } from "vitest";
import {
  isHoustonIndustryEligibleOrganization,
  isHoustonIndustryEligibleOrganizationName,
  mergeHoustonOutsideIndustryBreakdowns,
} from "../../../src/pipeline/houstonFinance/houstonCandidateFinanceSync.js";

describe("Houston finance outside-industry donor safety", () => {
  it("keeps businesses and excludes political committees", () => {
    expect(isHoustonIndustryEligibleOrganizationName("Friedkin Group LLC")).toBe(true);
    expect(isHoustonIndustryEligibleOrganizationName("Petroplex Energy")).toBe(true);
    expect(isHoustonIndustryEligibleOrganizationName("Dade Phelan Campaign")).toBe(false);
    expect(isHoustonIndustryEligibleOrganizationName("Texas Realtors PAC")).toBe(false);
    expect(isHoustonIndustryEligibleOrganizationName("Citizens for Jane Doe")).toBe(false);
  });

  it("excludes names present in the official TEC filer registry", () => {
    expect(isHoustonIndustryEligibleOrganization("Koch for Dallas", new Set(["KOCH FOR DALLAS"]))).toBe(false);
    expect(isHoustonIndustryEligibleOrganization("The Friedkin Group", new Set(["KOCH FOR DALLAS"]))).toBe(true);
  });

  it("sums donors classified into the same industry before the snapshot upsert", () => {
    expect(mergeHoustonOutsideIndustryBreakdowns([
      {
        committeeId: "00063767",
        supportOppose: "support",
        categoryType: "industry",
        categoryName: "finance_investment",
        amount: 20_000,
        contributorCount: 1,
        sourceUrl: "https://example.test/tec.zip",
      },
      {
        committeeId: "00063767",
        supportOppose: "support",
        categoryType: "industry",
        categoryName: "finance_investment",
        amount: 7_000,
        contributorCount: 1,
        sourceUrl: "https://example.test/tec.zip",
      },
      {
        committeeId: "00085365",
        supportOppose: "support",
        categoryType: "industry",
        categoryName: "finance_investment",
        amount: 3_000,
        contributorCount: 1,
        sourceUrl: "https://example.test/tec.zip",
      },
    ])).toEqual([
      {
        committeeId: "00063767",
        supportOppose: "support",
        categoryType: "industry",
        categoryName: "finance_investment",
        amount: 27_000,
        contributorCount: 2,
        sourceUrl: "https://example.test/tec.zip",
      },
      {
        committeeId: "00085365",
        supportOppose: "support",
        categoryType: "industry",
        categoryName: "finance_investment",
        amount: 3_000,
        contributorCount: 1,
        sourceUrl: "https://example.test/tec.zip",
      },
    ]);
  });

  it("sums known contributor counts regardless of null ordering", () => {
    const breakdown = (contributorCount: number | null) => ({
      committeeId: "00063767",
      supportOppose: "support" as const,
      categoryType: "industry" as const,
      categoryName: "finance_investment",
      amount: 1_000,
      contributorCount,
      sourceUrl: "https://example.test/tec.zip",
    });

    expect(mergeHoustonOutsideIndustryBreakdowns([breakdown(null), breakdown(1)])[0]?.contributorCount).toBe(1);
    expect(mergeHoustonOutsideIndustryBreakdowns([breakdown(1), breakdown(null)])[0]?.contributorCount).toBe(1);
    expect(mergeHoustonOutsideIndustryBreakdowns([breakdown(null), breakdown(null)])[0]?.contributorCount).toBeNull();
  });
});
