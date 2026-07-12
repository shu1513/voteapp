import { describe, expect, it } from "vitest";
import { isHoustonIndustryEligibleOrganization, isHoustonIndustryEligibleOrganizationName } from "../../../src/pipeline/houstonFinance/houstonCandidateFinanceSync.js";

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
});
