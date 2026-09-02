import { describe, expect, it } from "vitest";

import {
  NORTH_DAKOTA_FINANCE_ELIGIBLE_OFFICE_KEYS,
  isNorthDakotaFinanceEligibleOffice,
  northDakotaDistrictNumberFromDistrictName,
  northDakotaEligibleOfficeForRace,
  northDakotaRegistryDistrictLabel,
} from "../../../src/pipeline/northDakotaFinance/northDakotaFinanceEligibleOffices.js";

describe("North Dakota eligible offices", () => {
  it("covers the two chambers, six statewide executives and the Supreme Court", () => {
    expect([...NORTH_DAKOTA_FINANCE_ELIGIBLE_OFFICE_KEYS].sort()).toEqual([
      "state_lower::State Lower Chamber Legislator",
      "state_upper::State Senator",
      "statewide::Attorney General",
      "statewide::Commissioner of Agriculture",
      "statewide::Comptroller",
      "statewide::Public Service Commissioner",
      "statewide::Secretary of State",
      "statewide::State Level Judge",
      "statewide::Superintendent of Public Instruction",
    ]);
    expect(isNorthDakotaFinanceEligibleOffice({ officeScope: "state_upper", officeCanonicalName: "State Senator" })).toBe(true);
    expect(isNorthDakotaFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "Governor" })).toBe(false);
    expect(isNorthDakotaFinanceEligibleOffice({ officeScope: "county", officeCanonicalName: "Sheriff" })).toBe(false);
    expect(isNorthDakotaFinanceEligibleOffice({ officeScope: null, officeCanonicalName: "State Senator" })).toBe(false);
  });

  it("maps races to the registry office labels pinned live", () => {
    expect(northDakotaEligibleOfficeForRace({ officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator" })).toEqual({
      registryOffice: "State Representative",
      districted: true,
    });
    expect(northDakotaEligibleOfficeForRace({ officeScope: "statewide", officeCanonicalName: "Comptroller" })).toEqual({
      registryOffice: "Tax Commissioner",
      districted: false,
    });
    expect(northDakotaEligibleOfficeForRace({ officeScope: "statewide", officeCanonicalName: "State Level Judge" })).toEqual({
      registryOffice: "Supreme Court Justice",
      districted: false,
    });
    expect(northDakotaEligibleOfficeForRace({ officeScope: "statewide", officeCanonicalName: "Commissioner of Agriculture" })?.registryOffice).toBe(
      "Agriculture Commissioner"
    );
    expect(northDakotaEligibleOfficeForRace({ officeScope: "judicial", officeCanonicalName: "District Court Judge" })).toBeNull();
  });

  it("parses seat numbers from VoteApp district names and renders registry labels", () => {
    expect(northDakotaDistrictNumberFromDistrictName("State Senate District 11 (2024); North Dakota")).toBe(11);
    expect(northDakotaDistrictNumberFromDistrictName("State House District 4 (2024); North Dakota")).toBe(4);
    expect(northDakotaDistrictNumberFromDistrictName("North Dakota")).toBeNull();
    expect(northDakotaDistrictNumberFromDistrictName(null)).toBeNull();
    expect(northDakotaRegistryDistrictLabel(11)).toBe("District 11");
    expect(() => northDakotaRegistryDistrictLabel(0)).toThrow(/district number/);
  });
});
