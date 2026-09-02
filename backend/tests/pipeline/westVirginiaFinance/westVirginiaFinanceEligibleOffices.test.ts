import { describe, expect, it } from "vitest";

import {
  WEST_VIRGINIA_FINANCE_ELIGIBLE_OFFICE_KEYS,
  isWestVirginiaFinanceEligibleOffice,
  westVirginiaDistrictNumberFromDistrictName,
  westVirginiaRegistryOfficeForRace,
} from "../../../src/pipeline/westVirginiaFinance/westVirginiaFinanceEligibleOffices.js";

describe("West Virginia eligible offices", () => {
  it("covers exactly the two legislative chambers", () => {
    expect([...WEST_VIRGINIA_FINANCE_ELIGIBLE_OFFICE_KEYS].sort()).toEqual([
      "state_lower::State Lower Chamber Legislator",
      "state_upper::State Senator",
    ]);
    expect(isWestVirginiaFinanceEligibleOffice({ officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator" })).toBe(true);
    expect(isWestVirginiaFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "Governor" })).toBe(false);
    expect(isWestVirginiaFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "State Level Judge" })).toBe(false);
    expect(isWestVirginiaFinanceEligibleOffice({ officeScope: null, officeCanonicalName: "State Senator" })).toBe(false);
  });

  it("maps races to the registry office labels pinned live", () => {
    expect(westVirginiaRegistryOfficeForRace({ officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator" })).toBe("House of Delegates");
    expect(westVirginiaRegistryOfficeForRace({ officeScope: "state_upper", officeCanonicalName: "State Senator" })).toBe("State Senator");
    expect(westVirginiaRegistryOfficeForRace({ officeScope: "county", officeCanonicalName: "Sheriff" })).toBeNull();
  });

  it("parses seat numbers from VoteApp district names", () => {
    expect(westVirginiaDistrictNumberFromDistrictName("Delegate District 12 (2024); West Virginia")).toBe(12);
    expect(westVirginiaDistrictNumberFromDistrictName("State Senate District 3 (2024); West Virginia")).toBe(3);
    expect(westVirginiaDistrictNumberFromDistrictName("West Virginia")).toBeNull();
    expect(westVirginiaDistrictNumberFromDistrictName(null)).toBeNull();
  });
});
