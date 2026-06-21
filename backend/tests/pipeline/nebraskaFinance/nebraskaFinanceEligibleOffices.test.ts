import { describe, expect, it } from "vitest";

import {
  isNebraskaFinanceEligibleOffice,
  mapNebraskaNadcJurisdictionOffice,
  normalizeNebraskaNadcOfficeLabel,
} from "../../../src/pipeline/nebraskaFinance/nebraskaFinanceEligibleOffices.js";

describe("nebraskaFinanceEligibleOffices", () => {
  it("allows only the explicit Nebraska NADC-safe office set", () => {
    expect(
      isNebraskaFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "Governor",
      })
    ).toBe(true);
    expect(
      isNebraskaFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "State Auditor",
      })
    ).toBe(true);
    expect(
      isNebraskaFinanceEligibleOffice({
        officeScope: "state_upper",
        officeCanonicalName: "State Senator",
      })
    ).toBe(true);

    expect(
      isNebraskaFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "United States Senator",
      })
    ).toBe(false);
    expect(
      isNebraskaFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "Public Service Commissioner",
      })
    ).toBe(false);
    expect(
      isNebraskaFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "State Board of Education Member",
      })
    ).toBe(false);
    expect(
      isNebraskaFinanceEligibleOffice({
        officeScope: "state_lower",
        officeCanonicalName: "State Lower Chamber Legislator",
      })
    ).toBe(false);
    expect(
      isNebraskaFinanceEligibleOffice({
        officeScope: "county",
        officeCanonicalName: "Sheriff",
      })
    ).toBe(false);
  });

  it("normalizes Nebraska NADC office labels conservatively", () => {
    expect(normalizeNebraskaNadcOfficeLabel(" Nebraska  -  State   Legislature  -  20 ")).toBe(
      "NEBRASKA - STATE LEGISLATURE - 20"
    );
    expect(normalizeNebraskaNadcOfficeLabel("\tNebraska - auditor of public accounts\n")).toBe(
      "NEBRASKA - AUDITOR OF PUBLIC ACCOUNTS"
    );
    expect(normalizeNebraskaNadcOfficeLabel("   ")).toBeNull();
    expect(normalizeNebraskaNadcOfficeLabel(null)).toBeNull();
  });

  it("maps safe statewide NADC jurisdiction labels to canonical app offices", () => {
    expect(mapNebraskaNadcJurisdictionOffice({ jurisdictionOfficeDistrict: "NEBRASKA - GOVERNOR" })).toEqual({
      officeScope: "statewide",
      officeCanonicalName: "Governor",
      officeKey: "statewide::Governor",
      requiresDistrict: false,
      district: null,
    });
    expect(mapNebraskaNadcJurisdictionOffice({ jurisdictionOfficeDistrict: "NEBRASKA - SECRETARY OF STATE" })).toEqual({
      officeScope: "statewide",
      officeCanonicalName: "Secretary of State",
      officeKey: "statewide::Secretary of State",
      requiresDistrict: false,
      district: null,
    });
    expect(mapNebraskaNadcJurisdictionOffice({ jurisdictionOfficeDistrict: "NEBRASKA - ATTORNEY GENERAL" })).toEqual({
      officeScope: "statewide",
      officeCanonicalName: "Attorney General",
      officeKey: "statewide::Attorney General",
      requiresDistrict: false,
      district: null,
    });
    expect(mapNebraskaNadcJurisdictionOffice({ jurisdictionOfficeDistrict: "NEBRASKA - STATE TREASURER" })).toEqual({
      officeScope: "statewide",
      officeCanonicalName: "State Treasurer",
      officeKey: "statewide::State Treasurer",
      requiresDistrict: false,
      district: null,
    });
    expect(mapNebraskaNadcJurisdictionOffice({ jurisdictionOfficeDistrict: "NEBRASKA - AUDITOR OF PUBLIC ACCOUNTS" })).toEqual(
      {
        officeScope: "statewide",
        officeCanonicalName: "State Auditor",
        officeKey: "statewide::State Auditor",
        requiresDistrict: false,
        district: null,
      }
    );
  });

  it("maps Nebraska unicameral legislative labels to state_upper State Senator with district", () => {
    expect(mapNebraskaNadcJurisdictionOffice({ jurisdictionOfficeDistrict: "NEBRASKA - STATE LEGISLATURE - 20" })).toEqual({
      officeScope: "state_upper",
      officeCanonicalName: "State Senator",
      officeKey: "state_upper::State Senator",
      requiresDistrict: true,
      district: "20",
    });
    expect(mapNebraskaNadcJurisdictionOffice({ jurisdictionOfficeDistrict: "NEBRASKA - STATE LEGISLATURE - 004" })).toEqual(
      expect.objectContaining({ district: "4" })
    );

    expect(mapNebraskaNadcJurisdictionOffice({ jurisdictionOfficeDistrict: "NEBRASKA - STATE LEGISLATURE" })).toBeNull();
    expect(mapNebraskaNadcJurisdictionOffice({ jurisdictionOfficeDistrict: "NEBRASKA - STATE LEGISLATURE - AT LARGE" })).toBeNull();
  });

  it("rejects unsafe or local NADC jurisdiction labels", () => {
    expect(mapNebraskaNadcJurisdictionOffice({ jurisdictionOfficeDistrict: "NEBRASKA - PUBLIC SERVICE COMMISSION - 1" })).toBeNull();
    expect(mapNebraskaNadcJurisdictionOffice({ jurisdictionOfficeDistrict: "NEBRASKA - STATE BOARD OF EDUCATION - 5" })).toBeNull();
    expect(mapNebraskaNadcJurisdictionOffice({ jurisdictionOfficeDistrict: "NEBRASKA - BOARD OF REGENTS - University of Nebraska - 4" })).toBeNull();
    expect(mapNebraskaNadcJurisdictionOffice({ jurisdictionOfficeDistrict: "OMAHA - MAYOR" })).toBeNull();
    expect(mapNebraskaNadcJurisdictionOffice({ jurisdictionOfficeDistrict: "BUFFALO - COUNTY COMMISSIONER - 5" })).toBeNull();
    expect(mapNebraskaNadcJurisdictionOffice({ jurisdictionOfficeDistrict: "NEBRASKA - GOVERNOR - 1" })).toBeNull();
    expect(mapNebraskaNadcJurisdictionOffice({ jurisdictionOfficeDistrict: "" })).toBeNull();
  });
});
