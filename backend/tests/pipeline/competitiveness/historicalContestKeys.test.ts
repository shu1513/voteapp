import { describe, expect, it } from "vitest";

import {
  buildHistoricalContestLookupKey,
  expectedDistrictTypeForHistoricalOffice,
  fromMitDistrict,
  mapHistoricalOfficeTypeToMitOffice,
  mapOfficeCanonicalNameToHistoricalOfficeType,
  toMitDistrict,
} from "../../../src/pipeline/competitiveness/historicalContestKeys.js";

describe("historicalContestKeys", () => {
  it("maps supported canonical offices to historical office types", () => {
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("President of the United States")).toBe("US_PRESIDENT");
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("United States Senator")).toBe("US_SENATE");
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("United States Representative")).toBe("US_HOUSE");
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("Governor")).toBe("GOVERNOR");
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("Lieutenant Governor")).toBe("LIEUTENANT_GOVERNOR");
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("Secretary of State")).toBe("SECRETARY_OF_STATE");
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("Attorney General")).toBe("ATTORNEY_GENERAL");
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("State Treasurer")).toBe("STATE_TREASURER");
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("State Auditor")).toBe("STATE_AUDITOR");
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("Comptroller")).toBe("COMPTROLLER");
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("Superintendent of Public Instruction")).toBe(
      "SUPERINTENDENT_OF_PUBLIC_INSTRUCTION"
    );
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("Commissioner of Agriculture")).toBe(
      "COMMISSIONER_OF_AGRICULTURE"
    );
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("Commissioner of Insurance")).toBe(
      "COMMISSIONER_OF_INSURANCE"
    );
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("Labor Commissioner")).toBe("LABOR_COMMISSIONER");
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("Land Commissioner")).toBe("LAND_COMMISSIONER");
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("State Senator")).toBe("STATE_SENATE");
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("State Lower Chamber Legislator")).toBe("STATE_HOUSE");
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("Sheriff")).toBe("COUNTY_SHERIFF");
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("District Attorney")).toBe("DISTRICT_ATTORNEY");
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("County Clerk")).toBe("COUNTY_CLERK");
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("County Assessor")).toBe("COUNTY_ASSESSOR");
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("County Auditor")).toBe("COUNTY_AUDITOR");
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("County Treasurer")).toBe("COUNTY_TREASURER");
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("County Recorder")).toBe("COUNTY_RECORDER");
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("County Coroner")).toBe("COUNTY_CORONER");
  });

  it("returns null for unsupported offices and blank office names", () => {
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("County Commissioner")).toBeNull();
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("County Supervisor")).toBeNull();
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("Mayor")).toBeNull();
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("School Board Member")).toBeNull();
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("Corporation Commissioner")).toBeNull();
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("Public Service Commissioner")).toBeNull();
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("")).toBeNull();
    expect(mapOfficeCanonicalNameToHistoricalOfficeType(null)).toBeNull();
  });

  it("maps historical office types to MIT office labels and expected district types", () => {
    expect(mapHistoricalOfficeTypeToMitOffice("US_HOUSE")).toBe("US HOUSE");
    expect(mapHistoricalOfficeTypeToMitOffice("STATE_HOUSE")).toBe("STATE HOUSE");
    expect(mapHistoricalOfficeTypeToMitOffice("LIEUTENANT_GOVERNOR")).toBe("LIEUTENANT GOVERNOR");
    expect(mapHistoricalOfficeTypeToMitOffice("SECRETARY_OF_STATE")).toBe("SECRETARY OF STATE");
    expect(mapHistoricalOfficeTypeToMitOffice("ATTORNEY_GENERAL")).toBe("ATTORNEY GENERAL");
    expect(mapHistoricalOfficeTypeToMitOffice("STATE_TREASURER")).toBe("STATE TREASURER");
    expect(mapHistoricalOfficeTypeToMitOffice("STATE_AUDITOR")).toBe("STATE AUDITOR");
    expect(mapHistoricalOfficeTypeToMitOffice("COMPTROLLER")).toBe("STATE CONTROLLER");
    expect(mapHistoricalOfficeTypeToMitOffice("SUPERINTENDENT_OF_PUBLIC_INSTRUCTION")).toBe(
      "SUPERINTENDENT OF PUBLIC INSTRUCTION"
    );
    expect(mapHistoricalOfficeTypeToMitOffice("COMMISSIONER_OF_AGRICULTURE")).toBe("COMMISSIONER OF AGRICULTURE");
    expect(mapHistoricalOfficeTypeToMitOffice("COMMISSIONER_OF_INSURANCE")).toBe("COMMISSIONER OF INSURANCE");
    expect(mapHistoricalOfficeTypeToMitOffice("LABOR_COMMISSIONER")).toBe("LABOR COMMISSIONER");
    expect(mapHistoricalOfficeTypeToMitOffice("LAND_COMMISSIONER")).toBe("LAND COMMISSIONER");
    expect(mapHistoricalOfficeTypeToMitOffice("COUNTY_SHERIFF")).toBe("COUNTY SHERIFF");
    expect(mapHistoricalOfficeTypeToMitOffice("COUNTY_AUDITOR")).toBe("COUNTY AUDITOR");
    expect(mapHistoricalOfficeTypeToMitOffice("DISTRICT_ATTORNEY")).toBe("DISTRICT ATTORNEY");
    expect(expectedDistrictTypeForHistoricalOffice("US_PRESIDENT")).toBe("statewide");
    expect(expectedDistrictTypeForHistoricalOffice("US_HOUSE")).toBe("us_house");
    expect(expectedDistrictTypeForHistoricalOffice("ATTORNEY_GENERAL")).toBe("statewide");
    expect(expectedDistrictTypeForHistoricalOffice("STATE_TREASURER")).toBe("statewide");
    expect(expectedDistrictTypeForHistoricalOffice("SUPERINTENDENT_OF_PUBLIC_INSTRUCTION")).toBe("statewide");
    expect(expectedDistrictTypeForHistoricalOffice("LABOR_COMMISSIONER")).toBe("statewide");
    expect(expectedDistrictTypeForHistoricalOffice("LAND_COMMISSIONER")).toBe("statewide");
    expect(expectedDistrictTypeForHistoricalOffice("STATE_SENATE")).toBe("state_upper");
    expect(expectedDistrictTypeForHistoricalOffice("STATE_HOUSE")).toBe("state_lower");
    expect(expectedDistrictTypeForHistoricalOffice("COUNTY_SHERIFF")).toBe("county");
    expect(expectedDistrictTypeForHistoricalOffice("COUNTY_RECORDER")).toBe("county");
  });

  it("converts app district GEOIDs to MIT district keys", () => {
    expect(toMitDistrict({ districtType: "statewide", geoidCompact: "06", stateFips: "06" })).toBe("STATEWIDE");
    expect(toMitDistrict({ districtType: "county", geoidCompact: "06037", stateFips: "06" })).toBe("06037");
    expect(toMitDistrict({ districtType: "us_house", geoidCompact: "0631", stateFips: "06" })).toBe("031");
    expect(toMitDistrict({ districtType: "state_upper", geoidCompact: "06022", stateFips: "06" })).toBe("022");
    expect(toMitDistrict({ districtType: "state_lower", geoidCompact: "06048", stateFips: "06" })).toBe("048");
    expect(toMitDistrict({ districtType: "us_house", geoidCompact: "0601", stateFips: "6" })).toBe("001");
  });

  it("returns null when app district GEOIDs cannot map to MIT district keys", () => {
    expect(toMitDistrict({ districtType: "statewide", geoidCompact: "06037", stateFips: "06" })).toBeNull();
    expect(toMitDistrict({ districtType: "county", geoidCompact: "0637", stateFips: "06" })).toBeNull();
    expect(toMitDistrict({ districtType: "county", geoidCompact: "12037", stateFips: "06" })).toBeNull();
    expect(toMitDistrict({ districtType: "us_house", geoidCompact: "1231", stateFips: "06" })).toBeNull();
    expect(toMitDistrict({ districtType: "state_upper", geoidCompact: "06A01", stateFips: "06" })).toBeNull();
    expect(toMitDistrict({ districtType: "us_house", geoidCompact: "0A31", stateFips: "A" })).toBeNull();
  });

  it("converts MIT district keys back to app district GEOIDs", () => {
    expect(fromMitDistrict({ districtType: "statewide", mitDistrict: "STATEWIDE", stateFips: "06" })).toBe("06");
    expect(fromMitDistrict({ districtType: "county", mitDistrict: "06037", stateFips: "06" })).toBe("06037");
    expect(fromMitDistrict({ districtType: "us_house", mitDistrict: "031", stateFips: "06" })).toBe("0631");
    expect(fromMitDistrict({ districtType: "state_upper", mitDistrict: "22", stateFips: "06" })).toBe("06022");
    expect(fromMitDistrict({ districtType: "state_lower", mitDistrict: "048", stateFips: "6" })).toBe("06048");
  });

  it("returns null when MIT district keys cannot map back to app district GEOIDs", () => {
    expect(fromMitDistrict({ districtType: "statewide", mitDistrict: "001", stateFips: "06" })).toBeNull();
    expect(fromMitDistrict({ districtType: "county", mitDistrict: "037", stateFips: "06" })).toBeNull();
    expect(fromMitDistrict({ districtType: "county", mitDistrict: "12037", stateFips: "06" })).toBeNull();
    expect(fromMitDistrict({ districtType: "us_house", mitDistrict: "AT-LARGE", stateFips: "06" })).toBeNull();
    expect(fromMitDistrict({ districtType: "us_house", mitDistrict: "031", stateFips: "A" })).toBeNull();
  });

  it("builds lookup keys for supported current election summaries", () => {
    expect(
      buildHistoricalContestLookupKey({
        officeCanonicalName: "United States Representative",
        districtType: "us_house",
        geoidCompact: "0631",
        stateFips: "06",
      })
    ).toEqual({
      state: "CA",
      state_fips: "06",
      office_type: "US_HOUSE",
      district_type: "us_house",
      district_key: "0631",
      mit_office: "US HOUSE",
      mit_district: "031",
    });

    expect(
      buildHistoricalContestLookupKey({
        officeCanonicalName: "Governor",
        districtType: "statewide",
        geoidCompact: "06",
        stateFips: "06",
      })
    ).toMatchObject({
      state: "CA",
      office_type: "GOVERNOR",
      district_type: "statewide",
      district_key: "06",
      mit_office: "GOVERNOR",
      mit_district: "STATEWIDE",
    });

    expect(
      buildHistoricalContestLookupKey({
        officeCanonicalName: "Attorney General",
        districtType: "statewide",
        geoidCompact: "06",
        stateFips: "06",
      })
    ).toMatchObject({
      state: "CA",
      office_type: "ATTORNEY_GENERAL",
      district_type: "statewide",
      district_key: "06",
      mit_office: "ATTORNEY GENERAL",
      mit_district: "STATEWIDE",
    });

    expect(
      buildHistoricalContestLookupKey({
        officeCanonicalName: "State Treasurer",
        districtType: "statewide",
        geoidCompact: "06",
        stateFips: "06",
      })
    ).toMatchObject({
      office_type: "STATE_TREASURER",
      district_type: "statewide",
      mit_office: "STATE TREASURER",
    });

    expect(
      buildHistoricalContestLookupKey({
        officeCanonicalName: "Land Commissioner",
        districtType: "statewide",
        geoidCompact: "06",
        stateFips: "06",
      })
    ).toMatchObject({
      office_type: "LAND_COMMISSIONER",
      district_type: "statewide",
      mit_office: "LAND COMMISSIONER",
      mit_district: "STATEWIDE",
    });

    expect(
      buildHistoricalContestLookupKey({
        officeCanonicalName: "Sheriff",
        districtType: "county",
        geoidCompact: "06037",
        stateFips: "06",
      })
    ).toMatchObject({
      state: "CA",
      office_type: "COUNTY_SHERIFF",
      district_type: "county",
      district_key: "06037",
      mit_office: "COUNTY SHERIFF",
      mit_district: "06037",
    });
  });

  it("returns null for unsupported or mismatched lookup inputs", () => {
    expect(
      buildHistoricalContestLookupKey({
        officeCanonicalName: "Mayor",
        districtType: "place",
        geoidCompact: "06037",
        stateFips: "06",
      })
    ).toBeNull();

    expect(
      buildHistoricalContestLookupKey({
        officeCanonicalName: "United States Senator",
        districtType: "us_house",
        geoidCompact: "0631",
        stateFips: "06",
      })
    ).toBeNull();

    expect(
      buildHistoricalContestLookupKey({
        officeCanonicalName: "Attorney General",
        districtType: "county",
        geoidCompact: "06037",
        stateFips: "06",
      })
    ).toBeNull();

    expect(
      buildHistoricalContestLookupKey({
        officeCanonicalName: "United States Representative",
        districtType: "us_house",
        geoidCompact: "0A31",
        stateFips: "A",
      })
    ).toBeNull();
  });
});
