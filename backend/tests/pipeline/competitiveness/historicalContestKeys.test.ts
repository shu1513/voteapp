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
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("State Senator")).toBe("STATE_SENATE");
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("State Lower Chamber Legislator")).toBe("STATE_HOUSE");
  });

  it("returns null for unsupported offices and blank office names", () => {
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("Sheriff")).toBeNull();
    expect(mapOfficeCanonicalNameToHistoricalOfficeType("")).toBeNull();
    expect(mapOfficeCanonicalNameToHistoricalOfficeType(null)).toBeNull();
  });

  it("maps historical office types to MIT office labels and expected district types", () => {
    expect(mapHistoricalOfficeTypeToMitOffice("US_HOUSE")).toBe("US HOUSE");
    expect(mapHistoricalOfficeTypeToMitOffice("STATE_HOUSE")).toBe("STATE HOUSE");
    expect(expectedDistrictTypeForHistoricalOffice("US_PRESIDENT")).toBe("statewide");
    expect(expectedDistrictTypeForHistoricalOffice("US_HOUSE")).toBe("us_house");
    expect(expectedDistrictTypeForHistoricalOffice("STATE_SENATE")).toBe("state_upper");
    expect(expectedDistrictTypeForHistoricalOffice("STATE_HOUSE")).toBe("state_lower");
  });

  it("converts app district GEOIDs to MIT district keys", () => {
    expect(toMitDistrict({ districtType: "statewide", geoidCompact: "06", stateFips: "06" })).toBe("STATEWIDE");
    expect(toMitDistrict({ districtType: "us_house", geoidCompact: "0631", stateFips: "06" })).toBe("031");
    expect(toMitDistrict({ districtType: "state_upper", geoidCompact: "06022", stateFips: "06" })).toBe("022");
    expect(toMitDistrict({ districtType: "state_lower", geoidCompact: "06048", stateFips: "06" })).toBe("048");
    expect(toMitDistrict({ districtType: "us_house", geoidCompact: "0601", stateFips: "6" })).toBe("001");
  });

  it("returns null when app district GEOIDs cannot map to MIT district keys", () => {
    expect(toMitDistrict({ districtType: "statewide", geoidCompact: "06037", stateFips: "06" })).toBeNull();
    expect(toMitDistrict({ districtType: "county", geoidCompact: "06037", stateFips: "06" })).toBeNull();
    expect(toMitDistrict({ districtType: "us_house", geoidCompact: "1231", stateFips: "06" })).toBeNull();
    expect(toMitDistrict({ districtType: "state_upper", geoidCompact: "06A01", stateFips: "06" })).toBeNull();
  });

  it("converts MIT district keys back to app district GEOIDs", () => {
    expect(fromMitDistrict({ districtType: "statewide", mitDistrict: "STATEWIDE", stateFips: "06" })).toBe("06");
    expect(fromMitDistrict({ districtType: "us_house", mitDistrict: "031", stateFips: "06" })).toBe("0631");
    expect(fromMitDistrict({ districtType: "state_upper", mitDistrict: "22", stateFips: "06" })).toBe("06022");
    expect(fromMitDistrict({ districtType: "state_lower", mitDistrict: "048", stateFips: "6" })).toBe("06048");
  });

  it("returns null when MIT district keys cannot map back to app district GEOIDs", () => {
    expect(fromMitDistrict({ districtType: "statewide", mitDistrict: "001", stateFips: "06" })).toBeNull();
    expect(fromMitDistrict({ districtType: "us_house", mitDistrict: "AT-LARGE", stateFips: "06" })).toBeNull();
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
  });

  it("returns null for unsupported or mismatched lookup inputs", () => {
    expect(
      buildHistoricalContestLookupKey({
        officeCanonicalName: "Sheriff",
        districtType: "county",
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
  });
});
