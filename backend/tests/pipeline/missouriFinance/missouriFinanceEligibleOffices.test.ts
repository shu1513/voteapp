import { describe, expect, it } from "vitest";

import {
  MISSOURI_DIRECT_FINANCE_ELIGIBLE_OFFICE_KEYS,
  MISSOURI_FINANCE_ELIGIBLE_OFFICE_KEYS,
  isMissouriDirectFinanceEligibleOffice,
  normalizeMissouriMecJurisdiction,
  normalizeMissouriMecPoliticalDistrict,
  toMissouriMecOfficeSearchInput,
} from "../../../src/pipeline/missouriFinance/missouriFinanceEligibleOffices.js";

describe("missouriFinanceEligibleOffices", () => {
  it("maps legislative offices to MEC office, district, and history vocabulary", () => {
    expect(
      toMissouriMecOfficeSearchInput({
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        ballotTitle: "State Representative",
        legislativeDistrict: "001",
      })
    ).toEqual({
      politicalOffice: "State Representative",
      requiresSubdivision: false,
      politicalDistrict: "DISTRICT 1",
      historySubdivision: "Missouri House of Representatives",
    });
    expect(
      toMissouriMecOfficeSearchInput({
        officeScope: "state_upper",
        officeName: "State Senator",
        ballotTitle: "State Senator",
        legislativeDistrict: "34",
      })
    ).toMatchObject({
      politicalOffice: "State Senator",
      politicalDistrict: "DISTRICT 34",
      historySubdivision: "Missouri State Senate",
    });
  });

  it("uses ballot evidence to choose county source offices", () => {
    expect(
      toMissouriMecOfficeSearchInput({
        officeScope: "county",
        officeName: "County Commissioner",
        ballotTitle: "Presiding Commissioner",
      })
    ).toMatchObject({ politicalOffice: "Presiding Commissioner", requiresSubdivision: true });
    expect(
      toMissouriMecOfficeSearchInput({
        officeScope: "county",
        officeName: "County Supervisor",
        ballotTitle: "County Council - District 7",
      })
    ).toMatchObject({ politicalOffice: "County Council", politicalDistrict: "DISTRICT 7" });
    expect(
      toMissouriMecOfficeSearchInput({
        officeScope: "county",
        officeName: "District Attorney",
        ballotTitle: "Prosecuting Attorney",
      })
    ).toMatchObject({ politicalOffice: "Prosecuting Attorney" });
  });

  it("maps known municipal and school source labels without fuzzy office matching", () => {
    expect(
      toMissouriMecOfficeSearchInput({
        officeScope: "place",
        officeName: "City Council Member",
        ballotTitle: "Alderperson Ward 3",
      })
    ).toEqual({
      politicalOffice: "Alderperson",
      requiresSubdivision: true,
      politicalDistrict: "WARD 3",
      historySubdivision: null,
    });
    expect(
      toMissouriMecOfficeSearchInput({
        officeScope: "place",
        officeName: "Place Level Judge",
        ballotTitle: "Municipal Judge, Division 1",
      })
    ).toMatchObject({ politicalOffice: "Municipal Judge", requiresSubdivision: true });
    expect(
      toMissouriMecOfficeSearchInput({
        officeScope: "school_unified",
        officeName: "School Board Member",
        ballotTitle: "Member, St. Louis Board of Education",
      })
    ).toMatchObject({ politicalOffice: "Boardmember", requiresSubdivision: true });
  });

  it("normalizes source and roster jurisdiction/district spellings", () => {
    expect(normalizeMissouriMecJurisdiction("City of Jackson")).toBe("JACKSON CITY");
    expect(normalizeMissouriMecJurisdiction("Jackson city, Missouri")).toBe("JACKSON CITY");
    expect(normalizeMissouriMecJurisdiction("St. Louis County, Missouri")).toBe("ST LOUIS COUNTY");
    expect(normalizeMissouriMecJurisdiction("Lee's Summit city, Missouri")).toBe("LEES SUMMIT CITY");
    expect(normalizeMissouriMecJurisdiction("City of Lees Summit")).toBe("LEES SUMMIT CITY");
    expect(normalizeMissouriMecPoliticalDistrict("District No. 001")).toBe("DISTRICT 1");
    expect(normalizeMissouriMecPoliticalDistrict("Alderperson Ward 03")).toBe("WARD 3");
  });

  it("excludes unsupported/federal offices from automatic linking", () => {
    expect(
      toMissouriMecOfficeSearchInput({
        officeScope: "statewide",
        officeName: "United States Senator",
        ballotTitle: "United States Senator",
      })
    ).toBeNull();
    expect(MISSOURI_FINANCE_ELIGIBLE_OFFICE_KEYS.has("statewide::United States Senator")).toBe(false);
  });

  it("keeps no-primary local offices out of direct-finance v1", () => {
    expect(MISSOURI_DIRECT_FINANCE_ELIGIBLE_OFFICE_KEYS.has("state_lower::State Lower Chamber Legislator")).toBe(true);
    expect(MISSOURI_DIRECT_FINANCE_ELIGIBLE_OFFICE_KEYS.has("county::County Executive")).toBe(true);
    expect(MISSOURI_DIRECT_FINANCE_ELIGIBLE_OFFICE_KEYS.has("place::City Council Member")).toBe(false);
    expect(MISSOURI_DIRECT_FINANCE_ELIGIBLE_OFFICE_KEYS.has("school_unified::School Board Member")).toBe(false);
    expect(isMissouriDirectFinanceEligibleOffice({
      officeScope: "school_unified",
      officeCanonicalName: "School Board Member",
    })).toBe(false);
  });
});
