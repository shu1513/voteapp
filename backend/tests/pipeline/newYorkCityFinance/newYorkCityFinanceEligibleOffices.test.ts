import { describe, expect, it } from "vitest";

import {
  NEW_YORK_CITY_FINANCE_ELIGIBLE_OFFICE_KEYS,
  toNewYorkCityCfbOfficeSearchInput,
} from "../../../src/pipeline/newYorkCityFinance/newYorkCityFinanceEligibleOffices.js";

describe("newYorkCityFinanceEligibleOffices", () => {
  it("maps only NYC citywide and borough-president districts", () => {
    expect(NEW_YORK_CITY_FINANCE_ELIGIBLE_OFFICE_KEYS.size).toBe(4);
    expect(toNewYorkCityCfbOfficeSearchInput({ officeScope: "place", officeCanonicalName: "Mayor", districtGeoid: "3651000" }))
      .toEqual({ officeCode: "1", boroughCode: null });
    expect(toNewYorkCityCfbOfficeSearchInput({ officeScope: "place", officeCanonicalName: "Public Advocate", districtGeoid: "3651000" }))
      .toEqual({ officeCode: "2", boroughCode: null });
    expect(toNewYorkCityCfbOfficeSearchInput({ officeScope: "place", officeCanonicalName: "Comptroller", districtGeoid: "3651000" }))
      .toEqual({ officeCode: "3", boroughCode: null });
    expect(toNewYorkCityCfbOfficeSearchInput({ officeScope: "county", officeCanonicalName: "Borough President", districtGeoid: "36047" }))
      .toEqual({ officeCode: "4", boroughCode: "K" });
  });

  it("rejects NYS comptroller, non-NYC mayor, and City Council", () => {
    expect(toNewYorkCityCfbOfficeSearchInput({ officeScope: "statewide", officeCanonicalName: "Comptroller", districtGeoid: "36" })).toBeNull();
    expect(toNewYorkCityCfbOfficeSearchInput({ officeScope: "place", officeCanonicalName: "Mayor", districtGeoid: "3611000" })).toBeNull();
    expect(toNewYorkCityCfbOfficeSearchInput({ officeScope: "place", officeCanonicalName: "City Council Member", districtGeoid: "3651000" })).toBeNull();
  });
});
