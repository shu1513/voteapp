import { describe, expect, it } from "vitest";

import {
  isPennsylvaniaFinanceEligibleOffice,
  mapPennsylvaniaFinanceOffice,
  toPennsylvaniaFinanceOfficeSearchInput,
} from "../../../src/pipeline/pennsylvaniaFinance/pennsylvaniaFinanceEligibleOffices.js";

describe("pennsylvaniaFinanceEligibleOffices", () => {
  it("maps PA source offices to supported app offices", () => {
    expect(mapPennsylvaniaFinanceOffice({ office: "GOV", district: "" })).toMatchObject({
      officeScope: "statewide",
      officeCanonicalName: "Governor",
      paOfficeCode: "GOV",
      district: null,
    });
    expect(mapPennsylvaniaFinanceOffice({ office: "STS", district: "SD 7" })).toMatchObject({
      officeScope: "state_upper",
      officeCanonicalName: "State Senator",
      paOfficeCode: "STS",
      district: "7",
    });
    expect(mapPennsylvaniaFinanceOffice({ office: "STH", district: "203" })).toMatchObject({
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
      paOfficeCode: "STH",
      district: "203",
    });
  });

  it("rejects invalid legislative districts and unsupported offices", () => {
    expect(mapPennsylvaniaFinanceOffice({ office: "STS", district: "51" })).toBeNull();
    expect(mapPennsylvaniaFinanceOffice({ office: "MAYOR", district: "" })).toBeNull();
    expect(
      toPennsylvaniaFinanceOfficeSearchInput({
        officeScope: "state_lower",
        officeCanonicalName: "State Lower Chamber Legislator",
      })
    ).toBeNull();
  });

  it("recognizes PA finance-eligible app offices", () => {
    expect(
      isPennsylvaniaFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "Attorney General",
      })
    ).toBe(true);
    expect(
      isPennsylvaniaFinanceEligibleOffice({
        officeScope: "local",
        officeCanonicalName: "Mayor",
      })
    ).toBe(false);
  });
});
