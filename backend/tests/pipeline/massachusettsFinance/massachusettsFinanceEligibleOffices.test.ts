import { describe, expect, it } from "vitest";

import {
  MASSACHUSETTS_FINANCE_ELIGIBLE_OFFICE_KEYS,
  isMassachusettsFinanceEligibleOffice,
  mapMassachusettsOcpfOffice,
  normalizeMassachusettsOcpfDistrict,
  normalizeMassachusettsOcpfOfficeLabel,
  toMassachusettsFinanceOfficeKey,
  toMassachusettsOcpfOfficeSearchInput,
} from "../../../src/pipeline/massachusettsFinance/massachusettsFinanceEligibleOffices.js";

describe("massachusettsFinanceEligibleOffices", () => {
  it("keeps a conservative explicit Massachusetts finance office allowlist", () => {
    expect(MASSACHUSETTS_FINANCE_ELIGIBLE_OFFICE_KEYS).toEqual([
      "statewide::Governor",
      "statewide::Lieutenant Governor",
      "statewide::Secretary of State",
      "statewide::Attorney General",
      "statewide::State Treasurer",
      "statewide::State Auditor",
      "state_upper::State Senator",
      "state_lower::State Lower Chamber Legislator",
    ]);
  });

  it("allows every explicit eligible office and rejects broad-scope guesses", () => {
    for (const key of MASSACHUSETTS_FINANCE_ELIGIBLE_OFFICE_KEYS) {
      const [officeScope, officeCanonicalName] = key.split("::");
      expect(isMassachusettsFinanceEligibleOffice({ officeScope, officeCanonicalName })).toBe(true);
    }

    expect(
      isMassachusettsFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "Governor's Council Member",
      })
    ).toBe(false);
    expect(
      isMassachusettsFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "State Supreme Court Justice",
      })
    ).toBe(false);
    expect(
      isMassachusettsFinanceEligibleOffice({
        officeScope: "county",
        officeCanonicalName: "Sheriff",
      })
    ).toBe(false);
    expect(
      isMassachusettsFinanceEligibleOffice({
        officeScope: "state_lower",
        officeCanonicalName: "State Senator",
      })
    ).toBe(false);
  });

  it("builds trimmed office keys and rejects missing parts", () => {
    expect(
      toMassachusettsFinanceOfficeKey({
        officeScope: " statewide ",
        officeCanonicalName: " Governor ",
      })
    ).toBe("statewide::Governor");
    expect(toMassachusettsFinanceOfficeKey({ officeScope: "", officeCanonicalName: "Governor" })).toBeNull();
    expect(toMassachusettsFinanceOfficeKey({ officeScope: "statewide", officeCanonicalName: " " })).toBeNull();
  });

  it("normalizes OCPF office labels and districts conservatively", () => {
    expect(normalizeMassachusettsOcpfOfficeLabel(" Statewide,   Governor ")).toBe("STATEWIDE GOVERNOR");
    expect(normalizeMassachusettsOcpfOfficeLabel("Statewide, Lt. Governor.")).toBe("STATEWIDE LT GOVERNOR");
    expect(normalizeMassachusettsOcpfOfficeLabel("   ")).toBeNull();
    expect(normalizeMassachusettsOcpfDistrict(" 3rd   Suffolk District ")).toBe("3RD SUFFOLK");
    expect(normalizeMassachusettsOcpfDistrict("2nd Bristol & Plymouth")).toBe("2ND BRISTOL & PLYMOUTH");
    expect(normalizeMassachusettsOcpfDistrict("   ")).toBeNull();
  });

  it("maps safe OCPF statewide office labels to canonical app offices", () => {
    expect(mapMassachusettsOcpfOffice({ officeSought: "Statewide, Governor" })).toEqual({
      officeScope: "statewide",
      officeCanonicalName: "Governor",
      officeKey: "statewide::Governor",
      ocpfOffice: "Statewide, Governor",
      requiresDistrict: false,
      district: null,
    });
    expect(mapMassachusettsOcpfOffice({ officeSought: "Statewide, Secretary of Commonwealth" })).toMatchObject({
      officeScope: "statewide",
      officeCanonicalName: "Secretary of State",
      ocpfOffice: "Statewide, Secretary of State",
    });
    expect(mapMassachusettsOcpfOffice({ officeSought: "Statewide, Treasurer" })).toMatchObject({
      officeCanonicalName: "State Treasurer",
    });
    expect(mapMassachusettsOcpfOffice({ officeSought: "Statewide, Auditor" })).toMatchObject({
      officeCanonicalName: "State Auditor",
    });
    expect(mapMassachusettsOcpfOffice({ officeSought: "Lt. Governor" })).toMatchObject({
      officeCanonicalName: "Lieutenant Governor",
    });
  });

  it("maps legislative OCPF labels and requires district text", () => {
    expect(mapMassachusettsOcpfOffice({ officeSought: "Senate, 2nd Bristol & Plymouth" })).toEqual({
      officeScope: "state_upper",
      officeCanonicalName: "State Senator",
      officeKey: "state_upper::State Senator",
      ocpfOffice: "Senate",
      requiresDistrict: true,
      district: "2ND BRISTOL & PLYMOUTH",
    });
    expect(mapMassachusettsOcpfOffice({ officeSought: "House, 3rd Suffolk" })).toEqual({
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
      officeKey: "state_lower::State Lower Chamber Legislator",
      ocpfOffice: "House",
      requiresDistrict: true,
      district: "3RD SUFFOLK",
    });

    expect(mapMassachusettsOcpfOffice({ officeSought: "Senate" })).toBeNull();
    expect(mapMassachusettsOcpfOffice({ officeSought: "House,   " })).toBeNull();
  });

  it("maps app canonical offices to OCPF search inputs", () => {
    expect(
      toMassachusettsOcpfOfficeSearchInput({
        officeScope: "statewide",
        officeCanonicalName: "Attorney General",
      })
    ).toEqual({ ocpfOffice: "Statewide, Attorney General", district: null });
    expect(
      toMassachusettsOcpfOfficeSearchInput({
        officeScope: "statewide",
        officeCanonicalName: "Secretary of State",
      })
    ).toEqual({ ocpfOffice: "Statewide, Secretary of State", district: null });
    expect(
      toMassachusettsOcpfOfficeSearchInput({
        officeScope: "state_upper",
        officeCanonicalName: "State Senator",
        district: "2nd Bristol & Plymouth District",
      })
    ).toEqual({ ocpfOffice: "Senate", district: "2ND BRISTOL & PLYMOUTH" });
    expect(
      toMassachusettsOcpfOfficeSearchInput({
        officeScope: "state_lower",
        officeCanonicalName: "State Lower Chamber Legislator",
        district: "3rd Suffolk",
      })
    ).toEqual({ ocpfOffice: "House", district: "3RD SUFFOLK" });
  });

  it("rejects unsafe OCPF labels and incomplete app search inputs", () => {
    expect(mapMassachusettsOcpfOffice({ officeSought: "Governor's Council, 1st District" })).toBeNull();
    expect(mapMassachusettsOcpfOffice({ officeSought: "District Attorney, Suffolk" })).toBeNull();
    expect(mapMassachusettsOcpfOffice({ officeSought: "Mayor, Boston" })).toBeNull();
    expect(mapMassachusettsOcpfOffice({ officeSought: "" })).toBeNull();

    expect(
      toMassachusettsOcpfOfficeSearchInput({
        officeScope: "state_upper",
        officeCanonicalName: "State Senator",
      })
    ).toBeNull();
    expect(
      toMassachusettsOcpfOfficeSearchInput({
        officeScope: "statewide",
        officeCanonicalName: "Governor's Council Member",
      })
    ).toBeNull();
  });
});
