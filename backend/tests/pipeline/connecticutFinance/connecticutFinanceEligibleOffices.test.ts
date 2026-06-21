import { describe, expect, it } from "vitest";

import {
  isConnecticutFinanceEligibleOffice,
  mapConnecticutEcrisOffice,
  normalizeConnecticutEcrisOfficeLabel,
} from "../../../src/pipeline/connecticutFinance/connecticutFinanceEligibleOffices.js";

describe("connecticutFinanceEligibleOffices", () => {
  it("allows only the explicit eCRIS-safe Connecticut office set", () => {
    expect(
      isConnecticutFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "Governor",
      })
    ).toBe(true);
    expect(
      isConnecticutFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "Comptroller",
      })
    ).toBe(true);
    expect(
      isConnecticutFinanceEligibleOffice({
        officeScope: "state_upper",
        officeCanonicalName: "State Senator",
      })
    ).toBe(true);
    expect(
      isConnecticutFinanceEligibleOffice({
        officeScope: "state_lower",
        officeCanonicalName: "State Lower Chamber Legislator",
      })
    ).toBe(true);

    expect(
      isConnecticutFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "United States Senator",
      })
    ).toBe(false);
    expect(
      isConnecticutFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "State Auditor",
      })
    ).toBe(false);
    expect(
      isConnecticutFinanceEligibleOffice({
        officeScope: "county",
        officeCanonicalName: "Sheriff",
      })
    ).toBe(false);
  });

  it("normalizes eCRIS office labels conservatively", () => {
    expect(normalizeConnecticutEcrisOfficeLabel(" Secretary   of   the State ")).toBe("SECRETARY OF THE STATE");
    expect(normalizeConnecticutEcrisOfficeLabel("\tstate representative\n")).toBe("STATE REPRESENTATIVE");
    expect(normalizeConnecticutEcrisOfficeLabel("   ")).toBeNull();
    expect(normalizeConnecticutEcrisOfficeLabel(null)).toBeNull();
  });

  it("maps safe statewide eCRIS labels to canonical app offices", () => {
    expect(mapConnecticutEcrisOffice({ officeSought: "Secretary of the State" })).toEqual({
      officeScope: "statewide",
      officeCanonicalName: "Secretary of State",
      officeKey: "statewide::Secretary of State",
      requiresDistrict: false,
    });
    expect(mapConnecticutEcrisOffice({ officeSought: "State Comptroller" })).toEqual({
      officeScope: "statewide",
      officeCanonicalName: "Comptroller",
      officeKey: "statewide::Comptroller",
      requiresDistrict: false,
    });
    expect(mapConnecticutEcrisOffice({ officeSought: "State Treasurer" })).toEqual({
      officeScope: "statewide",
      officeCanonicalName: "State Treasurer",
      officeKey: "statewide::State Treasurer",
      requiresDistrict: false,
    });
  });

  it("requires districts for Connecticut legislative offices", () => {
    expect(mapConnecticutEcrisOffice({ officeSought: "State Senator", district: "2" })).toEqual({
      officeScope: "state_upper",
      officeCanonicalName: "State Senator",
      officeKey: "state_upper::State Senator",
      requiresDistrict: true,
    });
    expect(mapConnecticutEcrisOffice({ officeSought: "State Representative", district: "8" })).toEqual({
      officeScope: "state_lower",
      officeCanonicalName: "State Lower Chamber Legislator",
      officeKey: "state_lower::State Lower Chamber Legislator",
      requiresDistrict: true,
    });

    expect(mapConnecticutEcrisOffice({ officeSought: "State Senator" })).toBeNull();
    expect(mapConnecticutEcrisOffice({ officeSought: "State Representative", district: "   " })).toBeNull();
  });

  it("rejects unsafe or non-candidate eCRIS office labels", () => {
    expect(mapConnecticutEcrisOffice({ officeSought: "Undetermined" })).toBeNull();
    expect(mapConnecticutEcrisOffice({ officeSought: "Judge of Probate" })).toBeNull();
    expect(mapConnecticutEcrisOffice({ officeSought: "Comptroller" })).toBeNull();
    expect(mapConnecticutEcrisOffice({ officeSought: "Mayor" })).toBeNull();
    expect(mapConnecticutEcrisOffice({ officeSought: "United States Senator" })).toBeNull();
    expect(mapConnecticutEcrisOffice({ officeSought: "" })).toBeNull();
  });
});
