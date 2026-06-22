import { describe, expect, it } from "vitest";

import {
  isNewMexicoFinanceEligibleOffice,
  toNewMexicoFinanceOfficeKey,
} from "../../../src/pipeline/newMexicoFinance/newMexicoFinanceEligibleOffices.js";

describe("newMexicoFinanceEligibleOffices", () => {
  it("allows only the explicit CFIS-safe New Mexico office set", () => {
    expect(
      isNewMexicoFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "Governor",
      })
    ).toBe(true);
    expect(
      isNewMexicoFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "Lieutenant Governor",
      })
    ).toBe(true);
    expect(
      isNewMexicoFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "Secretary of State",
      })
    ).toBe(true);
    expect(
      isNewMexicoFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "Attorney General",
      })
    ).toBe(true);
    expect(
      isNewMexicoFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "State Treasurer",
      })
    ).toBe(true);
    expect(
      isNewMexicoFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "State Auditor",
      })
    ).toBe(true);
    expect(
      isNewMexicoFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "Land Commissioner",
      })
    ).toBe(true);
    expect(
      isNewMexicoFinanceEligibleOffice({
        officeScope: "state_upper",
        officeCanonicalName: "State Senator",
      })
    ).toBe(true);
    expect(
      isNewMexicoFinanceEligibleOffice({
        officeScope: "state_lower",
        officeCanonicalName: "State Lower Chamber Legislator",
      })
    ).toBe(true);

    expect(
      isNewMexicoFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "United States Senator",
      })
    ).toBe(false);
    expect(
      isNewMexicoFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "Commissioner of Insurance",
      })
    ).toBe(false);
    expect(
      isNewMexicoFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "Superintendent of Public Instruction",
      })
    ).toBe(false);
    expect(
      isNewMexicoFinanceEligibleOffice({
        officeScope: "statewide",
        officeCanonicalName: "Public Service Commissioner",
      })
    ).toBe(false);
    expect(
      isNewMexicoFinanceEligibleOffice({
        officeScope: "state_upper",
        officeCanonicalName: "State Lower Chamber Legislator",
      })
    ).toBe(false);
    expect(
      isNewMexicoFinanceEligibleOffice({
        officeScope: "county",
        officeCanonicalName: "Sheriff",
      })
    ).toBe(false);
  });

  it("normalizes whitespace without changing canonical names", () => {
    expect(
      toNewMexicoFinanceOfficeKey({
        officeScope: " statewide ",
        officeCanonicalName: " Governor ",
      })
    ).toBe("statewide::Governor");
    expect(
      toNewMexicoFinanceOfficeKey({
        officeScope: "\tstate_upper\n",
        officeCanonicalName: " State Senator ",
      })
    ).toBe("state_upper::State Senator");
  });

  it("rejects missing office fields", () => {
    expect(toNewMexicoFinanceOfficeKey({ officeScope: null, officeCanonicalName: "Governor" })).toBeNull();
    expect(toNewMexicoFinanceOfficeKey({ officeScope: "statewide", officeCanonicalName: undefined })).toBeNull();
    expect(toNewMexicoFinanceOfficeKey({ officeScope: "   ", officeCanonicalName: "Governor" })).toBeNull();
    expect(toNewMexicoFinanceOfficeKey({ officeScope: "statewide", officeCanonicalName: "   " })).toBeNull();
  });
});
