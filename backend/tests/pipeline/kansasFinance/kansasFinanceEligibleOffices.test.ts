import { describe, expect, it } from "vitest";

import {
  formatKansasCfrDate,
  isKansasFinanceEligibleOffice,
  KANSAS_FINANCE_ELIGIBLE_OFFICE_KEYS,
  kansasCfrCycleStart,
  kansasCfrFiledDateWindow,
  kansasCfrOfficeForRace,
} from "../../../src/pipeline/kansasFinance/kansasFinanceEligibleOffices.js";

describe("kansasFinanceEligibleOffices", () => {
  it("covers the v1 scope: five statewide offices + both chambers", () => {
    expect([...KANSAS_FINANCE_ELIGIBLE_OFFICE_KEYS].sort()).toEqual([
      "state_lower::State Lower Chamber Legislator",
      "state_upper::State Senator",
      "statewide::Attorney General",
      "statewide::Commissioner of Insurance",
      "statewide::Governor",
      "statewide::Secretary of State",
      "statewide::State Treasurer",
    ]);
  });

  it("gates eligibility on scope + canonical name", () => {
    expect(isKansasFinanceEligibleOffice({ officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator" })).toBe(true);
    // Kansas county::District Attorney rows are County Attorneys (county filers), and federal races are the FEC path.
    expect(isKansasFinanceEligibleOffice({ officeScope: "county", officeCanonicalName: "District Attorney" })).toBe(false);
    expect(isKansasFinanceEligibleOffice({ officeScope: "statewide", officeCanonicalName: "United States Senator" })).toBe(false);
    expect(isKansasFinanceEligibleOffice({ officeScope: null, officeCanonicalName: null })).toBe(false);
  });

  it("maps races to live viewer office codes and labels", () => {
    expect(kansasCfrOfficeForRace({ officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator" })).toEqual({
      code: "7",
      label: "State Representative",
      districted: true,
      cycleYearsBefore: 1,
    });
    expect(kansasCfrOfficeForRace({ officeScope: "statewide", officeCanonicalName: "Commissioner of Insurance" })).toMatchObject({
      code: "4",
      label: "Insurance Commissioner",
      districted: false,
      cycleYearsBefore: 3,
    });
    expect(kansasCfrOfficeForRace({ officeScope: "state_upper", officeCanonicalName: "State Senator" })).toMatchObject({ code: "6", districted: true });
    expect(kansasCfrOfficeForRace({ officeScope: "county", officeCanonicalName: "District Attorney" })).toBeNull();
  });

  it("builds the cycle filed-date window in the viewer's MM/DD/YYYY format", () => {
    const now = new Date("2026-09-01T15:00:00.000Z");
    expect(formatKansasCfrDate(now)).toBe("09/01/2026");
    const house = kansasCfrOfficeForRace({ officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator" })!;
    expect(kansasCfrFiledDateWindow({ office: house, electionYear: 2026, now })).toEqual({ startDate: "01/01/2025", endDate: "09/01/2026" });
    const governor = kansasCfrOfficeForRace({ officeScope: "statewide", officeCanonicalName: "Governor" })!;
    expect(kansasCfrFiledDateWindow({ office: governor, electionYear: 2026, now })).toEqual({ startDate: "01/01/2023", endDate: "09/01/2026" });
    expect(() => kansasCfrFiledDateWindow({ office: house, electionYear: 1999, now })).toThrow("Invalid Kansas election year");
  });

  it("refuses an inverted window for a cycle that has not opened", () => {
    const house = kansasCfrOfficeForRace({ officeScope: "state_lower", officeCanonicalName: "State Lower Chamber Legislator" })!;
    expect(kansasCfrCycleStart(house, 2028).toISOString()).toBe("2027-01-01T00:00:00.000Z");
    // A Nov-2028 House race seen on 2026-11-08 has no cycle window yet.
    expect(() => kansasCfrFiledDateWindow({ office: house, electionYear: 2028, now: new Date("2026-11-08T12:00:00.000Z") })).toThrow(
      "opens 01/01/2027, after 11/08/2026"
    );
    expect(kansasCfrFiledDateWindow({ office: house, electionYear: 2028, now: new Date("2027-01-01T00:00:00.000Z") })).toEqual({
      startDate: "01/01/2027",
      endDate: "01/01/2027",
    });
  });
});
