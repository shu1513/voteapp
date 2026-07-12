import { describe, expect, it } from "vitest";
import { isLosAngelesCityFinanceEligibleElection } from "../../../src/pipeline/losAngelesCityFinance/losAngelesCityFinanceEligibleOffices.js";

describe("Los Angeles City finance eligibility", () => {
  const mayor = {
    state: "CA",
    districtType: "place",
    geoidCompact: "0644000",
    officeScope: "place",
    officeCanonicalName: "Mayor",
  };
  it("accepts only exact Los Angeles Mayor identity", () => {
    expect(isLosAngelesCityFinanceEligibleElection(mayor)).toBe(true);
    expect(
      isLosAngelesCityFinanceEligibleElection({
        ...mayor,
        geoidCompact: "0666000",
      }),
    ).toBe(false);
    expect(
      isLosAngelesCityFinanceEligibleElection({
        ...mayor,
        officeCanonicalName: "City Council Member",
      }),
    ).toBe(false);
    expect(
      isLosAngelesCityFinanceEligibleElection({
        ...mayor,
        districtType: "county",
      }),
    ).toBe(false);
  });
});
