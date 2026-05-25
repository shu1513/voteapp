import { describe, expect, it } from "vitest";

import { hasSpecialSeatMarker, isUsSenateOfficeTitle } from "../../src/utils/senateOffice.js";

describe("senateOffice utils", () => {
  it("detects U.S. Senate office titles", () => {
    expect(isUsSenateOfficeTitle("United States Senator")).toBe(true);
    expect(isUsSenateOfficeTitle("U.S. Senator (Unexpired Term)")).toBe(true);
    expect(isUsSenateOfficeTitle("Governor")).toBe(false);
  });

  it("detects special seat markers", () => {
    expect(
      hasSpecialSeatMarker({
        official_ballot_title: "United States Senator (Unexpired Term)",
        description: "Fills a vacancy for remainder of term.",
        election_stage: "general",
      })
    ).toBe(true);

    expect(
      hasSpecialSeatMarker({
        official_ballot_title: "United States Senator",
        description: "Regular six-year term.",
        election_stage: "general",
      })
    ).toBe(false);
  });
});
