import { describe, expect, it } from "vitest";

import { isPresidentialOfficeTitle } from "../../src/utils/presidentialOffice.js";

describe("presidentialOffice utils", () => {
  it("detects presidential contest titles", () => {
    expect(isPresidentialOfficeTitle("President and Vice President")).toBe(true);
    expect(isPresidentialOfficeTitle("U.S. President")).toBe(true);
    expect(isPresidentialOfficeTitle("President of the United States")).toBe(true);
    expect(isPresidentialOfficeTitle("Presidential Electors")).toBe(true);
    expect(isPresidentialOfficeTitle("Democratic Presidential Preference Primary")).toBe(true);
  });

  it("does not treat unrelated offices containing president-like words as presidential contests", () => {
    expect(isPresidentialOfficeTitle("President of the City Council")).toBe(false);
    expect(isPresidentialOfficeTitle("President, Board of Education")).toBe(false);
    expect(isPresidentialOfficeTitle("Governor")).toBe(false);
    expect(isPresidentialOfficeTitle("United States Senator")).toBe(false);
  });
});
