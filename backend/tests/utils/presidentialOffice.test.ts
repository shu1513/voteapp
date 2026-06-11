import { describe, expect, it } from "vitest";

import {
  filterPresidentialElectionEntries,
  isPresidentialOfficeTitle,
} from "../../src/utils/presidentialOffice.js";

describe("presidentialOffice utils", () => {
  it("detects presidential contest titles", () => {
    expect(isPresidentialOfficeTitle("President and Vice President")).toBe(true);
    expect(isPresidentialOfficeTitle("President and Vice-President")).toBe(true);
    expect(isPresidentialOfficeTitle("President/Vice-President")).toBe(true);
    expect(isPresidentialOfficeTitle("Vice-President and President")).toBe(true);
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

  it("filters presidential election entries and reports removed titles", () => {
    const result = filterPresidentialElectionEntries([
      {
        race_type: "office",
        official_ballot_title: "Governor",
        election_date: "2028-11-07",
        sources: ["https://example.org/governor"],
      },
      {
        race_type: "office",
        official_ballot_title: "President and Vice-President",
        election_date: "2028-11-07",
        sources: ["https://example.org/president"],
      },
    ]);

    expect(result.entries.map((entry) => entry.official_ballot_title)).toEqual(["Governor"]);
    expect(result.removedTitles).toEqual(["President and Vice-President"]);
  });
});
