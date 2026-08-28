import { describe, expect, it } from "vitest";

import {
  classifySouthCarolinaRunOfficeLabel,
  southCarolinaConflictingOfficeLabels,
  southCarolinaLinkedDistrictNumber,
} from "../../../src/pipeline/southCarolinaFinance/southCarolinaOfficeEvidence.js";

describe("classifySouthCarolinaRunOfficeLabel", () => {
  it("classifies real legislative, statewide, and local labels", () => {
    expect(classifySouthCarolinaRunOfficeLabel("SC House of Representatives District 23")).toEqual({
      officeClass: "state_lower",
      district: 23,
    });
    expect(classifySouthCarolinaRunOfficeLabel("SC Senate District 7")).toEqual({
      officeClass: "state_upper",
      district: 7,
    });
    expect(classifySouthCarolinaRunOfficeLabel("Attorney General")).toEqual({
      officeClass: "statewide",
      district: null,
    });
    expect(classifySouthCarolinaRunOfficeLabel("Greenville City Council District 4")).toEqual({
      officeClass: "local",
      district: 4,
    });
    expect(classifySouthCarolinaRunOfficeLabel("Laurens Sheriff")).toEqual({
      officeClass: "local",
      district: null,
    });
  });

  it("treats broken, empty, and federal labels as no evidence", () => {
    // The literal "4" the API serves for current-cycle statewide runs.
    expect(classifySouthCarolinaRunOfficeLabel("4")).toBeNull();
    expect(classifySouthCarolinaRunOfficeLabel("")).toBeNull();
    expect(classifySouthCarolinaRunOfficeLabel("Other Office")).toBeNull();
    expect(classifySouthCarolinaRunOfficeLabel("United States Senate")).toBeNull();
  });
});

describe("southCarolinaLinkedDistrictNumber", () => {
  it("parses the district number out of the stored district name", () => {
    expect(southCarolinaLinkedDistrictNumber("State House District 23 (2024); South Carolina")).toBe(23);
    expect(southCarolinaLinkedDistrictNumber(null)).toBeNull();
    expect(southCarolinaLinkedDistrictNumber("South Carolina")).toBeNull();
  });
});

describe("southCarolinaConflictingOfficeLabels", () => {
  it("flags a different office class and a different district in the same chamber", () => {
    expect(
      southCarolinaConflictingOfficeLabels({
        officeScope: "statewide",
        district: null,
        rowOfficeLabels: ["SC House of Representatives District 23", "4"],
      })
    ).toEqual(["SC House of Representatives District 23"]);

    expect(
      southCarolinaConflictingOfficeLabels({
        officeScope: "state_lower",
        district: "State House District 23 (2024); South Carolina",
        rowOfficeLabels: ["SC House of Representatives District 24"],
      })
    ).toEqual(["SC House of Representatives District 24"]);
  });

  it("passes matching and no-evidence labels", () => {
    expect(
      southCarolinaConflictingOfficeLabels({
        officeScope: "state_lower",
        district: "State House District 23 (2024); South Carolina",
        rowOfficeLabels: ["SC House of Representatives District 23", "4", ""],
      })
    ).toEqual([]);
    expect(
      southCarolinaConflictingOfficeLabels({
        officeScope: "statewide",
        district: null,
        rowOfficeLabels: ["4", "Governor"],
      })
    ).toEqual([]);
  });
});
