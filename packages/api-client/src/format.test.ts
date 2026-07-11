import { describe, expect, it } from "vitest";
import {
  formatDistrictType,
  formatElectionDate,
  formatMoney,
  formatOutcome,
  formatSourceHost,
  formatVotePowerLabel,
} from "./format";

describe("formatElectionDate", () => {
  it("renders YYYY-MM-DD as a local calendar date without timezone drift", () => {
    // new Date("2026-11-03") would be UTC midnight = Nov 2 in US timezones.
    expect(formatElectionDate("2026-11-03")).toBe("November 3, 2026");
  });

  it("passes through unparseable values", () => {
    expect(formatElectionDate("TBD")).toBe("TBD");
  });
});

describe("formatMoney", () => {
  it("formats whole dollars and dashes null", () => {
    expect(formatMoney(1234567)).toBe("$1,234,567");
    expect(formatMoney(null)).toBe("—");
  });
});

describe("formatDistrictType", () => {
  it("maps known types and humanizes unknown ones", () => {
    expect(formatDistrictType("state_upper")).toBe("State senate district");
    expect(formatDistrictType("us_house")).toBe("U.S. House district");
    expect(formatDistrictType("water_board_zone")).toBe("water board zone");
  });
});

describe("formatSourceHost", () => {
  it("extracts the hostname and strips www", () => {
    expect(formatSourceHost("https://www.capitol.texas.gov/BillLookup/History.aspx?Bill=SB2")).toBe(
      "capitol.texas.gov"
    );
    expect(formatSourceHost("not a url")).toBe("not a url");
  });
});

describe("formatVotePowerLabel", () => {
  it("maps known labels and passes unknown ones through untouched", () => {
    expect(formatVotePowerLabel("very_low")).toBe("Very low");
    expect(formatVotePowerLabel("unknown")).toBe("Unknown");
    expect(formatVotePowerLabel("super_high")).toBe("super_high");
  });
});

describe("formatOutcome", () => {
  it("sentence-cases snake_case outcomes", () => {
    expect(formatOutcome("too_close")).toBe("Too close");
    expect(formatOutcome("won")).toBe("Won");
  });

  it("returns empty or whitespace-only input unchanged", () => {
    expect(formatOutcome("")).toBe("");
    expect(formatOutcome("  ")).toBe("  ");
    expect(formatOutcome("___")).toBe("___");
  });
});
