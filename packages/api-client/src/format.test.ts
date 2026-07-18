import { describe, expect, it } from "vitest";
import {
  financeSourceLabel,
  formatDistrictType,
  formatElectionDate,
  formatFinanceCategory,
  formatMoney,
  formatOutcome,
  formatRosterStatus,
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

describe("formatRosterStatus", () => {
  it("includes the re-check date only when present", () => {
    expect(formatRosterStatus({ reason: "awaiting_official_roster", check_after: "2026-08-27" })).toEqual({
      short: "Candidate list not final",
      long: "Election officials haven't published a final candidate list for this race. We'll check again after August 27, 2026.",
    });
    expect(formatRosterStatus({ reason: "awaiting_official_roster", check_after: null }).long).toBe(
      "Election officials haven't published a final candidate list for this race."
    );
  });

  it("describes staged rosters as processing", () => {
    expect(formatRosterStatus({ reason: "roster_processing", check_after: null })).toEqual({
      short: "Candidate details coming soon",
      long: "A candidate list is available — candidate profiles are being prepared.",
    });
  });

  it("falls back to unavailable copy for the generic and unknown reasons", () => {
    const unavailable = {
      short: "Candidate list unavailable",
      long: "Candidate information for this race isn't available yet.",
    };
    expect(formatRosterStatus({ reason: "candidate_information_unavailable", check_after: null })).toEqual(
      unavailable
    );
    // Forward-compat: a newer backend may add reasons this client predates.
    expect(formatRosterStatus({ reason: "some_future_reason", check_after: null })).toEqual(unavailable);
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
    // "Low" reads as a verdict on the voter; the chip says "Below average".
    expect(formatVotePowerLabel("low")).toBe("Below average");
    // "Medium" is a size word; the scale speaks in average-relative terms.
    expect(formatVotePowerLabel("medium")).toBe("Average");
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

describe("formatFinanceCategory", () => {
  it("maps known industry slugs to display names", () => {
    expect(formatFinanceCategory("oil_gas_energy")).toBe("Oil, gas, and energy");
    expect(formatFinanceCategory("labor_unions")).toBe("Labor unions");
  });

  it("sentence-cases unknown slugs", () => {
    expect(formatFinanceCategory("crypto_assets")).toBe("Crypto assets");
  });

  it("passes free-text occupation names through unchanged", () => {
    expect(formatFinanceCategory("Retired")).toBe("Retired");
    expect(formatFinanceCategory("Software Engineer")).toBe("Software Engineer");
  });
});

describe("financeSourceLabel", () => {
  it("maps known source enums to display names", () => {
    expect(financeSourceLabel("FEC")).toBe("FEC");
    expect(financeSourceLabel("MASSACHUSETTS_OCPF")).toBe("Massachusetts OCPF");
    expect(financeSourceLabel("UTAH_DISCLOSURES")).toBe("Utah Financial Disclosures");
    expect(financeSourceLabel("NEW_YORK_CITY_CFB")).toBe("NYC Campaign Finance Board");
    expect(financeSourceLabel("HOUSTON_CAMPAIGN_FINANCE")).toBe("City of Houston / Texas Ethics Commission");
  });

  it("title-cases unknown enums instead of leaking raw values", () => {
    expect(financeSourceLabel("NEW_STATE_PORTAL")).toBe("New State Portal");
  });
});
