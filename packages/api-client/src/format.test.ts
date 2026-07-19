import { describe, expect, it } from "vitest";
import {
  financeSourceLabel,
  formatDistrictType,
  formatElectionDate,
  formatFinanceCategory,
  formatMoney,
  formatNameList,
  formatOutcome,
  formatOutsideEvidenceLines,
  formatRosterStatus,
  formatSourceHost,
  formatVotePowerLabel,
  sortContributionSizeBuckets,
} from "./format";
import type { FinanceOutsideIndustryEvidence } from "./types";

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

describe("formatNameList", () => {
  it("joins one, two, and three-plus names with Oxford-comma grammar", () => {
    expect(formatNameList(["Google"])).toBe("Google");
    expect(formatNameList(["Google", "Anthropic"])).toBe("Google and Anthropic");
    expect(formatNameList(["Google", "Anthropic", "Amazon"])).toBe("Google, Anthropic, and Amazon");
  });

  it("drops duplicates and whitespace-only entries, returning '' when nothing survives", () => {
    expect(formatNameList(["Google", " Google", "Google "])).toBe("Google");
    expect(formatNameList(["  ", "", "\t"])).toBe("");
    expect(formatNameList([])).toBe("");
  });
});

describe("sortContributionSizeBuckets", () => {
  it("orders buckets largest-first by the leading dollar amount", () => {
    const rows = [
      { category_name: "$1-$99" },
      { category_name: "$5,000+" },
      { category_name: "$500-$999" },
      { category_name: "$1,000-$4,999" },
    ];
    expect(sortContributionSizeBuckets(rows).map((row) => row.category_name)).toEqual([
      "$5,000+",
      "$1,000-$4,999",
      "$500-$999",
      "$1-$99",
    ]);
  });

  it("keeps unparseable labels last in their original relative order", () => {
    const rows = [
      { category_name: "Unitemized" },
      { category_name: "$100-$499" },
      { category_name: "Other receipts" },
      { category_name: "$5,000+" },
    ];
    expect(sortContributionSizeBuckets(rows).map((row) => row.category_name)).toEqual([
      "$5,000+",
      "$100-$499",
      "Unitemized",
      "Other receipts",
    ]);
  });

  it("does not mutate its input", () => {
    const rows = [{ category_name: "$1-$99" }, { category_name: "$5,000+" }];
    sortContributionSizeBuckets(rows);
    expect(rows.map((row) => row.category_name)).toEqual(["$1-$99", "$5,000+"]);
  });
});

describe("formatOutsideEvidenceLines", () => {
  const evidence = (overrides: Partial<FinanceOutsideIndustryEvidence>): FinanceOutsideIndustryEvidence => ({
    organization_name: "Google",
    organization_type: "donor",
    amount: 1000,
    contributor_count: null,
    committee_id: "pac-1",
    committee_name: "Growth PAC",
    source_url: null,
    ...overrides,
  });

  it("presents donor rows as the organization's own money", () => {
    expect(formatOutsideEvidenceLines([evidence({})])).toEqual(["Money from Google, given to Growth PAC."]);
  });

  it("presents employer rows as contributors' money, never the company's", () => {
    expect(formatOutsideEvidenceLines([evidence({ organization_type: "employer" })])).toEqual([
      "Money from contributors employed by Google, given to Growth PAC.",
    ]);
  });

  it("combines donor and employer rows for one committee in a single line", () => {
    expect(
      formatOutsideEvidenceLines([
        evidence({}),
        evidence({ organization_name: "Anthropic", organization_type: "employer" }),
      ])
    ).toEqual(["Money from Google, and from contributors employed by Anthropic, given to Growth PAC."]);
  });

  it("keeps each organization paired with its own committee across lines", () => {
    expect(
      formatOutsideEvidenceLines([
        evidence({}),
        evidence({
          organization_name: "Anthropic",
          organization_type: "employer",
          committee_id: "pac-2",
          committee_name: "Future PAC",
        }),
      ])
    ).toEqual([
      "Money from Google, given to Growth PAC.",
      "Money from contributors employed by Anthropic, given to Future PAC.",
    ]);
  });

  it("returns no lines for empty or name-less evidence", () => {
    expect(formatOutsideEvidenceLines([])).toEqual([]);
    expect(formatOutsideEvidenceLines([evidence({ organization_name: "  " })])).toEqual([]);
  });
});
