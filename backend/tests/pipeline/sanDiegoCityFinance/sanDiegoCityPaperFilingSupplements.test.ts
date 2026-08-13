import { describe, expect, it } from "vitest";

import {
  SAN_DIEGO_CITY_PAPER_496_SUPPLEMENTS,
  validateSanDiegoCityPaper496Supplements,
  type SanDiegoCityPaper496Supplement,
} from "../../../src/pipeline/sanDiegoCityFinance/sanDiegoCityPaperFilingSupplements.js";

function entry(
  overrides: Partial<SanDiegoCityPaper496Supplement> = {},
): SanDiegoCityPaper496Supplement {
  return {
    electionYear: 2026,
    spenderFilerId: "941786",
    spenderName: "Santa Clara County Government Attorneys' Association PAC",
    candidateLastName: "Campos",
    candidateFirstName: "Nora",
    officeCd: "CCM",
    jurisDscr: "City of San Jose",
    distNo: "5",
    direction: "OPPOSE",
    amountCents: 5270_27,
    expenditureDate: "2026-05-11",
    eFilingId: "24823",
    sourceNote: "test",
    ...overrides,
  };
}

describe("validateSanDiegoCityPaper496Supplements", () => {
  it("accepts the shipped curated list", () => {
    expect(() =>
      validateSanDiegoCityPaper496Supplements(SAN_DIEGO_CITY_PAPER_496_SUPPLEMENTS),
    ).not.toThrow();
    // Phase 4 sweep (2026-08-12) found exactly one qualifying paper 496:
    // California Working Families Party's $2,273.31 mail piece supporting
    // Nicole Crosby (D2). Pin its identity so an accidental edit fails here.
    expect(SAN_DIEGO_CITY_PAPER_496_SUPPLEMENTS).toHaveLength(1);
    expect(SAN_DIEGO_CITY_PAPER_496_SUPPLEMENTS[0]).toMatchObject({
      eFilingId: "300071441",
      spenderFilerId: "1438874",
      candidateLastName: "Crosby",
      distNo: "2",
      direction: "SUPPORT",
      amountCents: 227331,
    });
  });

  it("rejects a nonpositive or fractional amount", () => {
    expect(() =>
      validateSanDiegoCityPaper496Supplements([entry({ amountCents: 0 })]),
    ).toThrow(/positive integer/);
    expect(() =>
      validateSanDiegoCityPaper496Supplements([entry({ amountCents: 12.5 })]),
    ).toThrow(/positive integer/);
  });

  it("rejects blank identity fields and malformed dates", () => {
    expect(() =>
      validateSanDiegoCityPaper496Supplements([entry({ spenderFilerId: "  " })]),
    ).toThrow(/spenderFilerId is blank/);
    expect(() =>
      validateSanDiegoCityPaper496Supplements([
        entry({ expenditureDate: "05/11/2026" }),
      ]),
    ).toThrow(/ISO date/);
    // Shape-valid but calendar-invalid must fail too.
    expect(() =>
      validateSanDiegoCityPaper496Supplements([
        entry({ expenditureDate: "2026-02-29" }),
      ]),
    ).toThrow(/calendar date/);
  });

  it("rejects the same candidate twice on one filing, allows two candidates", () => {
    expect(() =>
      validateSanDiegoCityPaper496Supplements([entry(), entry()]),
    ).toThrow(/duplicate entry/);
    expect(() =>
      validateSanDiegoCityPaper496Supplements([
        entry(),
        entry({ candidateLastName: "Martinez", candidateFirstName: "Karen" }),
      ]),
    ).not.toThrow();
  });
});
