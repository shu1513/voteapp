import { describe, expect, it } from "vitest";

import {
  SAN_JOSE_PAPER_496_SUPPLEMENTS,
  validateSanJosePaper496Supplements,
  type SanJosePaper496Supplement,
} from "../../../src/pipeline/sanJoseFinance/sanJosePaperFilingSupplements.js";

function entry(
  overrides: Partial<SanJosePaper496Supplement> = {},
): SanJosePaper496Supplement {
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

describe("validateSanJosePaper496Supplements", () => {
  it("accepts the shipped curated list", () => {
    expect(() =>
      validateSanJosePaper496Supplements(SAN_JOSE_PAPER_496_SUPPLEMENTS),
    ).not.toThrow();
    // The one live entry: the paper anti-Campos 496 absent from the export.
    expect(SAN_JOSE_PAPER_496_SUPPLEMENTS).toHaveLength(1);
    expect(SAN_JOSE_PAPER_496_SUPPLEMENTS[0]).toMatchObject({
      eFilingId: "24823",
      direction: "OPPOSE",
      amountCents: 527027,
    });
  });

  it("rejects a nonpositive or fractional amount", () => {
    expect(() =>
      validateSanJosePaper496Supplements([entry({ amountCents: 0 })]),
    ).toThrow(/positive integer/);
    expect(() =>
      validateSanJosePaper496Supplements([entry({ amountCents: 12.5 })]),
    ).toThrow(/positive integer/);
  });

  it("rejects blank identity fields and malformed dates", () => {
    expect(() =>
      validateSanJosePaper496Supplements([entry({ spenderFilerId: "  " })]),
    ).toThrow(/spenderFilerId is blank/);
    expect(() =>
      validateSanJosePaper496Supplements([
        entry({ expenditureDate: "05/11/2026" }),
      ]),
    ).toThrow(/ISO date/);
  });

  it("rejects the same candidate twice on one filing, allows two candidates", () => {
    expect(() =>
      validateSanJosePaper496Supplements([entry(), entry()]),
    ).toThrow(/duplicate entry/);
    expect(() =>
      validateSanJosePaper496Supplements([
        entry(),
        entry({ candidateLastName: "Martinez", candidateFirstName: "Karen" }),
      ]),
    ).not.toThrow();
  });
});
