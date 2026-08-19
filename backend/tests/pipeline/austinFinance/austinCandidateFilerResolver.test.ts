import { describe, expect, it } from "vitest";
import {
  austinPersonNameMatchesCandidate,
  collectAustinReportFilers,
  resolveAustinCandidateFilers,
  type AustinAppCandidate,
  type AustinReportFiler,
} from "../../../src/pipeline/austinFinance/austinCandidateFilerResolver.js";
import type { AustinReportDetailRow } from "../../../src/pipeline/austinFinance/austinSocrataClient.js";

function reportRow(
  overrides: Partial<AustinReportDetailRow> & { reportId: string },
): AustinReportDetailRow {
  return {
    filerName: "Watson, Kirk P.",
    formTypeCode: "COH",
    formType: "COH - Candidate /Officeholder Campaign Finance Report",
    reportType: "Semiannual",
    dateFiled: "2026-07-15T00:00:00.000",
    periodFrom: "2026-01-01T00:00:00.000",
    periodTo: "2026-06-30T00:00:00.000",
    electionDate: "2026-11-03T00:00:00.000",
    electionType: "General",
    officeSought: "MAYOR",
    officeHeld: null,
    contribTotalCents: 0,
    expendTotalCents: 0,
    contribBalanceCents: 0,
    outstandingLoanCents: 0,
    reportUrl: null,
    ...overrides,
  };
}

function filer(
  filerName: string,
  officeCodes: string[] = ["COUNCIL_MBR_DISTRICT_09"],
): AustinReportFiler {
  return { filerName, officeCodes: officeCodes as AustinReportFiler["officeCodes"], rowCount: 1 };
}

function candidate(
  candidateId: string,
  displayName: string,
  officeCode: AustinAppCandidate["officeCode"] = "COUNCIL_MBR_DISTRICT_09",
): AustinAppCandidate {
  return { candidateId, displayName, officeCode };
}

describe("collectAustinReportFilers", () => {
  it("groups candidate-form rows by exact filer name with their parsed office codes", () => {
    const filers = collectAustinReportFilers([
      reportRow({ reportId: "r1", filerName: "Xie, Selena", officeSought: "COUNCIL_MBR_DISTRICT_08" }),
      reportRow({
        reportId: "r2",
        filerName: "Xie, Selena",
        officeSought: "COUNCIL_MBR_DISTRICT_08 District 8",
        formTypeCode: "CORCOH",
      }),
      reportRow({ reportId: "r3", filerName: "Xie, Selena", officeSought: "NONE", formTypeCode: "COHATX7" }),
      reportRow({ reportId: "r4", filerName: "Alter, Ryan", officeSought: "COUNCIL_MBR_DISTRICT_05" }),
      // Duplicate report rows count twice here — rowCount is informational,
      // identity is the name.
      reportRow({ reportId: "r4", filerName: "Alter, Ryan", officeSought: "COUNCIL_MBR_DISTRICT_05" }),
    ]);
    expect(filers).toEqual([
      { filerName: "Alter, Ryan", officeCodes: ["COUNCIL_MBR_DISTRICT_05"], rowCount: 2 },
      { filerName: "Xie, Selena", officeCodes: ["COUNCIL_MBR_DISTRICT_08"], rowCount: 3 },
    ]);
  });

  it("ignores PAC forms, dissolutions, and rows without a filer name; keeps code-less filers", () => {
    const filers = collectAustinReportFilers([
      reportRow({ reportId: "p1", filerName: "All for Austin", formTypeCode: "SPAC", officeSought: null }),
      reportRow({ reportId: "p2", filerName: "City Accountability Project", formTypeCode: "PACDR", officeSought: null }),
      reportRow({ reportId: "n1", filerName: null, formTypeCode: "COH" }),
      reportRow({ reportId: "n2", filerName: "  ", formTypeCode: "COH" }),
      reportRow({ reportId: "c1", filerName: "Kitchen, Ann", formTypeCode: "COH", officeSought: "NONE" }),
    ]);
    // A filer with no parsable office code is kept (it can never match, but
    // it is visible in dry-run output rather than silently dropped).
    expect(filers).toEqual([{ filerName: "Kitchen, Ann", officeCodes: [], rowCount: 1 }]);
  });

  it("keeps two spellings of one name as two filers", () => {
    const filers = collectAustinReportFilers([
      reportRow({ reportId: "a", filerName: "Watson, Kirk P." }),
      reportRow({ reportId: "b", filerName: "Watson, Kirk P" }),
    ]);
    expect(filers.map((entry) => entry.filerName)).toEqual(["Watson, Kirk P", "Watson, Kirk P."]);
  });
});

describe("austinPersonNameMatchesCandidate", () => {
  it("matches Last, First M. filer names token-wise with nicknames and diacritics folded", () => {
    expect(austinPersonNameMatchesCandidate("Watson, Kirk P.", "Kirk Watson")).toBe(true);
    expect(austinPersonNameMatchesCandidate("Heyman, Richard", "Rich Heyman")).toBe(true);
    expect(austinPersonNameMatchesCandidate("Velasquez, Jose", "José Velásquez")).toBe(true);
    expect(austinPersonNameMatchesCandidate("Bowen, Jeffery L.", "Jeffery Bowen")).toBe(true);
    expect(austinPersonNameMatchesCandidate("Goodwin, Amber K.", "Amber K. Goodwin")).toBe(true);
    expect(austinPersonNameMatchesCandidate("Herrin III, Louis C.", "Louis Herrin III")).toBe(true);
  });

  it("reads quoted call names on both sides (live filer spellings)", () => {
    expect(austinPersonNameMatchesCandidate('Vela, Jose "Chito", III', "Chito Vela")).toBe(true);
    expect(austinPersonNameMatchesCandidate('Vela, Jose "Chito", III', "Jose Vela")).toBe(true);
    expect(austinPersonNameMatchesCandidate('Renteria, Sabino "Pio"', "Pio Renteria")).toBe(true);
    expect(austinPersonNameMatchesCandidate('Craig, Kenneth O. "Ken", Jr.', "Ken Craig")).toBe(true);
    expect(austinPersonNameMatchesCandidate("Qadri, Zohaib", 'Zohaib "Zo" Qadri')).toBe(true);
    expect(austinPersonNameMatchesCandidate("Romero, Eduardo", 'Eduardo "Lalito" Romero')).toBe(true);
  });

  it("rejects substrings, middle conflicts, generational conflicts, and different people", () => {
    expect(austinPersonNameMatchesCandidate("Watson, Kirk P.", "Kirk")).toBe(false);
    expect(austinPersonNameMatchesCandidate("Watson, Kirk P.", "Kirk B. Watson")).toBe(false);
    expect(austinPersonNameMatchesCandidate("Herrin III, Louis C.", "Louis Herrin Jr.")).toBe(false);
    expect(austinPersonNameMatchesCandidate("Anderson, Alexandria M.", "Alexander Anderson")).toBe(false);
    expect(austinPersonNameMatchesCandidate("Watson, Kurt", "Kirk Watson")).toBe(false);
    // Bare "V" is a middle initial, not a suffix (the shared policy): it must
    // stay as middle evidence on either side, so these conflict.
    expect(austinPersonNameMatchesCandidate("Smith, John B.", "John V. Smith")).toBe(false);
    expect(austinPersonNameMatchesCandidate("Smith, John V", "John B. Smith")).toBe(false);
    expect(austinPersonNameMatchesCandidate("Smith, John V", "John V. Smith")).toBe(true);
    expect(austinPersonNameMatchesCandidate("Smith, John V", "John Smith")).toBe(true);
    expect(austinPersonNameMatchesCandidate("Watson, Kirk P.", "Kirk Watts")).toBe(false);
  });
});

describe("resolveAustinCandidateFilers", () => {
  it("matches a clean one-to-one filer under the office gate", () => {
    const [resolution] = resolveAustinCandidateFilers({
      candidates: [candidate("c1", 'Zohaib "Zo" Qadri')],
      filers: [
        filer("Qadri, Zohaib", ["COUNCIL_MBR_DISTRICT_09"]),
        filer("Heyman, Richard", ["COUNCIL_MBR_DISTRICT_09"]),
      ],
    });
    expect(resolution).toEqual({
      candidate: candidate("c1", 'Zohaib "Zo" Qadri'),
      status: "matched",
      filerName: "Qadri, Zohaib",
    });
  });

  it("fails closed on a candidate without an office code", () => {
    const [resolution] = resolveAustinCandidateFilers({
      candidates: [candidate("c1", "Zohaib Qadri", null)],
      filers: [filer("Qadri, Zohaib")],
    });
    expect(resolution).toMatchObject({
      status: "unmatched",
      reason: expect.stringContaining("no office code"),
    });
  });

  it("does not match a same-named filer for a different seat or with no seat", () => {
    const resolutions = resolveAustinCandidateFilers({
      candidates: [candidate("c1", "Zohaib Qadri", "COUNCIL_MBR_DISTRICT_09")],
      filers: [filer("Qadri, Zohaib", ["MAYOR"]), filer("Qadri, Zohaib ", [])],
    });
    expect(resolutions[0]).toMatchObject({
      status: "unmatched",
      reason: "no Report Detail filer for COUNCIL_MBR_DISTRICT_09 name-matches",
    });
  });

  it("matches a filer whose rows span the seat among others (any-code office gate)", () => {
    const [resolution] = resolveAustinCandidateFilers({
      candidates: [candidate("c1", "Kirk Watson", "MAYOR")],
      filers: [filer("Watson, Kirk P.", ["COUNCIL_MBR_DISTRICT_09", "MAYOR"])],
    });
    expect(resolution).toMatchObject({ status: "matched", filerName: "Watson, Kirk P." });
  });

  it("flags two name-matching filers as ambiguous, including two spellings of one name", () => {
    const [resolution] = resolveAustinCandidateFilers({
      candidates: [candidate("c1", "Kirk Watson", "MAYOR")],
      filers: [filer("Watson, Kirk P.", ["MAYOR"]), filer("Watson, Kirk P", ["MAYOR"])],
    });
    expect(resolution).toMatchObject({
      status: "ambiguous",
      reason: '2 Report Detail filers name-match ("Watson, Kirk P.", "Watson, Kirk P"); link manually',
    });
  });

  it("fails one filer claimed by two roster candidates closed for both", () => {
    const resolutions = resolveAustinCandidateFilers({
      candidates: [candidate("c1", "Kirk Watson", "MAYOR"), candidate("c2", "Kirk P. Watson", "MAYOR")],
      filers: [filer("Watson, Kirk P.", ["MAYOR"])],
    });
    expect(resolutions.map((resolution) => resolution.status)).toEqual(["ambiguous", "ambiguous"]);
    expect(resolutions[0]).toMatchObject({
      reason: 'filer "Watson, Kirk P." resolves to multiple roster candidates; link manually',
    });
  });

  it("reports no match when no filer name-matches", () => {
    const [resolution] = resolveAustinCandidateFilers({
      candidates: [candidate("c1", "Kai Huang")],
      filers: [filer("Qadri, Zohaib"), filer("Heyman, Richard")],
    });
    expect(resolution).toMatchObject({ status: "unmatched" });
  });
});
