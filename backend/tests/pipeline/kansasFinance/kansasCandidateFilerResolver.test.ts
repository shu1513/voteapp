import { describe, expect, it } from "vitest";

import {
  kansasDistrictNumberFromGrid,
  kansasFiledNameCommaVariants,
  resolveKansasCandidateFiler,
  type KansasFilerRow,
} from "../../../src/pipeline/kansasFinance/kansasCandidateFilerResolver.js";

// Filed names below are synthetic on purpose (the plan's 25-4154(d) posture
// keeps real Kansas contributor identities out of fixtures; candidate names
// are shaped like the live grid but invented).
function row(overrides: Partial<KansasFilerRow>): KansasFilerRow {
  return {
    filedName: "HOLLOWAY MARGARET",
    district: "85",
    officeSought: "State Representative",
    filingKind: "report",
    fileDate: "07/27/2026",
    ...overrides,
  };
}

describe("kansasFiledNameCommaVariants", () => {
  it("offers every surname split of a LAST FIRST grid name", () => {
    expect(kansasFiledNameCommaVariants("HOLLOWAY MARGARET")).toEqual(["HOLLOWAY, MARGARET"]);
    expect(kansasFiledNameCommaVariants("VAN DYKE MARY")).toEqual(["VAN, DYKE MARY", "VAN DYKE, MARY"]);
    expect(kansasFiledNameCommaVariants("Holloway, Margaret")).toEqual(["Holloway, Margaret"]);
    expect(kansasFiledNameCommaVariants("HOLLOWAY")).toEqual([]);
    expect(kansasFiledNameCommaVariants("  ")).toEqual([]);
  });
});

describe("kansasDistrictNumberFromGrid", () => {
  it("reads plain numbers and fails closed on blanks", () => {
    expect(kansasDistrictNumberFromGrid("85")).toBe(85);
    expect(kansasDistrictNumberFromGrid(" 7 ")).toBe(7);
    expect(kansasDistrictNumberFromGrid("")).toBeNull();
    expect(kansasDistrictNumberFromGrid("0")).toBeNull();
    expect(kansasDistrictNumberFromGrid(null)).toBeNull();
  });
});

describe("resolveKansasCandidateFiler", () => {
  it("matches full name + district and folds spelling variants of one person", () => {
    const resolution = resolveKansasCandidateFiler({
      candidateName: "Margaret Holloway",
      districtNumber: 85,
      rows: [
        row({}),
        row({ filedName: "HOLLOWAY MARGARET", filingKind: "appointment_of_treasurer" }),
        row({ filedName: "HOLLOWAY MARGARET A" }),
        // Same surname, other district: excluded by the district gate.
        row({ filedName: "HOLLOWAY MARGARET", district: "86" }),
        // Same district, different person.
        row({ filedName: "PRUITT DANIEL" }),
      ],
    });
    expect(resolution).toEqual({
      status: "matched",
      match: {
        surname: "HOLLOWAY",
        firstName: "MARGARET",
        committeeName: "HOLLOWAY MARGARET",
        filedNames: ["HOLLOWAY MARGARET", "HOLLOWAY MARGARET A"],
        rowCount: 3,
        confidence: "name_exact",
      },
    });
  });

  it("never links on a bare surname or a different surname", () => {
    expect(
      resolveKansasCandidateFiler({ candidateName: "Margaret Holloway", districtNumber: 85, rows: [row({ filedName: "HOLLOWAY" })] })
    ).toEqual({ status: "unmatched", reason: "no_matching_filer" });
    expect(
      resolveKansasCandidateFiler({ candidateName: "Margaret Halloway", districtNumber: 85, rows: [row({})] })
    ).toEqual({ status: "unmatched", reason: "no_matching_filer" });
    expect(resolveKansasCandidateFiler({ candidateName: "  ", districtNumber: 85, rows: [row({})] })).toEqual({
      status: "unmatched",
      reason: "missing_candidate_name",
    });
  });

  it("expands roster nicknames one-sidedly and labels the confidence", () => {
    const resolution = resolveKansasCandidateFiler({
      candidateName: "Steve Brunson",
      districtNumber: 85,
      rows: [row({ filedName: "BRUNSON STEVEN" }), row({ filedName: "BRUNSON STEVE" })],
    });
    expect(resolution).toMatchObject({
      status: "matched",
      match: { surname: "BRUNSON", filedNames: ["BRUNSON STEVE", "BRUNSON STEVEN"], confidence: "name_nickname" },
    });
    // Frequency decides the stored spelling.
    const frequent = resolveKansasCandidateFiler({
      candidateName: "Steve Brunson",
      districtNumber: 85,
      rows: [row({ filedName: "BRUNSON STEVEN" }), row({ filedName: "BRUNSON STEVEN" }), row({ filedName: "BRUNSON STEVE" })],
    });
    expect(frequent).toMatchObject({ status: "matched", match: { committeeName: "BRUNSON STEVEN", firstName: "STEVEN" } });
  });

  it("rejects a middle-initial conflict with the roster name", () => {
    const resolution = resolveKansasCandidateFiler({
      candidateName: "Margaret T. Holloway",
      districtNumber: 85,
      rows: [row({ filedName: "HOLLOWAY MARGARET B" }), row({ filedName: "HOLLOWAY MARGARET T" })],
    });
    expect(resolution).toMatchObject({
      status: "matched",
      match: { filedNames: ["HOLLOWAY MARGARET T"], rowCount: 1 },
    });
  });

  it("reports ambiguity when aligned spellings contradict each other", () => {
    const resolution = resolveKansasCandidateFiler({
      candidateName: "Margaret Holloway",
      districtNumber: 85,
      rows: [row({ filedName: "HOLLOWAY MARGARET B" }), row({ filedName: "HOLLOWAY MARGARET T" })],
    });
    expect(resolution).toEqual({
      status: "ambiguous",
      reason: "conflicting_filed_names",
      filedNames: ["HOLLOWAY MARGARET B", "HOLLOWAY MARGARET T"],
    });
    const generations = resolveKansasCandidateFiler({
      candidateName: "Daniel Pruitt",
      districtNumber: 85,
      rows: [row({ filedName: "PRUITT DANIEL JR" }), row({ filedName: "PRUITT DANIEL SR" })],
    });
    expect(generations).toMatchObject({ status: "ambiguous" });
  });

  it("reports ambiguity when a roster nickname aligns with two different legal names", () => {
    // "Pat" expands to both PATRICK and PATRICIA: two people, no link.
    const resolution = resolveKansasCandidateFiler({
      candidateName: "Pat Sandoval",
      districtNumber: 85,
      rows: [row({ filedName: "SANDOVAL PATRICK" }), row({ filedName: "SANDOVAL PATRICK" }), row({ filedName: "SANDOVAL PATRICIA" })],
    });
    expect(resolution).toEqual({
      status: "ambiguous",
      reason: "conflicting_filed_names",
      filedNames: ["SANDOVAL PATRICK", "SANDOVAL PATRICIA"],
    });
    // One nickname family ("BILL"/"WILLIAM") is still one person.
    expect(
      resolveKansasCandidateFiler({
        candidateName: "Bill Sandoval",
        districtNumber: 85,
        rows: [row({ filedName: "SANDOVAL BILL" }), row({ filedName: "SANDOVAL WILLIAM" })],
      })
    ).toMatchObject({ status: "matched", match: { filedNames: ["SANDOVAL BILL", "SANDOVAL WILLIAM"], confidence: "name_nickname" } });
  });

  it("sends blank-district-only matches to manual review", () => {
    const resolution = resolveKansasCandidateFiler({
      candidateName: "Margaret Holloway",
      districtNumber: 85,
      rows: [row({ district: "" })],
    });
    expect(resolution).toEqual({
      status: "manual_confirm_required",
      reason: "filings_missing_district",
      filedNames: ["HOLLOWAY MARGARET"],
    });
    // A blank-district filing never blocks a district-confirmed match.
    expect(
      resolveKansasCandidateFiler({ candidateName: "Margaret Holloway", districtNumber: 85, rows: [row({}), row({ district: "" })] })
    ).toMatchObject({ status: "matched", match: { rowCount: 1 } });
  });

  it("ignores the district column for statewide offices", () => {
    // Live: a Governor filing carried a stray district ("4").
    const resolution = resolveKansasCandidateFiler({
      candidateName: "Stacy Rowan",
      districtNumber: null,
      rows: [row({ filedName: "ROWAN STACY", district: "4", officeSought: "Governor" }), row({ filedName: "ROWAN STACY", district: "", officeSought: "Governor" })],
    });
    expect(resolution).toMatchObject({ status: "matched", match: { surname: "ROWAN", rowCount: 2 } });
  });

  it("keeps a filed generational suffix out of the recipe key", () => {
    // Live 2026 shape: the suffix typed into the surname cell.
    const resolution = resolveKansasCandidateFiler({
      candidateName: "Bobby Robinsonette",
      districtNumber: 73,
      rows: [row({ filedName: "JR ROBINSONETTE BOBBY JOE", district: "73" })],
    });
    expect(resolution).toMatchObject({
      status: "matched",
      match: { surname: "ROBINSONETTE", firstName: "BOBBY", committeeName: "JR ROBINSONETTE BOBBY JOE" },
    });
  });

  it("aligns compound surnames through the split variants", () => {
    const resolution = resolveKansasCandidateFiler({
      candidateName: "Mary Van Dyke",
      districtNumber: 12,
      rows: [row({ filedName: "VAN DYKE MARY", district: "12" })],
    });
    expect(resolution).toMatchObject({ status: "matched", match: { surname: "VAN DYKE", firstName: "MARY" } });
  });
});
