import { describe, expect, it } from "vitest";

import type {
  AlabamaCommitteeSearchRow,
  AlabamaRaceRow,
} from "../../../src/pipeline/alabamaFinance/alabamaFcpaClient.js";
import {
  alabamaDistrictNumberFromDistrictName,
  alabamaJurisdictionDistrict,
  resolveAlabamaCandidateRace,
} from "../../../src/pipeline/alabamaFinance/alabamaCandidateRaceResolver.js";

function raceRow(overrides: Partial<AlabamaRaceRow>): AlabamaRaceRow {
  return {
    COMMITTEEID: 7962,
    CANDIDATE: "Doug Jones",
    CANDIDATESTATUS: "Active",
    BEGINNINGFUNDS: 0,
    MONETARYCONTRIB: 100,
    MONETARYEXP: 40,
    NONMONETARYCONTRIB: 0,
    OTHERSOURCES: 0,
    ENDINGFUNDS: 60,
    YEAR: null,
    ...overrides,
  };
}

function committeeRow(overrides: Partial<AlabamaCommitteeSearchRow>): AlabamaCommitteeSearchRow {
  return {
    id: 7962,
    committeeId: "32837",
    candidateFirstName: "Doug",
    candidateLastName: "Jones",
    jurisdiction: null,
    ...overrides,
  };
}

function byId(rows: AlabamaCommitteeSearchRow[]): Map<number, AlabamaCommitteeSearchRow> {
  return new Map(rows.map((row) => [row.id, row]));
}

describe("alabamaDistrictNumberFromDistrictName / alabamaJurisdictionDistrict", () => {
  it("parses both district shapes", () => {
    expect(alabamaDistrictNumberFromDistrictName("State House District 68 (2024); Alabama")).toBe(68);
    expect(alabamaDistrictNumberFromDistrictName("Alabama")).toBeNull();
    expect(alabamaJurisdictionDistrict("HOUSE DISTRICT 68")).toEqual({ chamber: "HOUSE", district: 68 });
    expect(alabamaJurisdictionDistrict("SENATE DISTRICT 5")).toEqual({ chamber: "SENATE", district: 5 });
    expect(alabamaJurisdictionDistrict("MONTGOMERY COUNTY")).toBeNull();
    expect(alabamaJurisdictionDistrict(null)).toBeNull();
  });
});

describe("resolveAlabamaCandidateRace", () => {
  it("matches a statewide candidate by full name and carries both ids", () => {
    const resolution = resolveAlabamaCandidateRace({
      candidateName: "Doug Jones",
      raceRows: [raceRow({}), raceRow({ COMMITTEEID: 8000, CANDIDATE: "Tommy Tuberville" })],
      committeeRowsByInternalId: byId([committeeRow({})]),
      district: null,
    });
    expect(resolution).toMatchObject({
      status: "matched",
      internalCommitteeId: 7962,
      fcpaCommitteeNumber: "32837",
    });
  });

  it("matches Last, First ordering and generational suffixes", () => {
    const resolution = resolveAlabamaCandidateRace({
      candidateName: "Julius Walker, Jr.",
      raceRows: [raceRow({ CANDIDATE: "Walker, Julius Jr" })],
      committeeRowsByInternalId: byId([]),
      district: null,
    });
    expect(resolution).toMatchObject({ status: "matched", fcpaCommitteeNumber: null });
  });

  it("matches legal-name rows through the shared nickname table", () => {
    const resolution = resolveAlabamaCandidateRace({
      candidateName: "Tommy Tuberville",
      raceRows: [raceRow({ CANDIDATE: "TUBERVILLE, THOMAS H" })],
      committeeRowsByInternalId: byId([]),
      district: null,
    });
    expect(resolution).toMatchObject({ status: "matched" });
  });

  it("never matches on surname alone", () => {
    const resolution = resolveAlabamaCandidateRace({
      candidateName: "Doug Jones",
      raceRows: [raceRow({ CANDIDATE: "Mary Jones" })],
      committeeRowsByInternalId: byId([]),
      district: null,
    });
    expect(resolution).toEqual({ status: "unmatched", reason: "no_matching_race_row" });
  });

  it("tie-breaks to the single Active committee when every other row is a zero-money registration", () => {
    const resolution = resolveAlabamaCandidateRace({
      candidateName: "Doug Jones",
      raceRows: [
        raceRow({}),
        raceRow({
          COMMITTEEID: 9001,
          CANDIDATESTATUS: "Dissolved",
          MONETARYCONTRIB: 0,
          MONETARYEXP: 0,
          NONMONETARYCONTRIB: 0,
          OTHERSOURCES: 0,
          BEGINNINGFUNDS: 0,
          ENDINGFUNDS: 0,
        }),
      ],
      committeeRowsByInternalId: byId([]),
      district: null,
    });
    expect(resolution).toMatchObject({ status: "matched", internalCommitteeId: 7962 });
  });

  it("refuses the Active tie-break when a dissolved committee carries real money (Mendheim case)", () => {
    // Live 2026: Brad Mendheim's dissolved Supreme Court committee raised
    // $23,500 this cycle — auto-picking the Active row would undercount.
    const resolution = resolveAlabamaCandidateRace({
      candidateName: "Doug Jones",
      raceRows: [
        raceRow({}),
        raceRow({ COMMITTEEID: 9001, CANDIDATESTATUS: "Dissolved", MONETARYCONTRIB: 23_500, ENDINGFUNDS: 0 }),
      ],
      committeeRowsByInternalId: byId([]),
      district: null,
    });
    expect(resolution.status).toBe("ambiguous");
  });

  it("reports statewide same-name ambiguity when more than one committee is Active", () => {
    const resolution = resolveAlabamaCandidateRace({
      candidateName: "Doug Jones",
      raceRows: [raceRow({}), raceRow({ COMMITTEEID: 9001 })],
      committeeRowsByInternalId: byId([]),
      district: null,
    });
    expect(resolution.status).toBe("ambiguous");
  });

  it("confirms a legislative match through the jurisdiction join", () => {
    const resolution = resolveAlabamaCandidateRace({
      candidateName: "Doug Jones",
      raceRows: [raceRow({}), raceRow({ COMMITTEEID: 9001, CANDIDATE: "Doug Jones" })],
      committeeRowsByInternalId: byId([
        committeeRow({ jurisdiction: "HOUSE DISTRICT 68" }),
        committeeRow({ id: 9001, committeeId: "40000", jurisdiction: "HOUSE DISTRICT 12" }),
      ]),
      district: { chamber: "HOUSE", district: 68 },
    });
    expect(resolution).toMatchObject({ status: "matched", internalCommitteeId: 7962 });
  });

  it("requires the chamber to match, not just the number", () => {
    const resolution = resolveAlabamaCandidateRace({
      candidateName: "Doug Jones",
      raceRows: [raceRow({})],
      committeeRowsByInternalId: byId([committeeRow({ jurisdiction: "SENATE DISTRICT 68" })]),
      district: { chamber: "HOUSE", district: 68 },
    });
    expect(resolution).toMatchObject({ status: "manual_confirm_required", reason: "district_mismatch" });
  });

  it("fails closed when the jurisdiction is missing on a districted office", () => {
    const resolution = resolveAlabamaCandidateRace({
      candidateName: "Doug Jones",
      raceRows: [raceRow({})],
      committeeRowsByInternalId: byId([committeeRow({ jurisdiction: null })]),
      district: { chamber: "HOUSE", district: 68 },
    });
    expect(resolution).toMatchObject({
      status: "manual_confirm_required",
      reason: "missing_jurisdiction",
    });
  });

  it("fails closed when the committee-search row is missing entirely", () => {
    const resolution = resolveAlabamaCandidateRace({
      candidateName: "Doug Jones",
      raceRows: [raceRow({})],
      committeeRowsByInternalId: byId([]),
      district: { chamber: "HOUSE", district: 68 },
    });
    expect(resolution).toMatchObject({
      status: "manual_confirm_required",
      reason: "missing_committee_row",
    });
  });

  it("distrusts a confirmed row while a same-named row could not be district-checked", () => {
    const resolution = resolveAlabamaCandidateRace({
      candidateName: "Doug Jones",
      raceRows: [raceRow({}), raceRow({ COMMITTEEID: 9001, CANDIDATE: "Doug Jones" })],
      committeeRowsByInternalId: byId([
        committeeRow({ jurisdiction: "HOUSE DISTRICT 68" }),
        committeeRow({ id: 9001, committeeId: "40000", jurisdiction: null }),
      ]),
      district: { chamber: "HOUSE", district: 68 },
    });
    expect(resolution.status).toBe("manual_confirm_required");
  });
});
