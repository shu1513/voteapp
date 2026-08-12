import { describe, expect, it } from "vitest";
import {
  denverPersonNameMatchesCandidate,
  resolveDenverCandidateCommittees,
  type DenverRegistrantRecord,
} from "../../../src/pipeline/denverFinance/denverCandidateCommitteeResolver.js";

const CYCLE = 36;

// A clean registrant record modeled on live cycle-36 data (Jake Browne,
// filer 1326, committee 797, "Browne for Denver").
function record(
  overrides: {
    fullName?: string;
    filerId?: number;
    committeeId?: number;
    officeSought?: string | null;
    filer?: Partial<DenverRegistrantRecord["filer"]>;
    cycles?: number[];
    details?: Partial<DenverRegistrantRecord["details"]>;
  } = {},
): DenverRegistrantRecord {
  const filerId = overrides.filerId ?? 1326;
  const committeeId = overrides.committeeId ?? 797;
  const officeSought =
    overrides.officeSought === undefined
      ? "City Council At-Large Seat B"
      : overrides.officeSought;
  return {
    registrant: {
      fullName: overrides.fullName ?? "Jake Browne",
      firstName: null,
      middleName: null,
      lastName: null,
      officeSoughtId: 10,
      officeSought,
      district: null,
      committeeId,
      filerId,
    },
    filer: {
      filerId,
      filerTypeName: "Committee",
      filerStatusName: "New",
      isTerminated: false,
      committeeIds: [committeeId],
      independentExpenditureIds: [],
      ...overrides.filer,
    },
    cycles: (overrides.cycles ?? [CYCLE]).map((electionCycleId) => ({
      electionCycleId,
      name: `cycle ${electionCycleId}`,
      electionDate: null,
    })),
    details: {
      filerId,
      committeeId,
      committeeName: "Browne for Denver",
      committeeTypeId: 1,
      committeeType: "Candidate Committee",
      candidateName: overrides.fullName ?? "Jake Browne",
      office: officeSought,
      officeId: 10,
      electionCycleId: CYCLE,
      electionDate: "2026-11-03T07:00:00",
      ...overrides.details,
    },
  };
}

const candidate = {
  candidateId: "c1",
  displayName: "Jake Browne",
  electionYear: 2026,
  atLargeSeatLetter: "B",
};

function resolve(
  candidates: (typeof candidate)[],
  registrants: DenverRegistrantRecord[],
) {
  return resolveDenverCandidateCommittees({
    electionCycleId: CYCLE,
    candidates,
    registrants,
  });
}

describe("denverPersonNameMatchesCandidate", () => {
  it("matches token-wise with nickname expansion, never substrings", () => {
    expect(denverPersonNameMatchesCandidate("Jake Browne", "Jake Browne")).toBe(true);
    // Nickname expansion on the VoteApp side.
    expect(denverPersonNameMatchesCandidate("Mike Johnston", "Michael Johnston")).toBe(true);
    // Middle-evidence gate: conflicting middle initials never match.
    expect(denverPersonNameMatchesCandidate("Cynthia L Diaz", "Cynthia R Diaz")).toBe(false);
    // Surname containment is not a match.
    expect(denverPersonNameMatchesCandidate("Browne", "Jake Browne")).toBe(false);
    // Generational-suffix veto.
    expect(denverPersonNameMatchesCandidate("Jeff Walker Jr", "Jeff Walker Sr")).toBe(false);
  });
});

describe("resolveDenverCandidateCommittees", () => {
  it("matches a clean one-to-one registrant and carries the filer identity", () => {
    const [resolution] = resolve([candidate], [record()]);
    expect(resolution).toMatchObject({
      status: "matched",
      filerId: 1326,
      committeeName: "Browne for Denver",
      committeeEntityIds: [797],
    });
  });

  it("fails closed on a candidate without an at-large seat letter", () => {
    const [resolution] = resolve(
      [{ ...candidate, atLargeSeatLetter: null }],
      [record()],
    );
    expect(resolution).toMatchObject({
      status: "unmatched",
      reason: expect.stringContaining("no at-large seat letter"),
    });
  });

  it("does not match a same-named registrant for a different seat", () => {
    const [resolution] = resolve(
      [candidate],
      [record({ officeSought: "City Council At-Large Seat A" })],
    );
    expect(resolution).toMatchObject({ status: "unmatched" });
  });

  it("flags duplicate-name registrants as ambiguous (the Monica Martinez rule)", () => {
    const [resolution] = resolve(
      [{ ...candidate, displayName: "Monica Martinez" }],
      [
        record({ fullName: "Monica Martinez", filerId: 1322, committeeId: 806 }),
        record({ fullName: "Monica Martinez", filerId: 1328, committeeId: 799 }),
      ],
    );
    expect(resolution).toMatchObject({
      status: "ambiguous",
      reason: expect.stringContaining("1322, 1328"),
    });
  });

  it("blocks a registrant whose cycle list omits the cycle (filers 1329/1330)", () => {
    const [resolution] = resolve([candidate], [record({ cycles: [] })]);
    expect(resolution).toMatchObject({
      status: "unmatched",
      reason: expect.stringContaining("inconsistent registration"),
    });
  });

  it("blocks terminated filers and identity-echo mismatches", () => {
    expect(
      resolve([candidate], [record({ filer: { isTerminated: true } })])[0],
    ).toMatchObject({
      status: "unmatched",
      reason: expect.stringContaining("terminated"),
    });
    expect(
      resolve([candidate], [record({ filer: { filerId: 9999 } })])[0],
    ).toMatchObject({
      status: "unmatched",
      reason: expect.stringContaining("echoes 9999"),
    });
    expect(
      resolve([candidate], [record({ filer: { committeeIds: [123] } })])[0],
    ).toMatchObject({
      status: "unmatched",
      reason: expect.stringContaining("not on filer"),
    });
  });

  it("blocks committee details that reflect a different cycle (latest-registration drift)", () => {
    const [resolution] = resolve(
      [candidate],
      [record({ details: { electionCycleId: 33 } })],
    );
    expect(resolution).toMatchObject({
      status: "unmatched",
      reason: expect.stringContaining("latest-registration drift"),
    });
  });

  it("blocks non-candidate committee types, missing names, and office drift", () => {
    expect(
      resolve(
        [candidate],
        [record({ details: { committeeTypeId: 4, committeeType: "Independent Expenditure Committee" } })],
      )[0],
    ).toMatchObject({
      status: "unmatched",
      reason: expect.stringContaining("not a candidate committee"),
    });
    expect(
      resolve([candidate], [record({ details: { committeeName: null } })])[0],
    ).toMatchObject({
      status: "unmatched",
      reason: expect.stringContaining("no committee name"),
    });
    expect(
      resolve([candidate], [record({ details: { office: "Mayor" } })])[0],
    ).toMatchObject({
      status: "unmatched",
      reason: expect.stringContaining("disagrees with registration office"),
    });
  });

  it("fails one registrant claimed by two roster candidates closed for both", () => {
    // Two distinct roster entries whose names both match the same registrant
    // (call-name variant vs full name).
    const resolutions = resolve(
      [
        candidate,
        { ...candidate, candidateId: "c2", displayName: 'Jacob "Jake" Browne' },
      ],
      [record()],
    );
    expect(resolutions.map((resolution) => resolution.status)).toEqual([
      "ambiguous",
      "ambiguous",
    ]);
    expect(resolutions[0]).toMatchObject({
      reason: expect.stringContaining("multiple roster candidates"),
    });
  });

  it("reports no match when no registrant name-matches", () => {
    const [resolution] = resolve(
      [{ ...candidate, displayName: "Someone Else" }],
      [record()],
    );
    expect(resolution).toMatchObject({
      status: "unmatched",
      reason: "no cycle registrant name-matches",
    });
  });
});
