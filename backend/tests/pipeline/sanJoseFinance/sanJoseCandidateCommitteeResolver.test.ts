import { describe, expect, it } from "vitest";

import {
  collectSanJoseExportCommittees,
  resolveSanJoseCandidateCommittees,
  SAN_JOSE_PENDING_FILER_ID,
  type SanJoseAppCandidate,
  type SanJoseExportCommittee,
} from "../../../src/pipeline/sanJoseFinance/sanJoseCandidateCommitteeResolver.js";

function candidate(overrides: Partial<SanJoseAppCandidate>): SanJoseAppCandidate {
  return {
    candidateId: "cand-1",
    displayName: "Peter Ortiz",
    officeName: "City Council Member",
    seatNumber: 5,
    electionYear: 2026,
    stateFilingIds: [],
    ...overrides,
  };
}

function committee(overrides: Partial<SanJoseExportCommittee>): SanJoseExportCommittee {
  return {
    filerId: "1480385",
    committeeNames: ["Peter Ortiz for San Jose City Council District 5 2026"],
    committeeTypes: ["C"],
    ...overrides,
  };
}

function resolveOne(
  appCandidate: SanJoseAppCandidate,
  committees: readonly SanJoseExportCommittee[],
) {
  const resolutions = resolveSanJoseCandidateCommittees({
    candidates: [appCandidate],
    committees,
  });
  expect(resolutions).toHaveLength(1);
  return resolutions[0]!;
}

describe("collectSanJoseExportCommittees", () => {
  it("groups rows by Filer_ID, collecting name variants and types", () => {
    const committees = collectSanJoseExportCommittees([
      { filerId: "1480385", filerName: "Peter Ortiz for San Jose City Council District 5 2026", cmtteType: "C" },
      { filerId: "1480385", filerName: "Peter Ortiz for San José City Council District 5 2026", cmtteType: "C" },
      { filerId: "1480385", filerName: "Peter Ortiz for San Jose City Council District 5 2026", cmtteType: null },
      { filerId: "1487316", filerName: "South Bay Working Families Supporting Ortiz for City Council 2026", cmtteType: "P" },
    ]);
    expect(committees).toEqual([
      {
        filerId: "1480385",
        committeeNames: [
          "Peter Ortiz for San Jose City Council District 5 2026",
          "Peter Ortiz for San José City Council District 5 2026",
        ],
        committeeTypes: ["C"],
      },
      {
        filerId: "1487316",
        committeeNames: ["South Bay Working Families Supporting Ortiz for City Council 2026"],
        committeeTypes: ["P"],
      },
    ]);
  });

  it("never collapses two Pending committees into one identity", () => {
    const committees = collectSanJoseExportCommittees([
      { filerId: SAN_JOSE_PENDING_FILER_ID, filerName: "Alpha for City Council 2026", cmtteType: "C" },
      { filerId: SAN_JOSE_PENDING_FILER_ID, filerName: "Beta for City Council 2026", cmtteType: "C" },
    ]);
    expect(committees).toHaveLength(2);
    expect(committees.every((entry) => entry.filerId === SAN_JOSE_PENDING_FILER_ID)).toBe(true);
  });
});

describe("resolveSanJoseCandidateCommittees", () => {
  it("links the C committee and never the P committee carrying the candidate's name (Ortiz)", () => {
    const resolution = resolveOne(candidate({}), [
      committee({}),
      committee({
        filerId: "1487316",
        committeeNames: ["South Bay Working Families Supporting Ortiz for City Council 2026"],
        committeeTypes: ["P"],
      }),
      // Even a P committee whose name IS the candidate's name must not link.
      committee({
        filerId: "9999001",
        committeeNames: ["Peter Ortiz for City Council 2026"],
        committeeTypes: ["P"],
      }),
    ]);
    expect(resolution).toMatchObject({ status: "matched", filerId: "1480385", matchedBy: "name" });
  });

  it("resolves district-less committee names (Campos, Van Le)", () => {
    expect(
      resolveOne(candidate({ displayName: "Nora Campos", seatNumber: 5 }), [
        committee({ filerId: "1111111", committeeNames: ["Nora Campos for San Jose City Council 2026"] }),
      ]),
    ).toMatchObject({ status: "matched", filerId: "1111111" });
    expect(
      resolveOne(candidate({ displayName: "Van Le", seatNumber: 7 }), [
        committee({ filerId: "2222222", committeeNames: ["Van Le for City Council 2026"] }),
      ]),
    ).toMatchObject({ status: "matched", filerId: "2222222" });
  });

  it("short surnames never substring-match unrelated committees (Le vs Electrical/Valley)", () => {
    const resolution = resolveOne(candidate({ displayName: "Van Le", seatNumber: 7 }), [
      // Deliberately typed C so the NAME gate is what rejects, not the type gate.
      committee({ filerId: "3000001", committeeNames: ["IBEW Local 332 Electrical Workers PAC"] }),
      committee({ filerId: "3000002", committeeNames: ["COMMON GOOD SILICON VALLEY"] }),
      committee({ filerId: "3000003", committeeNames: ["Silicon Valley Biz PAC"] }),
    ]);
    expect(resolution.status).toBe("unmatched");
  });

  it("never cross-links same-surname different people (Nora vs Pamela Campos)", () => {
    const resolution = resolveOne(candidate({ displayName: "Nora Campos", seatNumber: 5 }), [
      committee({
        filerId: "4000001",
        committeeNames: ["Pamela Campos for San Jose City Council District 2 2024"],
      }),
    ]);
    expect(resolution.status).toBe("unmatched");
  });

  it("matches through accented and whitespace name variants of one committee", () => {
    const resolution = resolveOne(candidate({ displayName: "Bien Doan", seatNumber: 7 }), [
      committee({
        filerId: "5000001",
        committeeNames: [
          "Bien Doan for San José City Council D7 2026",
          "Bien Doan for San Jose City Council D7 2026",
        ],
      }),
    ]);
    expect(resolution).toMatchObject({ status: "matched", filerId: "5000001" });
  });

  it("vetoes on conflicting district evidence when the name carries one", () => {
    const resolution = resolveOne(candidate({ displayName: "Gordon Chester", seatNumber: 9 }), [
      committee({
        filerId: "6000001",
        committeeNames: ["Gordon Chester for San Jose City Council District 5 2026"],
      }),
    ]);
    expect(resolution.status).toBe("unmatched");
  });

  it("vetoes on conflicting election year when the name carries one", () => {
    const resolution = resolveOne(candidate({ displayName: "Bien Doan", seatNumber: 7 }), [
      committee({ filerId: "7000001", committeeNames: ["Bien Doan for San Jose City Council D7 2024"] }),
    ]);
    expect(resolution.status).toBe("unmatched");
  });

  it("vetoes cross-office and foreign-office committee names (Cohen Senate case)", () => {
    // A council candidate's own state-senate committee files copies with the city.
    expect(
      resolveOne(candidate({ displayName: "David Cohen", seatNumber: 10 }), [
        committee({
          filerId: "8000001",
          committeeNames: ["David Cohen for California Senate District 10 2026"],
        }),
      ]).status,
    ).toBe("unmatched");
    // Council name evidence never links to a mayoral candidacy and vice versa.
    expect(
      resolveOne(
        candidate({ displayName: "Sam Liccardo", officeName: "Mayor", seatNumber: null }),
        [committee({ filerId: "8000002", committeeNames: ["Sam Liccardo for City Council 2026"] })],
      ).status,
    ).toBe("unmatched");
    expect(
      resolveOne(candidate({ displayName: "Sam Liccardo", seatNumber: 3 }), [
        committee({ filerId: "8000003", committeeNames: ["Sam Liccardo for Mayor 2026"] }),
      ]).status,
    ).toBe("unmatched");
  });

  it("never auto-links a Pending filer id and says why", () => {
    const resolution = resolveOne(candidate({}), [
      committee({
        filerId: SAN_JOSE_PENDING_FILER_ID,
        committeeNames: ["Peter Ortiz for San Jose City Council District 5 2026"],
      }),
    ]);
    expect(resolution).toMatchObject({ status: "unmatched" });
    expect(resolution.status === "unmatched" && resolution.reason).toContain("Pending");
  });

  it("fails closed on unknown or conflicting committee type codes", () => {
    const unknownType = resolveOne(candidate({}), [
      committee({ filerId: "9100001", committeeTypes: ["X"] }),
    ]);
    expect(unknownType).toMatchObject({ status: "unmatched" });
    expect(unknownType.status === "unmatched" && unknownType.reason).toContain("Cmtte_Type");

    const conflictingTypes = resolveOne(candidate({}), [
      committee({ filerId: "9100002", committeeTypes: ["C", "G"] }),
    ]);
    expect(conflictingTypes).toMatchObject({ status: "unmatched" });

    const noTypes = resolveOne(candidate({}), [committee({ filerId: "9100003", committeeTypes: [] })]);
    expect(noTypes).toMatchObject({ status: "unmatched" });
  });

  it("a blocked name-match makes a sibling linkable match uncertain too", () => {
    const resolution = resolveOne(candidate({}), [
      committee({}),
      committee({
        filerId: "9100004",
        committeeNames: ["Peter Ortiz for City Council 2026"],
        committeeTypes: ["X"],
      }),
    ]);
    expect(resolution.status).toBe("unmatched");
  });

  it("two linkable committees for one candidate are ambiguous", () => {
    const resolution = resolveOne(candidate({}), [
      committee({ filerId: "9200001", committeeNames: ["Peter Ortiz for City Council 2026"] }),
      committee({ filerId: "9200002", committeeNames: ["Peter Ortiz for San Jose City Council 2026"] }),
    ]);
    expect(resolution.status).toBe("ambiguous");
  });

  it("one committee matching two candidates fails both closed", () => {
    const resolutions = resolveSanJoseCandidateCommittees({
      candidates: [
        candidate({ candidateId: "cand-1", displayName: "Peter Ortiz" }),
        candidate({ candidateId: "cand-2", displayName: "Peter Ortiz" }),
      ],
      committees: [committee({})],
    });
    expect(resolutions.map((resolution) => resolution.status)).toEqual(["ambiguous", "ambiguous"]);
  });

  it("prefers a stored FPPC id over name evidence", () => {
    const resolution = resolveOne(candidate({ stateFilingIds: ["1480385"] }), [
      committee({ committeeNames: ["Re-Elect Committee With Legal Name Only"] }),
    ]);
    expect(resolution).toMatchObject({ status: "matched", filerId: "1480385", matchedBy: "fppc_id" });
  });

  it("a stored FPPC id never overrides contradictory name evidence (Cohen Senate case)", () => {
    // state_filing_ids accumulates across the person's races: a council
    // candidate can carry their own state-senate committee's id, and that
    // committee files copies with the city. The id tier must not book
    // Senate money to the council race.
    const senateCommittee = committee({
      filerId: "8000001",
      committeeNames: ["David Cohen for California Senate District 10 2026"],
    });
    const cohen = candidate({
      displayName: "David Cohen",
      seatNumber: 10,
      stateFilingIds: ["8000001"],
    });
    expect(resolveOne(cohen, [senateCommittee]).status).toBe("unmatched");
    // The vetoed id falls through to the name tier, which still links the
    // real council committee.
    const resolution = resolveOne(cohen, [
      senateCommittee,
      committee({ filerId: "8100001", committeeNames: ["David Cohen for San Jose City Council District 10 2026"] }),
    ]);
    expect(resolution).toMatchObject({ status: "matched", filerId: "8100001", matchedBy: "name" });
  });

  it("a stored FPPC id never overrides a conflicting district, office, or year", () => {
    expect(
      resolveOne(candidate({ displayName: "Gordon Chester", seatNumber: 9, stateFilingIds: ["6000001"] }), [
        committee({ filerId: "6000001", committeeNames: ["Gordon Chester for San Jose City Council District 5 2026"] }),
      ]).status,
    ).toBe("unmatched");
    expect(
      resolveOne(candidate({ displayName: "Sam Liccardo", seatNumber: 3, stateFilingIds: ["8000003"] }), [
        committee({ filerId: "8000003", committeeNames: ["Sam Liccardo for Mayor 2026"] }),
      ]).status,
    ).toBe("unmatched");
    expect(
      resolveOne(candidate({ displayName: "Bien Doan", seatNumber: 7, stateFilingIds: ["7000001"] }), [
        committee({ filerId: "7000001", committeeNames: ["Bien Doan for San Jose City Council D7 2024"] }),
      ]).status,
    ).toBe("unmatched");
  });

  it("an FPPC id pointing at a non-C committee fails closed", () => {
    const resolution = resolveOne(candidate({ stateFilingIds: ["1487316"] }), [
      committee({
        filerId: "1487316",
        committeeNames: ["South Bay Working Families Supporting Ortiz for City Council 2026"],
        committeeTypes: ["P"],
      }),
    ]);
    expect(resolution).toMatchObject({ status: "unmatched" });
  });

  it("nickname variants match on the VoteApp side only", () => {
    const resolution = resolveOne(candidate({ displayName: "Robert Garcia", seatNumber: 5 }), [
      committee({ filerId: "9300001", committeeNames: ["Bob Garcia for City Council 2026"] }),
    ]);
    expect(resolution).toMatchObject({ status: "matched", filerId: "9300001" });
  });

  it("generational suffix conflicts reject (Jr vs Sr)", () => {
    const resolution = resolveOne(candidate({ displayName: "John Smith Jr." }), [
      committee({ filerId: "9400001", committeeNames: ["John Smith Sr. for City Council 2026"] }),
    ]);
    expect(resolution.status).toBe("unmatched");
  });

  it("quoted call names in roster display names still match", () => {
    const resolution = resolveOne(candidate({ displayName: 'Emanuel "Manny" Yekutiel', seatNumber: 5 }), [
      committee({ filerId: "9500001", committeeNames: ["Manny Yekutiel for City Council 2026"] }),
    ]);
    expect(resolution).toMatchObject({ status: "matched", filerId: "9500001" });
  });

  it("fails closed when a council candidate has no valid seat number", () => {
    const resolution = resolveOne(candidate({ seatNumber: null }), [committee({})]);
    expect(resolution).toMatchObject({ status: "unmatched" });
  });
});
