import { describe, expect, it } from "vitest";

import {
  collectSanDiegoCityExportCommittees,
  resolveSanDiegoCityCandidateCommittees,
  sanDiegoCityPersonNameMatchesCandidate,
  SAN_DIEGO_CITY_CLERK_LOG_COMMITTEES,
  type SanDiegoCityAppCandidate,
  type SanDiegoCityClerkLogCommittee,
  type SanDiegoCityExportCommittee,
} from "../../../src/pipeline/sanDiegoCityFinance/sanDiegoCityCandidateCommitteeResolver.js";
import { SAN_DIEGO_PENDING_FILER_ID } from "../../../src/pipeline/sanDiegoCityFinance/sanDiegoCityFinanceWriter.js";

function candidate(overrides: Partial<SanDiegoCityAppCandidate>): SanDiegoCityAppCandidate {
  return {
    candidateId: "cand-1",
    displayName: "Antonio Martinez",
    officeName: "City Council Member",
    seatNumber: 8,
    electionYear: 2026,
    stateFilingIds: [],
    ...overrides,
  };
}

function committee(overrides: Partial<SanDiegoCityExportCommittee>): SanDiegoCityExportCommittee {
  return {
    filerId: "1460125",
    committeeNames: ["Antonio Martinez for City Council 2026"],
    committeeTypes: ["C"],
    ...overrides,
  };
}

function resolveOne(
  appCandidate: SanDiegoCityAppCandidate,
  committees: readonly SanDiegoCityExportCommittee[],
  clerkLogCommittees?: readonly SanDiegoCityClerkLogCommittee[],
) {
  const resolutions = resolveSanDiegoCityCandidateCommittees({
    candidates: [appCandidate],
    committees,
    clerkLogCommittees,
  });
  expect(resolutions).toHaveLength(1);
  return resolutions[0]!;
}

describe("collectSanDiegoCityExportCommittees", () => {
  it("groups rows by Filer_ID, collecting name variants and types", () => {
    const committees = collectSanDiegoCityExportCommittees([
      { filerId: "1460125", filerName: "Antonio Martinez for City Council 2026", cmtteType: "C" },
      { filerId: "1460125", filerName: "Antonio  Martinez for City Council 2026", cmtteType: "C" },
      { filerId: "1460125", filerName: "Antonio Martinez for City Council 2026", cmtteType: null },
      { filerId: "1490398", filerName: "WORKING FAMILIES SUPPORTING GERARDO RAMIREZ", cmtteType: "P" },
    ]);
    expect(committees).toEqual([
      {
        filerId: "1460125",
        committeeNames: [
          "Antonio  Martinez for City Council 2026",
          "Antonio Martinez for City Council 2026",
        ],
        committeeTypes: ["C"],
      },
      {
        filerId: "1490398",
        committeeNames: ["WORKING FAMILIES SUPPORTING GERARDO RAMIREZ"],
        committeeTypes: ["P"],
      },
    ]);
  });

  it("never collapses two Pending committees into one identity", () => {
    const committees = collectSanDiegoCityExportCommittees([
      { filerId: SAN_DIEGO_PENDING_FILER_ID, filerName: "Alpha for City Council 2026", cmtteType: "C" },
      { filerId: SAN_DIEGO_PENDING_FILER_ID, filerName: "Beta for City Council 2026", cmtteType: "C" },
    ]);
    expect(committees).toHaveLength(2);
    expect(committees.every((entry) => entry.filerId === SAN_DIEGO_PENDING_FILER_ID)).toBe(true);
  });
});

describe("resolveSanDiegoCityCandidateCommittees clerk-log tier", () => {
  // The real Foster/Lee/Powell committees, exactly as the export reports them
  // (Phase 0 probe gate 5): "Re-Elect X…" and surname-only names defeat the
  // name tier by design, so only the clerk-log tier can link these.
  const fosterExport = committee({
    filerId: "1481166",
    committeeNames: ["Re-Elect Henry Foster III for San Diego City Council 2026"],
  });
  const leeExport = committee({
    filerId: "1478315",
    committeeNames: ["Re-Elect Kent Lee for City Council 2026"],
  });
  const powellExport = committee({
    filerId: "1485884",
    committeeNames: ["POWELL FOR CITY COUNCIL 2026"],
  });

  it("resolves the three curated November candidates via the default table", () => {
    const resolutions = resolveSanDiegoCityCandidateCommittees({
      candidates: [
        candidate({ candidateId: "foster", displayName: "Henry Foster III", seatNumber: 4 }),
        candidate({ candidateId: "lee", displayName: "Kent Lee", seatNumber: 6 }),
        candidate({ candidateId: "powell", displayName: "Mark Powell", seatNumber: 6 }),
      ],
      committees: [fosterExport, leeExport, powellExport],
    });
    expect(resolutions).toMatchObject([
      { status: "matched", filerId: "1481166", matchedBy: "clerk_log" },
      { status: "matched", filerId: "1478315", matchedBy: "clerk_log" },
      { status: "matched", filerId: "1485884", matchedBy: "clerk_log" },
    ]);
  });

  it("the name tier alone cannot resolve the curated candidates", () => {
    const resolution = resolveOne(
      candidate({ displayName: "Mark Powell", seatNumber: 6 }),
      [powellExport],
      [],
    );
    expect(resolution.status).toBe("unmatched");
  });

  it("fails closed when the clerk-log committee is missing from the export", () => {
    const resolution = resolveOne(
      candidate({ displayName: "Mark Powell", seatNumber: 6 }),
      [],
    );
    expect(resolution).toMatchObject({ status: "unmatched" });
    expect(resolution.status === "unmatched" && resolution.reason).toContain("not in the export");
  });

  it("fails closed when the export disagrees on the committee name", () => {
    const resolution = resolveOne(
      candidate({ displayName: "Mark Powell", seatNumber: 6 }),
      [committee({ filerId: "1485884", committeeNames: ["Some Other Committee Entirely"] })],
    );
    expect(resolution).toMatchObject({ status: "unmatched" });
    expect(resolution.status === "unmatched" && resolution.reason).toContain("does not match");
  });

  it("fails closed when the clerk-log committee is not a lone C", () => {
    const resolution = resolveOne(
      candidate({ displayName: "Mark Powell", seatNumber: 6 }),
      [committee({ filerId: "1485884", committeeNames: ["POWELL FOR CITY COUNCIL 2026"], committeeTypes: ["P"] })],
    );
    expect(resolution).toMatchObject({ status: "unmatched" });
    expect(resolution.status === "unmatched" && resolution.reason).toContain("not a lone C");
  });

  it("fails closed on contradictory name evidence instead of trusting the entry", () => {
    const entry: SanDiegoCityClerkLogCommittee = {
      candidateNameKey: "MARK POWELL",
      seatNumber: 6,
      electionYear: 2026,
      filerId: "9990001",
      committeeName: "Powell for City Council District 3 2026",
      clerkGuid: "test-guid",
    };
    const resolution = resolveOne(
      candidate({ displayName: "Mark Powell", seatNumber: 6 }),
      [committee({ filerId: "9990001", committeeNames: ["Powell for City Council District 3 2026"] })],
      [entry],
    );
    expect(resolution).toMatchObject({ status: "unmatched" });
    expect(resolution.status === "unmatched" && resolution.reason).toContain("contradictory name evidence");
  });

  it("only applies to the exact contest it was curated for", () => {
    // Same person, different seat/year: the entry is skipped and the fully
    // gated name tier still runs (here it links a plain-name committee).
    const resolution = resolveOne(
      candidate({ displayName: "Mark Powell", seatNumber: 2, electionYear: 2030 }),
      [committee({ filerId: "7770001", committeeNames: ["Mark Powell for City Council 2030"] })],
    );
    expect(resolution).toMatchObject({ status: "matched", filerId: "7770001", matchedBy: "name" });
  });

  it("two clerk-log entries for one contest are a curation error", () => {
    const entry: SanDiegoCityClerkLogCommittee = {
      candidateNameKey: "MARK POWELL",
      seatNumber: 6,
      electionYear: 2026,
      filerId: "1111111",
      committeeName: "Powell Committee A",
      clerkGuid: "guid-a",
    };
    const resolution = resolveOne(
      candidate({ displayName: "Mark Powell", seatNumber: 6 }),
      [powellExport],
      [...SAN_DIEGO_CITY_CLERK_LOG_COMMITTEES, entry],
    );
    expect(resolution).toMatchObject({ status: "ambiguous" });
    expect(resolution.status === "ambiguous" && resolution.reason).toContain("clerk-log entries");
  });

  it("a clerk-log match shared with a name-tier match fails both closed", () => {
    // The final one-committee-two-candidates pass must see clerk_log matches.
    const resolutions = resolveSanDiegoCityCandidateCommittees({
      candidates: [
        candidate({ candidateId: "powell", displayName: "Mark Powell", seatNumber: 6 }),
        candidate({ candidateId: "other", displayName: "Powell Cityco", seatNumber: 6 }),
      ],
      committees: [powellExport],
      clerkLogCommittees: [
        ...SAN_DIEGO_CITY_CLERK_LOG_COMMITTEES,
        {
          candidateNameKey: "POWELL CITYCO",
          seatNumber: 6,
          electionYear: 2026,
          filerId: "1485884",
          committeeName: "POWELL FOR CITY COUNCIL 2026",
          clerkGuid: "guid-dup",
        },
      ],
    });
    expect(resolutions.map((resolution) => resolution.status)).toEqual(["ambiguous", "ambiguous"]);
  });
});

describe("resolveSanDiegoCityCandidateCommittees", () => {
  it("links the C committee and never the P committee carrying the candidate's name (Ramirez)", () => {
    const resolution = resolveOne(candidate({ displayName: "Gerardo Ramirez" }), [
      committee({ filerId: "1489001", committeeNames: ["Gerardo Ramirez for City Council 2026"] }),
      committee({
        filerId: "1490398",
        committeeNames: ["WORKING FAMILIES SUPPORTING GERARDO RAMIREZ"],
        committeeTypes: ["P"],
      }),
      // Even a P committee whose name IS the candidate's name must not link.
      committee({
        filerId: "9999001",
        committeeNames: ["Gerardo Ramirez for City Council 2026"],
        committeeTypes: ["P"],
      }),
    ]);
    expect(resolution).toMatchObject({ status: "matched", filerId: "1489001", matchedBy: "name" });
  });

  it("short surnames never substring-match unrelated committees", () => {
    const resolution = resolveOne(candidate({ displayName: "Kent Lee", seatNumber: 6 }), [
      // Deliberately typed C so the NAME gate is what rejects, not the type gate.
      committee({ filerId: "3000001", committeeNames: ["IBEW Electrical Workers PAC"] }),
      committee({ filerId: "3000002", committeeNames: ["Mission Valley Biz PAC"] }),
    ]);
    expect(resolution.status).toBe("unmatched");
  });

  it("never cross-links same-surname different people (the $200k Ramirez trap)", () => {
    const resolution = resolveOne(candidate({ displayName: "Gerardo Ramirez", seatNumber: 8 }), [
      committee({
        filerId: "4000001",
        committeeNames: ["Maria Ramirez for San Diego City Council District 1 2024"],
      }),
    ]);
    expect(resolution.status).toBe("unmatched");
  });

  it("vetoes on conflicting district evidence when the name carries one", () => {
    const resolution = resolveOne(candidate({ displayName: "Antonio Martinez", seatNumber: 8 }), [
      committee({
        filerId: "6000001",
        committeeNames: ["Antonio Martinez for San Diego City Council District 5 2026"],
      }),
    ]);
    expect(resolution.status).toBe("unmatched");
  });

  it("vetoes on conflicting election year when the name carries one", () => {
    const resolution = resolveOne(candidate({ displayName: "Antonio Martinez", seatNumber: 8 }), [
      committee({ filerId: "7000001", committeeNames: ["Antonio Martinez for City Council D8 2024"] }),
    ]);
    expect(resolution.status).toBe("unmatched");
  });

  it("vetoes cross-office and foreign-office committee names, including City Attorney", () => {
    // A candidate's committee for a DIFFERENT office files copies with the city.
    expect(
      resolveOne(candidate({ displayName: "Antonio Martinez", seatNumber: 8 }), [
        committee({
          filerId: "8000001",
          committeeNames: ["Antonio Martinez for California Senate District 18 2026"],
        }),
      ]).status,
    ).toBe("unmatched");
    // Municipal Attorney is outside the Phase 2 whitelist — ATTORNEY vetoes.
    expect(
      resolveOne(candidate({ displayName: "Antonio Martinez", seatNumber: 8 }), [
        committee({ filerId: "8000004", committeeNames: ["Antonio Martinez for City Attorney 2026"] }),
      ]).status,
    ).toBe("unmatched");
    // Council name evidence never links to a mayoral candidacy and vice versa.
    expect(
      resolveOne(
        candidate({ displayName: "Antonio Martinez", officeName: "Mayor", seatNumber: null }),
        [committee({ filerId: "8000002", committeeNames: ["Antonio Martinez for City Council 2026"] })],
      ).status,
    ).toBe("unmatched");
    expect(
      resolveOne(candidate({ displayName: "Antonio Martinez", seatNumber: 8 }), [
        committee({ filerId: "8000003", committeeNames: ["Antonio Martinez for Mayor 2026"] }),
      ]).status,
    ).toBe("unmatched");
  });

  it("never auto-links a Pending filer id and says why", () => {
    const resolution = resolveOne(candidate({}), [
      committee({
        filerId: SAN_DIEGO_PENDING_FILER_ID,
        committeeNames: ["Antonio Martinez for City Council 2026"],
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
        committeeNames: ["Antonio Martinez for City Council 2026"],
        committeeTypes: ["X"],
      }),
    ]);
    expect(resolution.status).toBe("unmatched");
  });

  it("two linkable committees for one candidate are ambiguous", () => {
    const resolution = resolveOne(candidate({}), [
      committee({ filerId: "9200001", committeeNames: ["Antonio Martinez for City Council 2026"] }),
      committee({ filerId: "9200002", committeeNames: ["Antonio Martinez for San Diego City Council 2026"] }),
    ]);
    expect(resolution.status).toBe("ambiguous");
  });

  it("one committee matching two candidates fails both closed", () => {
    const resolutions = resolveSanDiegoCityCandidateCommittees({
      candidates: [
        candidate({ candidateId: "cand-1", displayName: "Antonio Martinez" }),
        candidate({ candidateId: "cand-2", displayName: "Antonio Martinez" }),
      ],
      committees: [committee({})],
    });
    expect(resolutions.map((resolution) => resolution.status)).toEqual(["ambiguous", "ambiguous"]);
  });

  it("prefers a stored FPPC id over name evidence", () => {
    const resolution = resolveOne(candidate({ stateFilingIds: ["1460125"] }), [
      committee({ committeeNames: ["Committee With Legal Name Only"] }),
    ]);
    expect(resolution).toMatchObject({ status: "matched", filerId: "1460125", matchedBy: "fppc_id" });
  });

  it("a stored FPPC id never overrides contradictory name evidence", () => {
    // state_filing_ids accumulates across the person's races: a council
    // candidate can carry their own state-senate committee's id, and that
    // committee files copies with the city. The id tier must not book
    // Senate money to the council race.
    const senateCommittee = committee({
      filerId: "8000001",
      committeeNames: ["Antonio Martinez for California Senate District 18 2026"],
    });
    const martinez = candidate({ stateFilingIds: ["8000001"] });
    expect(resolveOne(martinez, [senateCommittee]).status).toBe("unmatched");
    // The vetoed id falls through to the name tier, which still links the
    // real council committee.
    const resolution = resolveOne(martinez, [
      senateCommittee,
      committee({ filerId: "8100001", committeeNames: ["Antonio Martinez for San Diego City Council District 8 2026"] }),
    ]);
    expect(resolution).toMatchObject({ status: "matched", filerId: "8100001", matchedBy: "name" });
  });

  it("a stored FPPC id never overrides a conflicting district, office, or year", () => {
    expect(
      resolveOne(candidate({ seatNumber: 8, stateFilingIds: ["6000001"] }), [
        committee({ filerId: "6000001", committeeNames: ["Antonio Martinez for San Diego City Council District 5 2026"] }),
      ]).status,
    ).toBe("unmatched");
    expect(
      resolveOne(candidate({ seatNumber: 8, stateFilingIds: ["8000003"] }), [
        committee({ filerId: "8000003", committeeNames: ["Antonio Martinez for Mayor 2026"] }),
      ]).status,
    ).toBe("unmatched");
    expect(
      resolveOne(candidate({ seatNumber: 8, stateFilingIds: ["7000001"] }), [
        committee({ filerId: "7000001", committeeNames: ["Antonio Martinez for City Council D8 2024"] }),
      ]).status,
    ).toBe("unmatched");
  });

  it("an FPPC id pointing at a non-C committee fails closed", () => {
    const resolution = resolveOne(candidate({ displayName: "Gerardo Ramirez", stateFilingIds: ["1490398"] }), [
      committee({
        filerId: "1490398",
        committeeNames: ["WORKING FAMILIES SUPPORTING GERARDO RAMIREZ"],
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

  it("treats a bare V as a middle initial, not a generational suffix", () => {
    // Bare "V" is a middle initial, not a suffix (the shared
    // GENERATIONAL_SUFFIX_RANK policy deliberately excludes it), so it must
    // stay as middle evidence on either side instead of being stripped.
    expect(sanDiegoCityPersonNameMatchesCandidate("Smith, John B.", "John V. Smith")).toBe(false);
    expect(sanDiegoCityPersonNameMatchesCandidate("Smith, John V", "John B. Smith")).toBe(false);
    expect(sanDiegoCityPersonNameMatchesCandidate("Smith, John V", "John V. Smith")).toBe(true);
    expect(sanDiegoCityPersonNameMatchesCandidate("Smith, John V", "John Smith")).toBe(true);
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
    // San Diego has nine districts; SJ's District 10 is invalid here.
    const outOfRange = resolveOne(candidate({ seatNumber: 10 }), [committee({})]);
    expect(outOfRange).toMatchObject({ status: "unmatched" });
  });
});
