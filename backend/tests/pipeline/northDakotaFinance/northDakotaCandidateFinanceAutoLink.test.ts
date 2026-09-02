import { describe, expect, it, vi } from "vitest";

import type { NorthDakotaCommitteeRow } from "../../../src/pipeline/northDakotaFinance/northDakotaCfrsClient.js";
import { autoLinkMissingNorthDakotaCandidateFinanceLinks } from "../../../src/pipeline/northDakotaFinance/northDakotaCandidateFinanceAutoLink.js";

const LINK_ID = "33333333-3333-4333-8333-333333333333";

function committee(overrides: Partial<NorthDakotaCommitteeRow>): NorthDakotaCommitteeRow {
  return {
    orgID: 1478,
    entityId: "1010001478",
    orgName: "Friends of Jamie Selzler",
    candidateName: "Mr. Selzler, Jamie",
    orgType: "Candidate/Candidate Committee",
    orgTypeCode: "101",
    orgSubType: "Candidate Committee",
    orgSubTypeCode: "CNCM",
    election: "2026 Election - Statewide",
    office: "State Senator",
    district: "District 44",
    party: "North Dakota Democratic-NPL Party",
    orgStatus: "Active",
    registrationYear: "2026",
    ...overrides,
  };
}

function candidateElection(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    candidateId: "candidate-1",
    electionId: "election-1",
    candidateName: "Jamie Selzler",
    electionYear: 2026,
    officeScope: "state_upper",
    officeName: "State Senator",
    district: "State Senate District 44 (2024); North Dakota",
    ...overrides,
  } as never;
}

function linkDb() {
  const query = vi.fn((sql: unknown) => {
    if (String(sql).includes("INSERT INTO public.nd_candidate_finance_links")) {
      return Promise.resolve({ rows: [{ id: LINK_ID }], rowCount: 1 });
    }
    return Promise.resolve({ rows: [], rowCount: 0 });
  });
  return { query };
}

const batch = { now: new Date("2026-09-02T00:00:00Z"), maxCandidates: 10, electionLookbackDays: 78, electionLookaheadDays: 730 };

describe("autoLinkMissingNorthDakotaCandidateFinanceLinks", () => {
  it("links an exact office + seat + name match with the registry source", async () => {
    const db = linkDb();
    const results = await autoLinkMissingNorthDakotaCandidateFinanceLinks({
      db: db as never,
      ...batch,
      candidateElections: [candidateElection()],
      loadRegistry: vi.fn(async () => [committee({})]),
    });
    expect(results).toEqual([
      {
        candidateId: "candidate-1",
        electionId: "election-1",
        status: "linked",
        entityId: "1010001478",
        committeeName: "Friends of Jamie Selzler",
        orgStatus: "Active",
      },
    ]);
    const insert = db.query.mock.calls.find((call) => String(call[0]).includes("INSERT INTO public.nd_candidate_finance_links"));
    expect(insert?.[1]).toEqual([
      "candidate-1",
      "election-1",
      2026,
      "JAMIE SELZLER",
      "State Senator",
      "State Senate District 44 (2024); North Dakota",
      "1010001478",
      "Friends of Jamie Selzler",
      "active",
      "cfrs_registry",
      "https://cfrs.sos.nd.gov/",
      "2026-09-02T00:00:00.000Z",
    ]);
  });

  it("links a statewide race without a district and stores the state as the district", async () => {
    const db = linkDb();
    const results = await autoLinkMissingNorthDakotaCandidateFinanceLinks({
      db: db as never,
      ...batch,
      candidateElections: [
        candidateElection({ candidateName: "Mark Nelson", officeScope: "statewide", officeName: "Comptroller", district: "North Dakota" }),
      ],
      loadRegistry: vi.fn(async () => [
        committee({ entityId: "1010002001", orgID: 2001, orgName: null, candidateName: "Nelson, Mark", office: "Tax Commissioner", district: null }),
      ]),
    });
    expect(results[0]).toMatchObject({ status: "linked", entityId: "1010002001", committeeName: "Nelson, Mark" });
    const insert = db.query.mock.calls.find((call) => String(call[0]).includes("INSERT INTO public.nd_candidate_finance_links"));
    expect(insert?.[1]?.slice(4, 8)).toEqual(["Comptroller", "North Dakota", "1010002001", "Nelson, Mark"]);
  });

  it("reports unmappable offices and unparseable districts without fetching the registry", async () => {
    const loadRegistry = vi.fn();
    const results = await autoLinkMissingNorthDakotaCandidateFinanceLinks({
      db: linkDb() as never,
      ...batch,
      candidateElections: [
        candidateElection({ officeScope: "statewide", officeName: "Governor", district: "North Dakota" }),
        candidateElection({ candidateId: "candidate-2", district: "North Dakota" }),
      ],
      loadRegistry: loadRegistry as never,
    });
    expect(results[0]).toMatchObject({ status: "unmatched", reason: "office_unmapped" });
    expect(results[1]).toMatchObject({ status: "unmatched", reason: "district_unparseable" });
    expect(loadRegistry).not.toHaveBeenCalled();
  });

  it("reports ambiguity and no-match without writing", async () => {
    const db = linkDb();
    const results = await autoLinkMissingNorthDakotaCandidateFinanceLinks({
      db: db as never,
      ...batch,
      candidateElections: [candidateElection(), candidateElection({ candidateId: "candidate-2", candidateName: "Nobody Here" })],
      loadRegistry: vi.fn(async () => [committee({}), committee({ entityId: "1010000099", orgID: 99, orgStatus: "Inactive" })]),
    });
    expect(results[0]).toMatchObject({ status: "ambiguous", reason: "multiple_matching_committees" });
    expect(results[1]).toMatchObject({ status: "unmatched", reason: "no_matching_committee" });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("isolates per-candidate failures and keeps going", async () => {
    const results = await autoLinkMissingNorthDakotaCandidateFinanceLinks({
      db: linkDb() as never,
      ...batch,
      candidateElections: [candidateElection({ candidateId: "candidate-err" }), candidateElection({ candidateId: "candidate-2" })],
      loadRegistry: vi.fn(async () => {
        throw new Error("portal down");
      }),
    });
    expect(results).toHaveLength(2);
    expect(results[0]).toMatchObject({ status: "error", reason: "auto_link_failed", error: "portal down" });
    expect(results[1]).toMatchObject({ status: "error" });
  });
});
