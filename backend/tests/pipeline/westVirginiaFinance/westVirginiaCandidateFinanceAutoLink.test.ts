import { describe, expect, it, vi } from "vitest";

import type { WestVirginiaCommitteeRow } from "../../../src/pipeline/westVirginiaFinance/westVirginiaCfrsClient.js";
import { autoLinkMissingWestVirginiaCandidateFinanceLinks } from "../../../src/pipeline/westVirginiaFinance/westVirginiaCandidateFinanceAutoLink.js";

const LINK_ID = "33333333-3333-4333-8333-333333333333";

function committee(overrides: Partial<WestVirginiaCommitteeRow>): WestVirginiaCommitteeRow {
  return {
    orgID: 3610,
    entityId: "1010003610",
    orgName: "Committee to Elect Dean Jeffries",
    candidateName: "Jeffries, Warren Dean",
    orgType: "State Candidate",
    orgTypeCode: "101",
    orgSubType: "Candidate",
    office: "House of Delegates",
    district: "12",
    party: "Republican",
    election: "2026 Election",
    registrationYear: "2025",
    orgStatus: "Active",
    ...overrides,
  };
}

function candidateElection(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    candidateId: "candidate-1",
    electionId: "election-1",
    candidateName: "Warren Jeffries",
    electionYear: 2026,
    officeScope: "state_lower",
    officeName: "State Lower Chamber Legislator",
    district: "Delegate District 12 (2024); West Virginia",
    ...overrides,
  } as never;
}

function linkDb() {
  const query = vi.fn((sql: unknown) => {
    if (String(sql).includes("INSERT INTO public.wv_candidate_finance_links")) {
      return Promise.resolve({ rows: [{ id: LINK_ID }], rowCount: 1 });
    }
    return Promise.resolve({ rows: [], rowCount: 0 });
  });
  return { query };
}

const batch = { now: new Date("2026-09-01T00:00:00Z"), maxCandidates: 10, electionLookbackDays: 78, electionLookaheadDays: 730 };

describe("autoLinkMissingWestVirginiaCandidateFinanceLinks", () => {
  it("links an exact office + seat + name match with the registry source", async () => {
    const db = linkDb();
    const loadRegistry = vi.fn(async () => [committee({})]);
    const results = await autoLinkMissingWestVirginiaCandidateFinanceLinks({
      db: db as never,
      ...batch,
      candidateElections: [candidateElection()],
      loadRegistry,
    });
    expect(results).toEqual([
      {
        candidateId: "candidate-1",
        electionId: "election-1",
        status: "linked",
        entityId: "1010003610",
        committeeName: "Committee to Elect Dean Jeffries",
      },
    ]);
    const insert = db.query.mock.calls.find((call) => String(call[0]).includes("INSERT INTO public.wv_candidate_finance_links"));
    expect(insert?.[1]).toEqual([
      "candidate-1",
      "election-1",
      2026,
      "WARREN JEFFRIES",
      "State Lower Chamber Legislator",
      "Delegate District 12 (2024); West Virginia",
      "1010003610",
      "Committee to Elect Dean Jeffries",
      "active",
      "cfrs_registry",
      "https://cfrs.wvsos.gov/",
      "2026-09-01T00:00:00.000Z",
    ]);
  });

  it("reports unmappable offices and unparseable districts without fetching the registry", async () => {
    const loadRegistry = vi.fn();
    const results = await autoLinkMissingWestVirginiaCandidateFinanceLinks({
      db: linkDb() as never,
      ...batch,
      candidateElections: [
        candidateElection({ officeScope: "statewide", officeName: "State Level Judge", district: null }),
        candidateElection({ candidateId: "candidate-2", district: "West Virginia" }),
      ],
      loadRegistry: loadRegistry as never,
    });
    expect(results[0]).toMatchObject({ status: "unmatched", reason: "office_unmapped" });
    expect(results[1]).toMatchObject({ status: "unmatched", reason: "district_unparseable" });
    expect(loadRegistry).not.toHaveBeenCalled();
  });

  it("reports ambiguity and no-match without writing", async () => {
    const db = linkDb();
    const results = await autoLinkMissingWestVirginiaCandidateFinanceLinks({
      db: db as never,
      ...batch,
      candidateElections: [candidateElection(), candidateElection({ candidateId: "candidate-2", candidateName: "Nobody Here" })],
      loadRegistry: vi.fn(async () => [committee({}), committee({ entityId: "1010003999", orgID: 3999, orgStatus: "Terminated" })]),
    });
    expect(results[0]).toMatchObject({ status: "ambiguous", reason: "multiple_matching_committees" });
    expect(results[1]).toMatchObject({ status: "unmatched", reason: "no_matching_committee" });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("isolates per-candidate failures and keeps going", async () => {
    const results = await autoLinkMissingWestVirginiaCandidateFinanceLinks({
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
