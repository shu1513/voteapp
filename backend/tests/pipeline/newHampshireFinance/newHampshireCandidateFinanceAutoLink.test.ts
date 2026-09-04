import { describe, expect, it, vi } from "vitest";

import {
  autoLinkMissingNewHampshireCandidateFinanceLinks,
  resolveNewHampshireElectionCycleId,
  type NewHampshireFinanceAutoLinkCandidateElection,
} from "../../../src/pipeline/newHampshireFinance/newHampshireCandidateFinanceAutoLink.js";
import { CYCLE_2026_ID, ELECTION_CYCLES, filingEntity } from "./newHampshireTestFixtures.js";

const LINK_ID = "33333333-3333-4333-8333-333333333333";
const NOW = new Date("2026-09-03T00:00:00.000Z");
const INSERT = "INSERT INTO public.nh_candidate_finance_links";

function candidate(
  overrides: Partial<NewHampshireFinanceAutoLinkCandidateElection> = {}
): NewHampshireFinanceAutoLinkCandidateElection {
  return {
    candidateId: "11111111-1111-4111-8111-111111111111",
    electionId: "22222222-2222-4222-8222-222222222222",
    candidateNames: ["Sample Candidate"],
    electionYear: 2026,
    officeScope: "state_upper",
    officeName: "State Senator",
    district: "State Senate District 1",
    ...overrides,
  };
}

function fakeDb(onInsert?: () => Promise<never>) {
  return {
    query: vi.fn((sql: unknown) => {
      if (String(sql).includes(INSERT)) {
        return onInsert ? onInsert() : Promise.resolve({ rows: [{ id: LINK_ID }], rowCount: 1 });
      }
      return Promise.resolve({ rows: [], rowCount: 0 });
    }),
  };
}

function registryClient(rows = [filingEntity()]) {
  return {
    getElectionCycles: vi.fn().mockResolvedValue(ELECTION_CYCLES),
    getFilingEntities: vi.fn().mockResolvedValue(rows),
  };
}

const baseInput = {
  now: NOW,
  maxCandidates: null,
  electionLookbackDays: 30,
  electionLookaheadDays: 730,
};

describe("resolveNewHampshireElectionCycleId", () => {
  it("maps an election year to the one cycle named after it", () => {
    expect(resolveNewHampshireElectionCycleId({ cycles: ELECTION_CYCLES, electionYear: 2026 })).toBe(CYCLE_2026_ID);
    expect(() => resolveNewHampshireElectionCycleId({ cycles: ELECTION_CYCLES, electionYear: 2028 })).toThrow(
      "Expected one New Hampshire CFS cycle named 2028 Election Cycle; found 0"
    );
  });
});

describe("autoLinkMissingNewHampshireCandidateFinanceLinks", () => {
  it("writes a cfs_registration link for a resolved Active registration, pulling the registry once", async () => {
    const db = fakeDb();
    const client = registryClient();
    const results = await autoLinkMissingNewHampshireCandidateFinanceLinks({
      ...baseInput,
      db,
      candidateElections: [candidate(), candidate({ candidateId: "11111111-1111-4111-8111-111111111112", candidateNames: ["Nobody Here"] })],
      cfsClient: client,
    });
    expect(client.getElectionCycles).toHaveBeenCalledTimes(1);
    expect(client.getFilingEntities).toHaveBeenCalledTimes(1);
    expect(client.getFilingEntities).toHaveBeenCalledWith({ electionCycleId: CYCLE_2026_ID }, undefined);
    expect(results).toEqual([
      {
        candidateId: "11111111-1111-4111-8111-111111111111",
        electionId: "22222222-2222-4222-8222-222222222222",
        electionCycleId: CYCLE_2026_ID,
        status: "linked",
        filingEntityId: 50_450,
        filerName: "Sample Candidate Committee",
        district: "1",
        confidence: "exact",
      },
      {
        candidateId: "11111111-1111-4111-8111-111111111112",
        electionId: "22222222-2222-4222-8222-222222222222",
        electionCycleId: CYCLE_2026_ID,
        status: "unmatched",
        reason: "no_candidate_filer_match",
      },
    ]);
    const insert = db.query.mock.calls.find((call) => String(call[0]).includes(INSERT));
    expect(insert?.[1]).toEqual([
      "11111111-1111-4111-8111-111111111111",
      "22222222-2222-4222-8222-222222222222",
      2026,
      "SAMPLE CANDIDATE",
      "State Senate",
      "1",
      "50450",
      "Sample Candidate Committee",
      "active",
      "cfs_registration",
      "https://cfs.sos.nh.gov/",
      NOW.toISOString(),
    ]);
  });

  it("falls back to the structured name spelling when the display name misses", async () => {
    const db = fakeDb();
    const results = await autoLinkMissingNewHampshireCandidateFinanceLinks({
      ...baseInput,
      db,
      candidateElections: [candidate({ candidateNames: ["Sam \"Sammy\" Candidate-Smith", "Sample Candidate"] })],
      electionCycles: ELECTION_CYCLES,
      cfsClient: { getFilingEntities: vi.fn().mockResolvedValue([filingEntity()]) },
    });
    expect(results[0]).toMatchObject({ status: "linked", filingEntityId: 50_450 });
    const insert = db.query.mock.calls.find((call) => String(call[0]).includes(INSERT));
    // The stored normalized name is always the display name.
    expect(insert?.[1]?.[3]).toBe("SAM SAMMY CANDIDATE SMITH");
  });

  it("reports without writing in dry-run mode and for ambiguous, non-Active, or unmatched candidates", async () => {
    const db = fakeDb();
    const terminatedOnly = filingEntity({
      filingEntityId: 60_600,
      filerName: "Friends of Other Person",
      candidateName: "Other Person",
      firstName: "Other",
      lastName: "Person",
      district: "2",
      status: "Terminated",
    });
    const results = await autoLinkMissingNewHampshireCandidateFinanceLinks({
      ...baseInput,
      db,
      dryRun: true,
      candidateElections: [
        candidate(),
        candidate({ candidateId: "11111111-1111-4111-8111-111111111112", candidateNames: ["Other Person"], district: "State Senate District 2" }),
        candidate({ candidateId: "11111111-1111-4111-8111-111111111113", candidateNames: ["Nobody Here"] }),
        candidate({ candidateId: "11111111-1111-4111-8111-111111111114", officeScope: "county", officeName: "Sheriff", district: null }),
      ],
      electionCycles: ELECTION_CYCLES,
      cfsClient: {
        getFilingEntities: vi.fn().mockResolvedValue([
          filingEntity(),
          filingEntity({ filingEntityId: 50_451, filerName: "Second Sample Committee" }),
          terminatedOnly,
        ]),
      },
    });
    expect(results.map((result) => [result.status, result.reason])).toEqual([
      ["ambiguous", "multiple_matching_filers"],
      ["unmatched", "no_active_registration"],
      ["unmatched", "no_candidate_filer_match"],
      ["unmatched", "missing_required_district"],
    ]);
    expect(results[0]?.matches).toEqual([
      { filingEntityId: 50_450, filerName: "Sample Candidate Committee", statuses: ["Active"] },
      { filingEntityId: 50_451, filerName: "Second Sample Committee", statuses: ["Active"] },
    ]);
    expect(results[1]?.matches).toEqual([
      { filingEntityId: 60_600, filerName: "Friends of Other Person", statuses: ["Terminated"] },
    ]);
    expect(db.query).not.toHaveBeenCalled();
  });

  it("never links a Terminated registration even outside dry-run", async () => {
    const db = fakeDb();
    const results = await autoLinkMissingNewHampshireCandidateFinanceLinks({
      ...baseInput,
      db,
      candidateElections: [candidate()],
      electionCycles: ELECTION_CYCLES,
      cfsClient: { getFilingEntities: vi.fn().mockResolvedValue([filingEntity({ status: "Terminated" })]) },
    });
    expect(results[0]).toMatchObject({ status: "unmatched", reason: "no_active_registration" });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("captures per-candidate failures, including an unknown cycle year, and skips the registry when nothing is due", async () => {
    const failing = fakeDb(() => Promise.reject(new Error("db down")));
    const results = await autoLinkMissingNewHampshireCandidateFinanceLinks({
      ...baseInput,
      db: failing,
      candidateElections: [candidate(), candidate({ candidateId: "11111111-1111-4111-8111-111111111112", electionYear: 2028 })],
      cfsClient: registryClient(),
    });
    expect(results[0]).toMatchObject({ status: "error", reason: "auto_link_failed", error: "db down" });
    expect(results[1]).toMatchObject({
      status: "error",
      reason: "auto_link_failed",
      error: "Expected one New Hampshire CFS cycle named 2028 Election Cycle; found 0",
    });

    const client = registryClient();
    await expect(
      autoLinkMissingNewHampshireCandidateFinanceLinks({ ...baseInput, db: fakeDb(), candidateElections: [], cfsClient: client })
    ).resolves.toEqual([]);
    expect(client.getElectionCycles).not.toHaveBeenCalled();
    expect(client.getFilingEntities).not.toHaveBeenCalled();
  });

  it("requests the registry once even when it fails", async () => {
    const client = registryClient();
    client.getFilingEntities.mockRejectedValue(new Error("cfs down"));
    const results = await autoLinkMissingNewHampshireCandidateFinanceLinks({
      ...baseInput,
      db: fakeDb(),
      candidateElections: [candidate(), candidate({ candidateId: "11111111-1111-4111-8111-111111111112" })],
      cfsClient: client,
    });
    expect(results.map((result) => result.error)).toEqual(["cfs down", "cfs down"]);
    expect(client.getFilingEntities).toHaveBeenCalledTimes(1);
  });

  it("lists New Hampshire general-election candidates in eligible offices without an active link", async () => {
    const db = { query: vi.fn().mockResolvedValue({ rows: [] }) };
    await autoLinkMissingNewHampshireCandidateFinanceLinks({ ...baseInput, db, maxCandidates: 5, cfsClient: registryClient() });
    const [sql, params] = db.query.mock.calls[0]!;
    expect(String(sql)).toContain("district.state = 'NH'");
    expect(String(sql)).toContain("election.election_stage = 'general'");
    expect(String(sql)).toContain("public.nh_candidate_finance_links");
    expect(params?.slice(0, 4)).toEqual([NOW.toISOString(), 5, 30, 730]);
    expect(params?.[4]).toContain("state_upper::State Senator");
  });
});
