import { describe, expect, it, vi } from "vitest";

import type { ArkansasFilerRegistrationRow } from "../../../src/pipeline/arkansasFinance/arkansasCfisClient.js";
import {
  ARKANSAS_CFIS_REGISTRATION_SEARCH_URL,
  autoLinkMissingArkansasCandidateFinanceLinks,
  createArkansasRegistrationSweepLoader,
} from "../../../src/pipeline/arkansasFinance/arkansasCandidateFinanceAutoLink.js";

const LINK_ID = "33333333-3333-4333-8333-333333333333";

function registration(overrides: Partial<ArkansasFilerRegistrationRow> = {}): ArkansasFilerRegistrationRow {
  return {
    registrationGuid: "0b27e93d-5e84-4cad-b859-6ae20a13782f",
    filerEntityId: 7817,
    filerEntityVersionId: 1,
    filerType: "Candidate",
    filerTypeCode: "CAN",
    filerStatus: "Active",
    filerName: "Doe, Robert S.",
    firstName: "Robert",
    lastName: "Doe",
    suffix: null,
    committeeName: null,
    office: "State Representative",
    officeDistrictName: "13",
    jurisdictionName: "Arkansas",
    politicalParty: "Republican Party",
    electionYear: 2026,
    filingYear: 2026,
    isPaperFiler: false,
    totalRaised: 27800,
    totalSpent: 18656.07,
    balanceOfFunds: 31854.07,
    ...overrides,
  };
}

function candidateElection(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    candidateId: "candidate-1",
    electionId: "election-1",
    candidateName: "Bob Doe",
    candidateParty: "Republican",
    electionYear: 2026,
    officeScope: "state_lower",
    officeName: "State Lower Chamber Legislator",
    district: "State House District 13 (2024); Arkansas",
    ...overrides,
  } as never;
}

function linkDb() {
  const query = vi.fn((sql: unknown) => {
    if (String(sql).includes("INSERT INTO public.ar_candidate_finance_links")) {
      return Promise.resolve({ rows: [{ id: LINK_ID }], rowCount: 1 });
    }
    return Promise.resolve({ rows: [], rowCount: 0 });
  });
  return { query };
}

const baseInput = {
  now: new Date("2026-09-02T00:00:00Z"),
  maxCandidates: 10,
  electionLookbackDays: 98,
  electionLookaheadDays: 730,
};

describe("autoLinkMissingArkansasCandidateFinanceLinks", () => {
  it("links an exact match with the automatic link source and loads the sweep once", async () => {
    const db = linkDb();
    const loadRegistrations = vi.fn(async () => [registration()]);
    const results = await autoLinkMissingArkansasCandidateFinanceLinks({
      ...baseInput,
      db: db as never,
      candidateElections: [candidateElection(), candidateElection({ candidateId: "candidate-2", candidateName: "Nobody Here" })],
      loadRegistrations,
    });
    expect(results).toEqual([
      { candidateId: "candidate-1", electionId: "election-1", status: "linked", filingEntityId: 7817, filerName: "Robert Doe" },
      { candidateId: "candidate-2", electionId: "election-1", status: "unmatched", reason: "no_candidate_filer_match" },
    ]);
    expect(loadRegistrations).toHaveBeenCalledTimes(2);
    const insert = db.query.mock.calls.find((call) => String(call[0]).includes("INSERT INTO public.ar_candidate_finance_links"));
    expect(insert).toBeDefined();
    const params = insert![1] as unknown[];
    expect(params).toEqual(
      expect.arrayContaining(["candidate-1", "election-1", 2026, "BOB DOE", "State Lower Chamber Legislator", "7817", "Robert Doe", "active", "cfis_registration", ARKANSAS_CFIS_REGISTRATION_SEARCH_URL])
    );
    expect(db.query.mock.calls.filter((call) => String(call[0]).includes("INSERT INTO"))).toHaveLength(1);
  });

  it("reports ambiguity and dormant duplicates without guessing", async () => {
    const db = linkDb();
    const funded = registration({ filerEntityId: 7298, registrationGuid: "69b74574-f3e2-43fe-9c18-1305d73813c5", totalRaised: 5 });
    const dormant = { ...funded, totalRaised: 0, totalSpent: 0, balanceOfFunds: 0 };
    const ambiguous = await autoLinkMissingArkansasCandidateFinanceLinks({
      ...baseInput,
      db: db as never,
      candidateElections: [candidateElection()],
      loadRegistrations: async () => [registration(), funded],
    });
    expect(ambiguous[0]).toMatchObject({ status: "ambiguous", reason: "multiple_matching_filers" });
    expect(ambiguous[0]!.candidates?.map((match) => match.filingEntityId)).toEqual([7298, 7817]);
    expect(db.query.mock.calls.some((call) => String(call[0]).includes("INSERT INTO"))).toBe(false);

    const linked = await autoLinkMissingArkansasCandidateFinanceLinks({
      ...baseInput,
      db: db as never,
      candidateElections: [candidateElection()],
      loadRegistrations: async () => [registration(), dormant],
    });
    expect(linked).toEqual([
      { candidateId: "candidate-1", electionId: "election-1", status: "linked", filingEntityId: 7817, filerName: "Robert Doe", dormantFilingEntityIds: [7298] },
    ]);
  });

  it("captures a sweep failure per candidate instead of aborting the run", async () => {
    const db = linkDb();
    const results = await autoLinkMissingArkansasCandidateFinanceLinks({
      ...baseInput,
      db: db as never,
      candidateElections: [candidateElection()],
      loadRegistrations: async () => {
        throw new Error("Arkansas CFIS request failed (PublicFilerDetails/GetCandidateCommitteDetails): ENOTFOUND");
      },
    });
    expect(results).toEqual([
      {
        candidateId: "candidate-1",
        electionId: "election-1",
        status: "error",
        reason: "auto_link_failed",
        error: expect.stringContaining("ENOTFOUND"),
      },
    ]);
    expect(db.query).not.toHaveBeenCalled();
  });

  it("memoizes the full registration sweep across calls", async () => {
    const fetchRegistrations = vi.fn(async () => [registration()]);
    const load = createArkansasRegistrationSweepLoader({ fetchRegistrations: fetchRegistrations as never });
    await load();
    await load();
    expect(fetchRegistrations).toHaveBeenCalledTimes(1);
    expect(fetchRegistrations).toHaveBeenCalledWith({ pageSize: 1000 }, undefined);
  });
});
