import { describe, expect, it, vi } from "vitest";

import type {
  AlabamaCommitteeSearchRow,
  AlabamaRaceRow,
} from "../../../src/pipeline/alabamaFinance/alabamaFcpaClient.js";
import { autoLinkMissingAlabamaCandidateFinanceLinks } from "../../../src/pipeline/alabamaFinance/alabamaCandidateFinanceAutoLink.js";

const LINK_ID = "33333333-3333-4333-8333-333333333333";

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
    jurisdiction: "HOUSE DISTRICT 68",
    ...overrides,
  };
}

function candidateElection(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    candidateId: "candidate-1",
    electionId: "election-1",
    candidateName: "Doug Jones",
    electionYear: 2026,
    officeScope: "state_lower",
    officeName: "State Lower Chamber Legislator",
    ballotTitle: "State Representative, District 68",
    district: "State House District 68 (2024); Alabama",
    ...overrides,
  } as never;
}

function linkDb() {
  const query = vi.fn((sql: unknown) => {
    if (String(sql).includes("INSERT INTO public.al_candidate_finance_links")) {
      return Promise.resolve({ rows: [{ id: LINK_ID }], rowCount: 1 });
    }
    return Promise.resolve({ rows: [], rowCount: 0 });
  });
  return { query };
}

describe("autoLinkMissingAlabamaCandidateFinanceLinks", () => {
  it("links a district-confirmed match and backfills the FCPA committee number", async () => {
    const db = linkDb();
    const results = await autoLinkMissingAlabamaCandidateFinanceLinks({
      db: db as never,
      now: new Date("2026-09-01T00:00:00Z"),
      maxCandidates: 10,
      electionLookbackDays: 90,
      electionLookaheadDays: 730,
      candidateElections: [candidateElection()],
      loadOfficeRaceContext: vi.fn(async () => ({
        raceRows: [raceRow({})],
        committeeRowsByInternalId: new Map([[7962, committeeRow({})]]),
      })),
    });
    expect(results).toEqual([
      {
        candidateId: "candidate-1",
        electionId: "election-1",
        status: "linked",
        internalCommitteeId: 7962,
        fcpaCommitteeNumber: "32837",
      },
    ]);
    const backfill = db.query.mock.calls.find((call) =>
      String(call[0]).includes("SET fcpa_committee_number")
    );
    expect(backfill?.[1]).toEqual([LINK_ID, "32837"]);
  });

  it("reports an unmappable office without touching the portal", async () => {
    const loadOfficeRaceContext = vi.fn();
    const results = await autoLinkMissingAlabamaCandidateFinanceLinks({
      db: linkDb() as never,
      now: new Date("2026-09-01T00:00:00Z"),
      maxCandidates: 10,
      electionLookbackDays: 90,
      electionLookaheadDays: 730,
      candidateElections: [
        candidateElection({
          officeScope: "statewide",
          officeName: "State Level Judge",
          ballotTitle: "Alabama Circuit Court, Place 2",
          district: null,
        }),
      ],
      loadOfficeRaceContext: loadOfficeRaceContext as never,
    });
    expect(results[0]).toMatchObject({ status: "unmatched", reason: "office_unmapped" });
    expect(loadOfficeRaceContext).not.toHaveBeenCalled();
  });

  it("fails closed when the roster district number cannot be parsed", async () => {
    const results = await autoLinkMissingAlabamaCandidateFinanceLinks({
      db: linkDb() as never,
      now: new Date("2026-09-01T00:00:00Z"),
      maxCandidates: 10,
      electionLookbackDays: 90,
      electionLookaheadDays: 730,
      candidateElections: [candidateElection({ district: "Alabama" })],
      loadOfficeRaceContext: vi.fn() as never,
    });
    expect(results[0]).toMatchObject({ status: "unmatched", reason: "district_unparseable" });
  });

  it("isolates per-candidate failures and keeps going", async () => {
    const db = linkDb();
    const results = await autoLinkMissingAlabamaCandidateFinanceLinks({
      db: db as never,
      now: new Date("2026-09-01T00:00:00Z"),
      maxCandidates: 10,
      electionLookbackDays: 90,
      electionLookaheadDays: 730,
      candidateElections: [
        candidateElection({ candidateId: "candidate-err" }),
        candidateElection({ candidateId: "candidate-2" }),
      ],
      loadOfficeRaceContext: vi.fn(async () => {
        throw new Error("portal down");
      }),
    });
    expect(results).toHaveLength(2);
    expect(results[0]).toMatchObject({ status: "error", reason: "auto_link_failed" });
    expect(results[1]).toMatchObject({ status: "error" });
  });
});
