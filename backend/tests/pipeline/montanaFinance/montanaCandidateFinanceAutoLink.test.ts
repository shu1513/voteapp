import { describe, expect, it, vi } from "vitest";

import type { MontanaCersCandidateSearchRow } from "../../../src/pipeline/montanaFinance/montanaCersParsers.js";
import {
  autoLinkMissingMontanaCandidateFinanceLinks,
  listMontanaCandidateElectionsMissingFinanceLinks,
  type MontanaFinanceAutoLinkCandidateElection,
} from "../../../src/pipeline/montanaFinance/montanaCandidateFinanceAutoLink.js";
import { MONTANA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "../../../src/pipeline/montanaFinance/montanaFinanceEligibleOffices.js";

const LINK_ID = "33333333-3333-4333-8333-333333333333";

function cersRow(overrides: Partial<MontanaCersCandidateSearchRow>): MontanaCersCandidateSearchRow {
  return {
    candidateId: 21020,
    lastName: "Bedey",
    firstName: "David",
    middleInitial: "F.",
    electionYear: 2026,
    officeTitle: "Senate District No. 43",
    officeCode: "236",
    partyDescr: "Republican",
    candidateStatusDescr: "Amended",
    resCountyDescr: "Ravalli",
    ...overrides,
  };
}

function candidateElection(
  overrides: Partial<MontanaFinanceAutoLinkCandidateElection>
): MontanaFinanceAutoLinkCandidateElection {
  return {
    candidateId: "candidate-1",
    electionId: "election-1",
    candidateName: "David Bedey",
    electionDate: "2026-11-03",
    electionYear: 2026,
    officeScope: "state_upper",
    officeName: "State Senator",
    district: "State Senate District 43 (2024); Montana",
    legislativeDistrict: "43",
    ballotTitle: "State Senator, District 43",
    ...overrides,
  };
}

function linkWritingDb() {
  return {
    query: vi.fn((sql: unknown) => {
      if (String(sql).includes("INSERT INTO public.mt_candidate_finance_links")) {
        return Promise.resolve({ rows: [{ id: LINK_ID }], rowCount: 1 });
      }
      return Promise.resolve({ rows: [], rowCount: 0 });
    }),
  };
}

describe("listMontanaCandidateElectionsMissingFinanceLinks", () => {
  it("selects eligible MT general candidate elections without an active link", async () => {
    const db = { query: vi.fn().mockResolvedValue({ rows: [] }) };
    await listMontanaCandidateElectionsMissingFinanceLinks(db, {
      now: new Date("2026-08-28T00:00:00.000Z"),
      maxCandidates: 25,
      electionLookbackDays: 55,
      electionLookaheadDays: 730,
    });
    const [sql, params] = db.query.mock.calls[0]!;
    expect(String(sql)).toContain("district.state = 'MT'");
    expect(String(sql)).toContain("election.election_stage = 'general'");
    expect(String(sql)).toContain("mt_candidate_finance_links");
    expect(String(sql)).toContain("geoid_compact");
    // PSC seat numbers live in the ballot title, not the district name.
    expect(String(sql)).toContain("official_ballot_title");
    expect(params?.[4]).toEqual([...MONTANA_FINANCE_ELIGIBLE_OFFICE_KEYS]);
  });
});

describe("autoLinkMissingMontanaCandidateFinanceLinks", () => {
  it("fetches the year list once and links a resolved candidate with cers_portal source", async () => {
    const db = linkWritingDb();
    const search = vi.fn().mockResolvedValue([cersRow({}), cersRow({ candidateId: 30000, lastName: "Wirth", firstName: "Zack", middleInitial: null, officeTitle: "Senate District No. 9" })]);
    const results = await autoLinkMissingMontanaCandidateFinanceLinks({
      db,
      now: new Date("2026-08-28T00:00:00.000Z"),
      maxCandidates: 25,
      electionLookbackDays: 55,
      electionLookaheadDays: 730,
      candidateElections: [
        candidateElection({}),
        candidateElection({
          candidateId: "candidate-2",
          electionId: "election-2",
          candidateName: "Zack Wirth",
          district: "State Senate District 9 (2024); Montana",
          legislativeDistrict: "9",
        }),
      ],
      searchCandidatesByYear: search,
    });

    expect(search).toHaveBeenCalledTimes(1);
    expect(results).toEqual([
      expect.objectContaining({ status: "linked", cersCandidateId: 21020, cersCandidateName: "Bedey, David F." }),
      expect.objectContaining({ status: "linked", cersCandidateId: 30000, cersCandidateName: "Wirth, Zack" }),
    ]);
    const insert = db.query.mock.calls.find(([sql]) =>
      String(sql).includes("INSERT INTO public.mt_candidate_finance_links")
    );
    expect(insert?.[1]).toContain("21020");
    expect(insert?.[1]).toContain("cers_portal");
  });

  it("reports unmatched and ambiguous outcomes without writing links", async () => {
    const db = linkWritingDb();
    const results = await autoLinkMissingMontanaCandidateFinanceLinks({
      db,
      now: new Date("2026-08-28T00:00:00.000Z"),
      maxCandidates: 25,
      electionLookbackDays: 55,
      electionLookaheadDays: 730,
      candidateElections: [
        candidateElection({ candidateName: "Nobody Here" }),
        candidateElection({ candidateId: "candidate-3", electionId: "election-3" }),
      ],
      searchCandidatesByYear: vi.fn().mockResolvedValue([cersRow({}), cersRow({ candidateId: 21021 })]),
    });
    expect(results[0]).toMatchObject({ status: "unmatched", reason: "no_matching_cers_candidate" });
    expect(results[1]).toMatchObject({ status: "ambiguous" });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("records a failed year fetch as per-candidate errors and continues", async () => {
    const db = linkWritingDb();
    const results = await autoLinkMissingMontanaCandidateFinanceLinks({
      db,
      now: new Date("2026-08-28T00:00:00.000Z"),
      maxCandidates: 25,
      electionLookbackDays: 55,
      electionLookaheadDays: 730,
      candidateElections: [candidateElection({})],
      searchCandidatesByYear: vi.fn().mockRejectedValue(new Error("CERS down")),
    });
    expect(results).toEqual([
      expect.objectContaining({ status: "error", reason: "auto_link_failed", error: "CERS down" }),
    ]);
  });
});
