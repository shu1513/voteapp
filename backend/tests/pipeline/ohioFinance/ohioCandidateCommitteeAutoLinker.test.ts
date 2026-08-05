import { describe, expect, it, vi } from "vitest";

import {
  autoLinkMissingOhioCandidateFinanceLinks,
  autoLinkOhioCandidateFinanceForCandidateElection,
  listOhioCandidateElectionsMissingFinanceLinks,
} from "../../../src/pipeline/ohioFinance/ohioCandidateCommitteeAutoLinker.js";
import type { OhioSosCandidateCommitteeListRow } from "../../../src/pipeline/ohioFinance/ohioSosBulkFiles.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2026-06-01T00:00:00.000Z");
const SOURCE_URL = "https://www6.ohiosos.gov/ords/f?p=CFDISCLOSURE:73";

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows, rowCount: rows.length }),
  };
}

function listRow(overrides: Partial<OhioSosCandidateCommitteeListRow> = {}): OhioSosCandidateCommitteeListRow {
  return {
    committeeName: "FRIENDS OF JANE DOE",
    masterKey: "12345",
    candidateFirstName: "JANE",
    candidateLastName: "DOE",
    office: "HOUSE",
    district: "87",
    party: "DEMOCRAT",
    ...overrides,
  };
}

const CANDIDATE_ELECTION = {
  candidateId: CANDIDATE_ID,
  electionId: ELECTION_ID,
  candidateName: "Jane Doe",
  electionYear: 2026,
  officeScope: "state_lower",
  officeName: "State Lower Chamber Legislator",
  district: "87",
};

describe("ohioCandidateCommitteeAutoLinker", () => {
  it("lists eligible Ohio candidate elections missing active finance links", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Jane Doe",
        election_year: 2026,
        office_scope: "state_lower",
        office_name: "State Lower Chamber Legislator",
        district: "87",
      },
    ]);

    await expect(
      listOhioCandidateElectionsMissingFinanceLinks(db, {
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
      })
    ).resolves.toEqual([CANDIDATE_ELECTION]);

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.candidate_elections AS candidate_election");
    expect(sql).toContain("district.state = 'OH'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($5::text[])");
    expect(sql).toContain("FROM public.oh_candidate_finance_links AS link");

    const params = db.query.mock.calls[0]?.[1] as unknown[];
    expect(params?.[4]).toContain("state_lower::State Lower Chamber Legislator");
  });

  it("links an exact single-committee match through the Ohio writer", async () => {
    const db = createMockDb([{ id: "33333333-3333-4333-8333-333333333333" }]);

    await expect(
      autoLinkOhioCandidateFinanceForCandidateElection({
        db,
        candidateElection: CANDIDATE_ELECTION,
        candidateListRows: [listRow()],
        sourceUrl: SOURCE_URL,
        now: NOW,
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "linked",
      committeeId: "12345",
    });

    expect(db.query).toHaveBeenCalledTimes(1);
    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("INSERT INTO public.oh_candidate_finance_links");
    const params = db.query.mock.calls[0]?.[1] as unknown[];
    expect(params).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "JANE DOE",
      "State Lower Chamber Legislator",
      "87",
      "12345",
      "FRIENDS OF JANE DOE",
      "active",
      "sos_bulk_export",
      SOURCE_URL,
      NOW.toISOString(),
    ]);
  });

  it("does not write when the resolver is unmatched or ambiguous", async () => {
    const db = createMockDb();

    await expect(
      autoLinkOhioCandidateFinanceForCandidateElection({
        db,
        candidateElection: CANDIDATE_ELECTION,
        candidateListRows: [listRow({ district: "88" })],
        sourceUrl: SOURCE_URL,
        now: NOW,
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "unmatched",
      reason: "no_candidate_committee_match",
    });

    await expect(
      autoLinkOhioCandidateFinanceForCandidateElection({
        db,
        candidateElection: CANDIDATE_ELECTION,
        candidateListRows: [listRow(), listRow({ masterKey: "67890", committeeName: "JANE DOE FOR OHIO" })],
        sourceUrl: SOURCE_URL,
        now: NOW,
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "ambiguous",
      reason: "multiple_matching_committees",
    });

    expect(db.query).not.toHaveBeenCalled();
  });

  it("continues past a per-candidate failure and reports it", async () => {
    const db = {
      query: vi.fn().mockRejectedValue(new Error("connection reset")),
    };
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      await expect(
        autoLinkMissingOhioCandidateFinanceLinks({
          db,
          now: NOW,
          maxCandidates: 25,
          electionLookbackDays: 1,
          electionLookaheadDays: 730,
          candidateListRows: [listRow()],
          sourceUrl: SOURCE_URL,
          candidateElections: [
            CANDIDATE_ELECTION,
            { ...CANDIDATE_ELECTION, electionId: "44444444-4444-4444-8444-444444444444", district: "88" },
          ],
        })
      ).resolves.toEqual([
        {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          status: "error",
          reason: "auto_link_failed",
          error: "connection reset",
        },
        {
          candidateId: CANDIDATE_ID,
          electionId: "44444444-4444-4444-8444-444444444444",
          status: "unmatched",
          reason: "no_candidate_committee_match",
        },
      ]);
    } finally {
      warn.mockRestore();
    }
  });
});
