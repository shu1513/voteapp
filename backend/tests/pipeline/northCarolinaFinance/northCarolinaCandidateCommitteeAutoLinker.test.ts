import { describe, expect, it, vi } from "vitest";

import {
  autoLinkMissingNorthCarolinaCandidateFinanceLinks,
  autoLinkNorthCarolinaCandidateFinanceForCandidateElection,
  listNorthCarolinaCandidateElectionsMissingFinanceLinks,
} from "../../../src/pipeline/northCarolinaFinance/northCarolinaCandidateCommitteeAutoLinker.js";
import type { NcsbeCommitteeSearchRow } from "../../../src/pipeline/northCarolinaFinance/northCarolinaNcsbeParsers.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2026-06-01T00:00:00.000Z");
const SOURCE_URL =
  "https://cf.ncsbe.gov/CFOrgLkup/CommitteeGeneralResult/?name=Jane%20Doe&useOrgName=True&useCandName=True&useInHouseName=True&useAcronym=False";

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows, rowCount: rows.length }),
  };
}

function searchRow(overrides: Partial<NcsbeCommitteeSearchRow> = {}): NcsbeCommitteeSearchRow {
  return {
    orgName: "COMMITTEE TO ELECT JANE DOE",
    sboeId: "STA-AB12CD-C-001",
    oldId: null,
    candName: "JANE DOE",
    statusDesc: "ACTIVE (NON-EXEMPT)",
    orgGroupId: 12345,
    ...overrides,
  };
}

function loader(rows: NcsbeCommitteeSearchRow[]) {
  return vi.fn().mockResolvedValue({ rows, sourceUrl: SOURCE_URL });
}

const CANDIDATE_ELECTION = {
  candidateId: CANDIDATE_ID,
  electionId: ELECTION_ID,
  candidateName: "Jane Doe",
  electionYear: 2026,
  electionDate: "2026-11-03",
  officeScope: "state_lower",
  officeName: "State Lower Chamber Legislator",
  district: "27",
};

describe("northCarolinaCandidateCommitteeAutoLinker", () => {
  it("lists eligible NC candidate elections missing active finance links", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Jane Doe",
        election_year: 2026,
        election_date: "2026-11-03",
        office_scope: "state_lower",
        office_name: "State Lower Chamber Legislator",
        district: "27",
      },
    ]);

    await expect(
      listNorthCarolinaCandidateElectionsMissingFinanceLinks(db, {
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
      })
    ).resolves.toEqual([CANDIDATE_ELECTION]);

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.candidate_elections AS candidate_election");
    expect(sql).toContain("district.state = 'NC'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($5::text[])");
    expect(sql).toContain("FROM public.nc_candidate_finance_links AS link");

    const params = db.query.mock.calls[0]?.[1] as unknown[];
    expect(params?.[4]).toContain("state_lower::State Lower Chamber Legislator");
    expect(params?.[4]).toContain("state_upper::State Senator");
  });

  it("links an exact single-committee match through the NC writer", async () => {
    const db = createMockDb([{ id: "33333333-3333-4333-8333-333333333333" }]);
    const loadCandidateSearchRows = loader([searchRow()]);

    await expect(
      autoLinkNorthCarolinaCandidateFinanceForCandidateElection({
        db,
        candidateElection: CANDIDATE_ELECTION,
        loadCandidateSearchRows,
        now: NOW,
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "linked",
      committeeId: "STA-AB12CD-C-001",
      orgGroupId: 12345,
    });

    expect(loadCandidateSearchRows).toHaveBeenCalledWith(CANDIDATE_ELECTION);
    expect(db.query).toHaveBeenCalledTimes(1);
    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("INSERT INTO public.nc_candidate_finance_links");
    const params = db.query.mock.calls[0]?.[1] as unknown[];
    expect(params).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "JANE DOE",
      "State Lower Chamber Legislator",
      "27",
      "STA-AB12CD-C-001",
      "COMMITTEE TO ELECT JANE DOE",
      "active",
      "ncsbe_portal",
      SOURCE_URL,
      NOW.toISOString(),
    ]);
  });

  it("does not write when the resolver is unmatched or ambiguous", async () => {
    const db = createMockDb();

    await expect(
      autoLinkNorthCarolinaCandidateFinanceForCandidateElection({
        db,
        candidateElection: CANDIDATE_ELECTION,
        loadCandidateSearchRows: loader([searchRow({ statusDesc: "CLOSED" })]),
        now: NOW,
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "unmatched",
      reason: "no_candidate_committee_match",
    });

    await expect(
      autoLinkNorthCarolinaCandidateFinanceForCandidateElection({
        db,
        candidateElection: CANDIDATE_ELECTION,
        loadCandidateSearchRows: loader([
          searchRow(),
          searchRow({ orgName: "JANE DOE FOR NC", sboeId: "STA-ZZ99XX-C-001", orgGroupId: 54321 }),
        ]),
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

  it("pages through the whole window so unmatched rows cannot starve later candidates", async () => {
    const unmatchableRow = {
      candidate_id: CANDIDATE_ID,
      election_id: ELECTION_ID,
      candidate_name: "No Committee",
      election_year: 2026,
      election_date: "2026-11-03",
      office_scope: "state_lower",
      office_name: "State Lower Chamber Legislator",
      district: "12",
    };
    const matchableRow = {
      candidate_id: "55555555-5555-4555-8555-555555555555",
      election_id: "66666666-6666-4666-8666-666666666666",
      candidate_name: "Jane Doe",
      election_year: 2026,
      election_date: "2026-11-03",
      office_scope: "state_lower",
      office_name: "State Lower Chamber Legislator",
      district: "27",
    };
    const db = {
      query: vi
        .fn()
        .mockResolvedValueOnce({ rows: [unmatchableRow], rowCount: 1 })
        .mockResolvedValueOnce({ rows: [matchableRow], rowCount: 1 })
        .mockResolvedValueOnce({ rows: [{ id: "77777777-7777-4777-8777-777777777777" }], rowCount: 1 })
        .mockResolvedValueOnce({ rows: [], rowCount: 0 }),
    };

    const results = await autoLinkMissingNorthCarolinaCandidateFinanceLinks({
      db,
      now: NOW,
      maxCandidates: 1,
      electionLookbackDays: 1,
      electionLookaheadDays: 730,
      loadCandidateSearchRows: loader([searchRow()]),
    });

    expect(results).toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        status: "unmatched",
        reason: "no_candidate_committee_match",
      },
      {
        candidateId: "55555555-5555-4555-8555-555555555555",
        electionId: "66666666-6666-4666-8666-666666666666",
        status: "linked",
        committeeId: "STA-AB12CD-C-001",
        orgGroupId: 12345,
      },
    ]);

    // Page 2's query resumed strictly after page 1's last row.
    const secondListParams = db.query.mock.calls[1]?.[1] as unknown[];
    expect(secondListParams?.slice(5)).toEqual(["2026-11-03", ELECTION_ID, CANDIDATE_ID]);
    // First page had no cursor.
    const firstListParams = db.query.mock.calls[0]?.[1] as unknown[];
    expect(firstListParams?.slice(5)).toEqual([null, null, null]);
    expect(db.query).toHaveBeenCalledTimes(4);
  });

  it("continues past a per-candidate loader failure and reports it", async () => {
    const db = createMockDb([{ id: "33333333-3333-4333-8333-333333333333" }]);
    const loadCandidateSearchRows = vi
      .fn()
      .mockRejectedValueOnce(new Error("portal unreachable"))
      .mockResolvedValueOnce({ rows: [searchRow()], sourceUrl: SOURCE_URL });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      await expect(
        autoLinkMissingNorthCarolinaCandidateFinanceLinks({
          db,
          now: NOW,
          maxCandidates: 25,
          electionLookbackDays: 1,
          electionLookaheadDays: 730,
          loadCandidateSearchRows,
          candidateElections: [
            CANDIDATE_ELECTION,
            { ...CANDIDATE_ELECTION, electionId: "44444444-4444-4444-8444-444444444444" },
          ],
        })
      ).resolves.toEqual([
        {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          status: "error",
          reason: "auto_link_failed",
          error: "portal unreachable",
        },
        {
          candidateId: CANDIDATE_ID,
          electionId: "44444444-4444-4444-8444-444444444444",
          status: "linked",
          committeeId: "STA-AB12CD-C-001",
          orgGroupId: 12345,
        },
      ]);
    } finally {
      warn.mockRestore();
    }
  });
});
