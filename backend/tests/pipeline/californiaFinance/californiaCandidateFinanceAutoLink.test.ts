import { describe, expect, it, vi } from "vitest";

import {
  autoLinkCaliforniaCandidateFinanceForCandidateElection,
  autoLinkMissingCaliforniaCandidateFinanceLinks,
  listCaliforniaCandidateElectionsMissingFinanceLinks,
} from "../../../src/pipeline/californiaFinance/californiaCandidateFinanceAutoLink.js";
import type { CalAccessCommitteeResolutionData } from "../../../src/pipeline/californiaFinance/calAccessRawDataLoader.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2026-06-01T00:00:00.000Z");

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows, rowCount: rows.length }),
  };
}

function resolutionData(
  overrides: Partial<CalAccessCommitteeResolutionData> = {}
): CalAccessCommitteeResolutionData {
  return {
    zipPath: "/tmp/dbwebexport.zip",
    sourceUrl: "https://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip",
    campaignCoverRows: [
      {
        FILING_ID: "F1",
        FILER_ID: "1456045",
        FILER_NAML: "Newsom for California Governor 2026",
        ELECT_DATE: "11/3/2026 12:00:00 AM",
        CAND_NAML: "NEWSOM",
        CAND_NAMF: "GAVIN",
        CAND_NAMT: "",
        OFFICE_CD: "GOV",
        OFFIC_DSCR: "Governor",
      },
    ],
    filerNameRows: [],
    ...overrides,
  };
}

describe("californiaCandidateFinanceAutoLink", () => {
  it("lists eligible California candidate elections missing active finance links", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Gavin Newsom",
        election_year: 2026,
        office_name: "Governor",
      },
    ]);

    await expect(
      listCaliforniaCandidateElectionsMissingFinanceLinks(db, {
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Gavin Newsom",
        electionYear: 2026,
        officeName: "Governor",
      },
    ]);

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.candidate_elections AS candidate_election");
    expect(sql).toContain("district.state = 'CA'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($5::text[])");
    expect(sql).toContain("NOT EXISTS");
    expect(sql).toContain("FROM public.ca_candidate_finance_links AS link");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      25,
      1,
      730,
      expect.arrayContaining([
        "statewide::Governor",
        "statewide::Attorney General",
        "state_upper::State Senator",
        "state_lower::State Lower Chamber Legislator",
      ]),
    ]);
    expect(db.query.mock.calls[0]?.[1]?.[4]).not.toContain("statewide::United States Senator");
  });

  it("links a matched candidate election to the resolved CAL-ACCESS controlled committee", async () => {
    const db = createMockDb([{ id: "link-1" }]);

    await expect(
      autoLinkCaliforniaCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        resolutionData: resolutionData(),
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Gavin Newsom",
          electionYear: 2026,
          officeName: "Governor",
        },
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "linked",
      controlledCommitteeId: "1456045",
    });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ca_candidate_finance_links");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "GAVIN NEWSOM",
      "Governor",
      "1456045",
      "Newsom for California Governor 2026",
      "active",
      "cal_access",
      "https://campaignfinance.cdn.sos.ca.gov/dbwebexport.zip",
      "2026-06-01T00:00:00.000Z",
    ]);
  });

  it("does not write a link when committee resolution is ambiguous", async () => {
    const db = createMockDb();

    await expect(
      autoLinkCaliforniaCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        resolutionData: resolutionData({
          campaignCoverRows: [
            ...resolutionData().campaignCoverRows,
            {
              FILING_ID: "F2",
              FILER_ID: "1999999",
              FILER_NAML: "Californians for Newsom 2026",
              ELECT_DATE: "11/3/2026 12:00:00 AM",
              CAND_NAML: "NEWSOM",
              CAND_NAMF: "GAVIN",
              CAND_NAMT: "",
              OFFICE_CD: "GOV",
              OFFIC_DSCR: "Governor",
            },
          ],
        }),
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Gavin Newsom",
          electionYear: 2026,
          officeName: "Governor",
        },
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "ambiguous",
      reason: "multiple_matching_committees",
    });

    expect(db.query).not.toHaveBeenCalled();
  });

  it("does not write a link when committee resolution is unmatched", async () => {
    const db = createMockDb();

    await expect(
      autoLinkCaliforniaCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        resolutionData: resolutionData(),
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Rob Bonta",
          electionYear: 2026,
          officeName: "Attorney General",
        },
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "unmatched",
      reason: "no_candidate_office_year_match",
    });

    expect(db.query).not.toHaveBeenCalled();
  });

  it("returns no results when no CAL-ACCESS resolution data is available", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Gavin Newsom",
        election_year: 2026,
        office_name: "Governor",
      },
    ]);

    await expect(
      autoLinkMissingCaliforniaCandidateFinanceLinks({
        db,
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
        resolutionData: null,
      })
    ).resolves.toEqual([]);

    expect(db.query).not.toHaveBeenCalled();
  });

  it("lists missing links and auto-links matched candidates when resolution data is available", async () => {
    const db = {
      query: vi
        .fn()
        .mockResolvedValueOnce({
          rows: [
            {
              candidate_id: CANDIDATE_ID,
              election_id: ELECTION_ID,
              candidate_name: "Gavin Newsom",
              election_year: 2026,
              office_name: "Governor",
            },
          ],
          rowCount: 1,
        })
        .mockResolvedValueOnce({ rows: [{ id: "link-1" }], rowCount: 1 }),
    };

    await expect(
      autoLinkMissingCaliforniaCandidateFinanceLinks({
        db,
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
        resolutionData: resolutionData(),
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        status: "linked",
        controlledCommitteeId: "1456045",
      },
    ]);

    expect(db.query).toHaveBeenCalledTimes(2);
    expect(String(db.query.mock.calls[0]?.[0])).toContain(
      "FROM public.candidate_elections AS candidate_election"
    );
    expect(String(db.query.mock.calls[1]?.[0])).toContain("INSERT INTO public.ca_candidate_finance_links");
  });
});
