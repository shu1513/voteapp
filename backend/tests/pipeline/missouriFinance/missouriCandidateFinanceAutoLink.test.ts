import { describe, expect, it, vi } from "vitest";

import {
  autoLinkMissingMissouriCandidateFinanceLinks,
  autoLinkMissouriCandidateFinanceForCandidateElection,
  listMissouriCandidateElectionsMissingFinanceLinks,
  type MissouriFinanceAutoLinkCandidateElection,
} from "../../../src/pipeline/missouriFinance/missouriCandidateFinanceAutoLink.js";
import type {
  MissouriCandidateCommitteeResolution,
  MissouriMecCandidateCommitteeRecord,
} from "../../../src/pipeline/missouriFinance/missouriCandidateCommitteeResolver.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2026-08-19T00:00:00.000Z");
const SOURCE_URL = "https://www.mec.mo.gov/MEC/Campaign_Finance/CommInfo.aspx?MECID=C221944";

function candidateElection(
  overrides: Partial<MissouriFinanceAutoLinkCandidateElection> = {}
): MissouriFinanceAutoLinkCandidateElection {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    candidateName: "Jeff Farnan",
    electionDate: "2026-11-03",
    electionYear: 2026,
    officeScope: "state_lower",
    officeName: "State Lower Chamber Legislator",
    ballotTitle: "State Representative",
    districtName: "State House District 1 (2024); Missouri",
    legislativeDistrict: "1",
    ...overrides,
  };
}

function matchedResolution(): Extract<MissouriCandidateCommitteeResolution, { status: "matched" }> {
  return {
    status: "matched",
    mecid: "C221944",
    committeeName: "Forward With Farnan",
    candidateName: "Jeff Farnan",
    officeSought: "State Representative - District 1 - Missouri House of Representatives",
    confidence: "election_history_exact",
    source: "mec_portal",
    sourceUrl: SOURCE_URL,
    matchedCandidateRowCount: 1,
  };
}

function sourceRecord(): MissouriMecCandidateCommitteeRecord {
  return {
    mecid: "C221944",
    committeeName: "Forward With Farnan",
    candidateName: "Jeff Farnan",
    party: "R",
    officeSought: "State Representative - District 1 - Missouri House of Representatives",
    status: "A",
    searchElectionDate: "2026-11-03",
    searchPoliticalOffice: "State Representative",
    searchPoliticalSubdivision: null,
    searchPoliticalDistrict: "District 1",
    committeeInfo: {
      mecid: "C221944",
      committeeName: "Forward With Farnan",
      candidateName: "Jeff Farnan",
      sourceUrl: SOURCE_URL,
      electionHistory: [
        {
          electionDate: "2026-11-03",
          electionType: "General Election",
          office: "State Representative",
          politicalSubdivision: "Missouri House of Representatives",
        },
      ],
    },
  };
}

describe("missouriCandidateFinanceAutoLink", () => {
  it("lists rostered eligible Missouri candidates missing active finance links", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({
        rows: [
          {
            candidate_id: CANDIDATE_ID,
            election_id: ELECTION_ID,
            candidate_name: "Jeff Farnan",
            election_date: "2026-11-03T00:00:00.000Z",
            election_year: 2026,
            office_scope: "state_lower",
            office_name: "State Lower Chamber Legislator",
            ballot_title: "State Representative",
            district_name: "State House District 1 (2024); Missouri",
            legislative_district: "1",
          },
        ],
      }),
    };

    await expect(
      listMissouriCandidateElectionsMissingFinanceLinks(db, {
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 30,
        electionLookaheadDays: 365,
      })
    ).resolves.toEqual([candidateElection()]);

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("district.state = 'MO'");
    expect(sql).toContain("FROM public.mo_candidate_finance_links AS link");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($5::text[])");
    expect(db.query.mock.calls[0]?.[1]?.[4]).toEqual(
      expect.arrayContaining([
        "state_lower::State Lower Chamber Legislator",
        "state_upper::State Senator",
        "county::County Executive",
        "place::City Council Member",
        "school_unified::School Board Member",
      ])
    );
    expect(db.query.mock.calls[0]?.[1]?.[4]).not.toContain("statewide::United States Senator");
  });

  it("writes an exact MEC match through the manual-protected Missouri writer", async () => {
    const db = {
      query: vi.fn((sql: unknown) => {
        if (String(sql).includes("INSERT INTO public.mo_candidate_finance_links")) {
          return Promise.resolve({ rows: [{ id: "link-1" }], rowCount: 1 });
        }
        return Promise.resolve({ rows: [], rowCount: 0 });
      }),
    };
    const resolveCandidateCommittee = vi.fn(async () => matchedResolution());

    await expect(
      autoLinkMissouriCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        candidateElection: candidateElection(),
        resolveCandidateCommittee,
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "linked",
      mecid: "C221944",
    });

    expect(resolveCandidateCommittee).toHaveBeenCalledWith(
      {
        candidateName: "Jeff Farnan",
        electionDate: "2026-11-03",
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        ballotTitle: "State Representative",
        districtName: "State House District 1 (2024); Missouri",
        legislativeDistrict: "1",
      },
      undefined
    );
    const insert = db.query.mock.calls.find((call) => String(call[0]).includes("INSERT INTO"));
    expect(insert?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "JEFF FARNAN",
      "State Lower Chamber Legislator",
      "1",
      "C221944",
      "Forward With Farnan",
      "active",
      "mec_portal",
      SOURCE_URL,
      NOW.toISOString(),
    ]);
  });

  it("does not write when resolution is ambiguous", async () => {
    const db = { query: vi.fn() };
    const resolveCandidateCommittee = vi.fn(async (): Promise<MissouriCandidateCommitteeResolution> => ({
      status: "ambiguous",
      reason: "multiple_matching_committees",
      candidateNameNormalized: "JEFF FARNAN",
      officeNameNormalized: "STATE REPRESENTATIVE",
      matches: [matchedResolution(), { ...matchedResolution(), mecid: "C260002" }],
    }));
    await expect(
      autoLinkMissouriCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        candidateElection: candidateElection(),
        resolveCandidateCommittee,
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "ambiguous",
      reason: "multiple_matching_committees",
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("fetches one source partition for every candidate in the same race", async () => {
    const db = {
      query: vi.fn((sql: unknown) => {
        if (String(sql).includes("INSERT INTO public.mo_candidate_finance_links")) {
          return Promise.resolve({ rows: [{ id: "link-1" }], rowCount: 1 });
        }
        return Promise.resolve({ rows: [], rowCount: 0 });
      }),
    };
    const searchCandidateCommitteeRecords = vi.fn(async () => [sourceRecord()]);

    await expect(
      autoLinkMissingMissouriCandidateFinanceLinks({
        db,
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 30,
        electionLookaheadDays: 365,
        candidateElections: [
          candidateElection(),
          candidateElection({
            candidateId: "33333333-3333-4333-8333-333333333333",
            electionId: "44444444-4444-4444-8444-444444444444",
          }),
        ],
        searchCandidateCommitteeRecords,
      })
    ).resolves.toEqual([
      { candidateId: CANDIDATE_ID, electionId: ELECTION_ID, status: "linked", mecid: "C221944" },
      {
        candidateId: "33333333-3333-4333-8333-333333333333",
        electionId: "44444444-4444-4444-8444-444444444444",
        status: "linked",
        mecid: "C221944",
      },
    ]);
    expect(searchCandidateCommitteeRecords).toHaveBeenCalledTimes(1);
  });

  it("isolates a protected-manual-link failure and continues the batch", async () => {
    const db = {
      query: vi
        .fn()
        .mockRejectedValueOnce(new Error("Missouri automatic finance link conflicts with protected manual link"))
        .mockResolvedValueOnce({ rows: [], rowCount: 0 })
        .mockResolvedValueOnce({ rows: [{ id: "link-2" }], rowCount: 1 }),
    };
    const resolveCandidateCommittee = vi.fn(async () => matchedResolution());
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      await expect(
        autoLinkMissingMissouriCandidateFinanceLinks({
          db,
          now: NOW,
          maxCandidates: 25,
          electionLookbackDays: 30,
          electionLookaheadDays: 365,
          candidateElections: [
            candidateElection(),
            candidateElection({
              candidateId: "33333333-3333-4333-8333-333333333333",
              electionId: "44444444-4444-4444-8444-444444444444",
            }),
          ],
          resolveCandidateCommittee,
        })
      ).resolves.toEqual([
        {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          status: "error",
          reason: "auto_link_failed",
          error: "Missouri automatic finance link conflicts with protected manual link",
        },
        {
          candidateId: "33333333-3333-4333-8333-333333333333",
          electionId: "44444444-4444-4444-8444-444444444444",
          status: "linked",
          mecid: "C221944",
        },
      ]);
      expect(warn).toHaveBeenCalledTimes(1);
    } finally {
      warn.mockRestore();
    }
  });
});
