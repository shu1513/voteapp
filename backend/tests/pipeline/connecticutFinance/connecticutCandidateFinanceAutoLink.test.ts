import { describe, expect, it, vi } from "vitest";

import {
  autoLinkConnecticutCandidateFinanceForCandidateElection,
  autoLinkMissingConnecticutCandidateFinanceLinks,
  listConnecticutCandidateElectionsMissingFinanceLinks,
} from "../../../src/pipeline/connecticutFinance/connecticutCandidateFinanceAutoLink.js";
import type { ConnecticutEcrisArtifactRow } from "../../../src/pipeline/connecticutFinance/connecticutEcrisArtifactReader.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2026-06-01T00:00:00.000Z");

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows, rowCount: rows.length }),
  };
}

function receipt(overrides: Partial<ConnecticutEcrisArtifactRow> = {}): ConnecticutEcrisArtifactRow {
  return {
    Committee: "ACKERT FOR THE 8TH",
    "Contributor Name": "Carolyn Gerrity",
    District: "8",
    "Office Sought": "State Representative",
    Employer: "RTX-Pratt Whitney",
    "Receipt Type": "Itemized Contributions from Individuals",
    "Committee Type": "Candidate Committee",
    "Transaction Date": "03/31/2026",
    "File To State": "04/01/2026",
    Amount: "50.00",
    "Receipt State": "Original",
    Occupation: "Business Manager",
    ElectionYear: "2026",
    "Committee ID": "14376",
    "Candidate First Name": "Timothy",
    "Candidate Middle Intial": "J",
    "Candidate Last Name": "Ackert",
    ...overrides,
  };
}

describe("connecticutCandidateFinanceAutoLink", () => {
  it("lists eligible Connecticut candidate elections missing active finance links", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Timothy Ackert",
        election_year: 2026,
        office_name: "State Lower Chamber Legislator",
        district: "8",
      },
    ]);

    await expect(
      listConnecticutCandidateElectionsMissingFinanceLinks(db, {
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Timothy Ackert",
        electionYear: 2026,
        officeName: "State Lower Chamber Legislator",
        district: "8",
      },
    ]);

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.candidate_elections AS candidate_election");
    expect(sql).toContain("district.state = 'CT'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($5::text[])");
    expect(sql).toContain("FROM public.ct_candidate_finance_links AS link");
    expect(sql).toContain("regexp_replace");
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

  it("links a matched candidate election to the resolved eCRIS committee", async () => {
    const db = createMockDb([{ id: "link-1" }]);

    await expect(
      autoLinkConnecticutCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        sourceUrl: "https://seec.ct.gov/portal/ecris/CurPreYears",
        receiptRows: [receipt()],
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Timothy Ackert",
          electionYear: 2026,
          officeName: "State Lower Chamber Legislator",
          district: "8",
        },
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "linked",
      committeeId: "14376",
    });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ct_candidate_finance_links");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "TIMOTHY ACKERT",
      "State Lower Chamber Legislator",
      "8",
      "14376",
      "ACKERT FOR THE 8TH",
      "active",
      "ecris_bulk",
      "https://seec.ct.gov/portal/ecris/CurPreYears",
      "2026-06-01T00:00:00.000Z",
    ]);
  });

  it("does not write a link when committee resolution is ambiguous", async () => {
    const db = createMockDb();

    await expect(
      autoLinkConnecticutCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        sourceUrl: "https://seec.ct.gov/portal/ecris/CurPreYears",
        receiptRows: [
          receipt(),
          receipt({
            Committee: "FRIENDS OF TIM ACKERT",
            "Committee ID": "99999",
          }),
        ],
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Timothy Ackert",
          electionYear: 2026,
          officeName: "State Lower Chamber Legislator",
          district: "8",
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

  it("uses provided candidate elections without querying when auto-linking a batch", async () => {
    const db = createMockDb([{ id: "link-1" }]);

    await expect(
      autoLinkMissingConnecticutCandidateFinanceLinks({
        db,
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
        candidateElections: [
          {
            candidateId: CANDIDATE_ID,
            electionId: ELECTION_ID,
            candidateName: "Timothy Ackert",
            electionYear: 2026,
            officeName: "State Lower Chamber Legislator",
            district: "8",
          },
        ],
        receiptRowsByYear: new Map([[2026, [receipt()]]]),
        sourceUrlByYear: new Map([[2026, "https://seec.ct.gov/portal/ecris/CurPreYears"]]),
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        status: "linked",
        committeeId: "14376",
      },
    ]);

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ct_candidate_finance_links");
  });

  it("continues auto-linking later candidates when one candidate write fails", async () => {
    const db = {
      query: vi
        .fn()
        .mockRejectedValueOnce(new Error("temporary write failure"))
        .mockResolvedValueOnce({ rows: [{ id: "link-2" }], rowCount: 1 }),
    };
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

    try {
      await expect(
        autoLinkMissingConnecticutCandidateFinanceLinks({
          db,
          now: NOW,
          maxCandidates: 25,
          electionLookbackDays: 1,
          electionLookaheadDays: 730,
          candidateElections: [
            {
              candidateId: CANDIDATE_ID,
              electionId: ELECTION_ID,
              candidateName: "Timothy Ackert",
              electionYear: 2026,
              officeName: "State Lower Chamber Legislator",
              district: "8",
            },
            {
              candidateId: "33333333-3333-4333-8333-333333333333",
              electionId: "44444444-4444-4444-8444-444444444444",
              candidateName: "Timothy Ackert",
              electionYear: 2026,
              officeName: "State Lower Chamber Legislator",
              district: "8",
            },
          ],
          receiptRowsByYear: new Map([[2026, [receipt()]]]),
          sourceUrlByYear: new Map([[2026, "https://seec.ct.gov/portal/ecris/CurPreYears"]]),
        })
      ).resolves.toEqual([
        {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          status: "error",
          reason: "auto_link_failed",
          error: "temporary write failure",
        },
        {
          candidateId: "33333333-3333-4333-8333-333333333333",
          electionId: "44444444-4444-4444-8444-444444444444",
          status: "linked",
          committeeId: "14376",
        },
      ]);

      expect(db.query).toHaveBeenCalledTimes(2);
      expect(warnSpy).toHaveBeenCalledWith(
        "Connecticut finance auto-link failed for candidate election; continuing:",
        expect.objectContaining({
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          error: "temporary write failure",
        })
      );
    } finally {
      warnSpy.mockRestore();
    }
  });
});
