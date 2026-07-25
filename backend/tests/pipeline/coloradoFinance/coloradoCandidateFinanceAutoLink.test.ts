import { describe, expect, it, vi } from "vitest";

import {
  autoLinkColoradoCandidateFinanceForCandidateElection,
  autoLinkMissingColoradoCandidateFinanceLinks,
  buildColoradoCandidateNamePredicate,
  listColoradoCandidateElectionsMissingFinanceLinks,
} from "../../../src/pipeline/coloradoFinance/coloradoCandidateFinanceAutoLink.js";
import type { ColoradoTracerContributionRow } from "../../../src/pipeline/coloradoFinance/coloradoTracerContributionReader.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2026-06-01T00:00:00.000Z");

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows, rowCount: rows.length }),
  };
}

function contribution(overrides: Partial<ColoradoTracerContributionRow> = {}): ColoradoTracerContributionRow {
  return {
    CO_ID: "202650001",
    ContributionAmount: "100.00",
    ContributionDate: "01/10/2026",
    LastName: "Doe",
    FirstName: "Jane",
    MI: "",
    Suffix: "",
    Address1: "",
    Address2: "",
    City: "Denver",
    State: "CO",
    Zip: "80203",
    Explanation: "",
    RecordID: "R1",
    FiledDate: "02/01/2026",
    ContributionType: "Monetary",
    ReceiptType: "Contribution",
    ContributorType: "Individual",
    Electioneering: "",
    CommitteeType: "Candidate Committee",
    CommitteeName: "Jane Doe for Colorado Governor",
    CandidateName: "Jane Doe",
    Employer: "Acme Inc",
    Occupation: "Engineer",
    Amended: "False",
    Amendment: "",
    AmendedRecordID: "",
    Jurisdiction: "STATEWIDE",
    OccupationComments: "",
    ...overrides,
  };
}

describe("coloradoCandidateFinanceAutoLink", () => {
  it("lists eligible Colorado candidate elections missing active finance links", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Jane Doe",
        election_year: 2026,
        office_name: "Governor",
      },
    ]);

    await expect(
      listColoradoCandidateElectionsMissingFinanceLinks(db, {
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Jane Doe",
        electionYear: 2026,
        officeName: "Governor",
      },
    ]);

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.candidate_elections AS candidate_election");
    expect(sql).toContain("district.state = 'CO'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($5::text[])");
    expect(sql).toContain("FROM public.co_candidate_finance_links AS link");
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

  it("links a matched candidate election to the resolved TRACER committee", async () => {
    const db = createMockDb([{ id: "link-1" }]);

    await expect(
      autoLinkColoradoCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        sourceUrl: "https://tracer.sos.colorado.gov/",
        contributionRows: [contribution()],
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Jane Doe",
          electionYear: 2026,
          officeName: "Governor",
        },
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "linked",
      committeeId: "202650001",
    });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.co_candidate_finance_links");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "JANE DOE",
      "Governor",
      null,
      "202650001",
      "Jane Doe for Colorado Governor",
      "active",
      "tracer_bulk",
      "https://tracer.sos.colorado.gov/",
      "2026-06-01T00:00:00.000Z",
    ]);
  });

  it("does not write a link when committee resolution is ambiguous", async () => {
    const db = createMockDb();

    await expect(
      autoLinkColoradoCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        sourceUrl: "https://tracer.sos.colorado.gov/",
        contributionRows: [
          contribution(),
          contribution({
            CO_ID: "202650002",
            CommitteeName: "Coloradans for Jane Doe",
            CandidateName: "Jane Doe",
          }),
        ],
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Jane Doe",
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

  it("uses provided candidate elections without querying when auto-linking a batch", async () => {
    const db = createMockDb([{ id: "link-1" }]);

    await expect(
      autoLinkMissingColoradoCandidateFinanceLinks({
        db,
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
        candidateElections: [
          {
            candidateId: CANDIDATE_ID,
            electionId: ELECTION_ID,
            candidateName: "Jane Doe",
            electionYear: 2026,
            officeName: "Governor",
          },
        ],
        contributionRowsByYear: new Map([[2026, [contribution()]]]),
        sourceUrlByYear: new Map([[2026, "https://tracer.sos.colorado.gov/"]]),
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        status: "linked",
        committeeId: "202650001",
      },
    ]);

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.co_candidate_finance_links");
  });

  it("reports a per-candidate error and keeps linking later candidates when one write fails", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const db = {
        query: vi
          .fn()
          .mockRejectedValueOnce(new Error("insert failed"))
          .mockResolvedValueOnce({ rows: [{ id: "link-2" }], rowCount: 1 }),
      };

      const results = await autoLinkMissingColoradoCandidateFinanceLinks({
        db,
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
        candidateElections: [
          {
            candidateId: CANDIDATE_ID,
            electionId: ELECTION_ID,
            candidateName: "Jane Doe",
            electionYear: 2026,
            officeName: "Governor",
          },
          {
            candidateId: "33333333-3333-4333-8333-333333333333",
            electionId: "44444444-4444-4444-8444-444444444444",
            candidateName: "John Roe",
            electionYear: 2026,
            officeName: "Governor",
          },
        ],
        contributionRowsByYear: new Map([
          [
            2026,
            [
              contribution(),
              contribution({
                CO_ID: "202650002",
                CommitteeName: "John Roe for Colorado Governor",
                CandidateName: "John Roe",
                LastName: "Roe",
                FirstName: "John",
              }),
            ],
          ],
        ]),
        sourceUrlByYear: new Map([[2026, "https://tracer.sos.colorado.gov/"]]),
      });

      expect(results).toEqual([
        {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          status: "error",
          reason: "auto_link_failed",
          error: "insert failed",
        },
        {
          candidateId: "33333333-3333-4333-8333-333333333333",
          electionId: "44444444-4444-4444-8444-444444444444",
          status: "linked",
          committeeId: "202650002",
        },
      ]);
    } finally {
      warnSpy.mockRestore();
    }
  });

  it("builds a candidate-name predicate for filtered TRACER reads", () => {
    const predicate = buildColoradoCandidateNamePredicate([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Jane Doe",
        electionYear: 2026,
        officeName: "Governor",
      },
    ]);

    expect(predicate(contribution({ CandidateName: "DOE, JANE" }))).toBe(true);
    expect(predicate(contribution({ CandidateName: "Other Person" }))).toBe(false);
  });
});
