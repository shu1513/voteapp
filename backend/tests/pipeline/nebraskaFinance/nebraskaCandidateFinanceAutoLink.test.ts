import { describe, expect, it, vi } from "vitest";

import {
  autoLinkMissingNebraskaCandidateFinanceLinks,
  autoLinkNebraskaCandidateFinanceForCandidateElection,
  buildNebraskaCandidateNamePredicate,
  listNebraskaCandidateElectionsMissingFinanceLinks,
} from "../../../src/pipeline/nebraskaFinance/nebraskaCandidateFinanceAutoLink.js";
import type { NebraskaNadcContributionRow } from "../../../src/pipeline/nebraskaFinance/nebraskaNadcArtifactReader.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2026-06-01T00:00:00.000Z");

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows, rowCount: rows.length }),
  };
}

function contribution(overrides: Partial<NebraskaNadcContributionRow> = {}): NebraskaNadcContributionRow {
  return {
    "Receipt ID": "R1",
    "Org ID": "7569",
    "Filer Type": "Candidate Committee",
    "Filer Name": "VOTE VEST",
    "Candidate Name": "Rick Vest",
    "Receipt Transaction/Contribution Type": "Monetary Contribution",
    "Other Funds Type": "",
    "Receipt Date": "01/10/2026",
    "Receipt Amount": "100.00",
    Description: "",
    "Contributor or Transaction Source Type": "Individual",
    "Contributor or Source Name (Individual Last Name)": "Doe",
    "First Name": "Jane",
    "Middle Name": "",
    Suffix: "",
    "Address 1": "",
    "Address 2": "",
    City: "Lincoln",
    State: "NE",
    Zip: "68508",
    "Filed Date": "02/01/2026",
    Amended: "False",
    Employer: "Acme Inc",
    Occupation: "Attorney",
    ...overrides,
  };
}

describe("nebraskaCandidateFinanceAutoLink", () => {
  it("lists eligible Nebraska candidate elections missing active finance links", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Rick Vest",
        election_year: 2026,
        office_scope: "state_upper",
        office_name: "State Senator",
        district: "30",
      },
    ]);

    await expect(
      listNebraskaCandidateElectionsMissingFinanceLinks(db, {
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Rick Vest",
        electionYear: 2026,
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "30",
      },
    ]);

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.candidate_elections AS candidate_election");
    expect(sql).toContain("district.state = 'NE'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($5::text[])");
    expect(sql).toContain("FROM public.ne_candidate_finance_links AS link");
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
      ]),
    ]);
    expect(db.query.mock.calls[0]?.[1]?.[4]).not.toContain("state_lower::State Lower Chamber Legislator");
  });

  it("links a matched candidate election to the resolved NADC committee", async () => {
    const db = createMockDb([{ id: "link-1" }]);

    await expect(
      autoLinkNebraskaCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        sourceUrl: "https://nadc-e.nebraska.gov/PublicSite/DataDownload.aspx",
        contributionRows: [contribution()],
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Rick Vest",
          electionYear: 2026,
          officeScope: "state_upper",
          officeName: "State Senator",
          district: "30",
        },
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "linked",
      committeeId: "7569",
    });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ne_candidate_finance_links");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "RICK VEST",
      "State Senator",
      "30",
      "7569",
      "VOTE VEST",
      "active",
      "nadc_bulk",
      "https://nadc-e.nebraska.gov/PublicSite/DataDownload.aspx",
      "2026-06-01T00:00:00.000Z",
    ]);
  });

  it("does not write a link when committee resolution is ambiguous", async () => {
    const db = createMockDb();

    await expect(
      autoLinkNebraskaCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        sourceUrl: "https://nadc-e.nebraska.gov/PublicSite/DataDownload.aspx",
        contributionRows: [
          contribution(),
          contribution({
            "Org ID": "9999",
            "Filer Name": "FRIENDS OF RICK VEST",
          }),
        ],
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Rick Vest",
          electionYear: 2026,
          officeScope: "state_upper",
          officeName: "State Senator",
          district: "30",
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
      autoLinkMissingNebraskaCandidateFinanceLinks({
        db,
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
        candidateElections: [
          {
            candidateId: CANDIDATE_ID,
            electionId: ELECTION_ID,
            candidateName: "Rick Vest",
            electionYear: 2026,
            officeScope: "state_upper",
            officeName: "State Senator",
            district: "30",
          },
        ],
        contributionRowsByYear: new Map([[2026, [contribution()]]]),
        sourceUrlByYear: new Map([[2026, "https://nadc-e.nebraska.gov/PublicSite/DataDownload.aspx"]]),
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        status: "linked",
        committeeId: "7569",
      },
    ]);

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ne_candidate_finance_links");
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
        autoLinkMissingNebraskaCandidateFinanceLinks({
          db,
          now: NOW,
          maxCandidates: 25,
          electionLookbackDays: 1,
          electionLookaheadDays: 730,
          candidateElections: [
            {
              candidateId: CANDIDATE_ID,
              electionId: ELECTION_ID,
              candidateName: "Rick Vest",
              electionYear: 2026,
              officeScope: "state_upper",
              officeName: "State Senator",
              district: "30",
            },
            {
              candidateId: "33333333-3333-4333-8333-333333333333",
              electionId: "44444444-4444-4444-8444-444444444444",
              candidateName: "Rick Vest",
              electionYear: 2026,
              officeScope: "state_upper",
              officeName: "State Senator",
              district: "30",
            },
          ],
          contributionRowsByYear: new Map([[2026, [contribution()]]]),
          sourceUrlByYear: new Map([[2026, "https://nadc-e.nebraska.gov/PublicSite/DataDownload.aspx"]]),
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
          committeeId: "7569",
        },
      ]);
    } finally {
      warnSpy.mockRestore();
    }

    expect(db.query).toHaveBeenCalledTimes(2);
  });

  it("builds a candidate-name predicate for filtered NADC reads", () => {
    const predicate = buildNebraskaCandidateNamePredicate([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Rick Vest",
        electionYear: 2026,
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "30",
      },
    ]);

    expect(predicate(contribution({ "Candidate Name": "VEST, RICK" }))).toBe(true);
    expect(predicate(contribution({ "Candidate Name": "Other Person" }))).toBe(false);
  });
});
