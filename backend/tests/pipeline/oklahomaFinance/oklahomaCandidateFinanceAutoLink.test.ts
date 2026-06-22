import { describe, expect, it, vi } from "vitest";

import {
  autoLinkMissingOklahomaCandidateFinanceLinks,
  autoLinkOklahomaCandidateFinanceForCandidateElection,
  buildOklahomaCandidateNamePredicate,
  listOklahomaCandidateElectionsMissingFinanceLinks,
} from "../../../src/pipeline/oklahomaFinance/oklahomaCandidateFinanceAutoLink.js";
import type { OklahomaGuardianContributionRow } from "../../../src/pipeline/oklahomaFinance/oklahomaGuardianContributionReader.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2026-06-01T00:00:00.000Z");

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows, rowCount: rows.length }),
  };
}

function contribution(overrides: Partial<OklahomaGuardianContributionRow> = {}): OklahomaGuardianContributionRow {
  return {
    "Receipt ID": "R1",
    "Org ID": "11954",
    "Receipt Type": "Contribution",
    "Receipt Date": "01/10/2026",
    "Receipt Amount": "100.00",
    Description: "",
    "Receipt Source Type": "Individual",
    "Last Name": "Doe",
    "First Name": "Jane",
    "Middle Name": "",
    Suffix: "",
    "Address 1": "",
    "Address 2": "",
    City: "Oklahoma City",
    State: "OK",
    Zip: "73102",
    "Filed Date": "02/01/2026",
    "Committee Type": "Candidate Committee",
    "Committee Name": "Dishman for Senate",
    "Candidate Name": "C. Brent Dishman",
    Amended: "",
    Employer: "Acme Inc",
    Occupation: "Attorney",
    ...overrides,
  };
}

describe("oklahomaCandidateFinanceAutoLink", () => {
  it("lists eligible Oklahoma candidate elections missing active finance links", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Brent Dishman",
        election_year: 2026,
        office_scope: "state_upper",
        office_name: "State Senator",
        district: "47",
      },
    ]);

    await expect(
      listOklahomaCandidateElectionsMissingFinanceLinks(db, {
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Brent Dishman",
        electionYear: 2026,
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "47",
      },
    ]);

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.candidate_elections AS candidate_election");
    expect(sql).toContain("district.state = 'OK'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($5::text[])");
    expect(sql).toContain("FROM public.ok_candidate_finance_links AS link");
    expect(sql).toContain("district.district_type IN ('state_upper', 'state_lower')");
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
  });

  it("links a matched candidate election to the resolved Guardian committee", async () => {
    const db = createMockDb([{ id: "link-1" }]);

    await expect(
      autoLinkOklahomaCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        sourceUrl: "https://guardian.ok.gov/PublicSite/DataDownload.aspx",
        contributionRows: [contribution()],
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Brent Dishman",
          electionYear: 2026,
          officeScope: "state_upper",
          officeName: "State Senator",
          district: "47",
        },
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "linked",
      committeeId: "11954",
    });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ok_candidate_finance_links");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "BRENT DISHMAN",
      "State Senator",
      "47",
      "11954",
      "Dishman for Senate",
      "active",
      "guardian_bulk",
      "https://guardian.ok.gov/PublicSite/DataDownload.aspx",
      "2026-06-01T00:00:00.000Z",
    ]);
  });

  it("does not write a link when committee resolution is ambiguous", async () => {
    const db = createMockDb();

    await expect(
      autoLinkOklahomaCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        sourceUrl: "https://guardian.ok.gov/PublicSite/DataDownload.aspx",
        contributionRows: [
          contribution(),
          contribution({
            "Org ID": "99999",
            "Committee Name": "Friends of Brent Dishman",
          }),
        ],
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Brent Dishman",
          electionYear: 2026,
          officeScope: "state_upper",
          officeName: "State Senator",
          district: "47",
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
      autoLinkMissingOklahomaCandidateFinanceLinks({
        db,
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
        candidateElections: [
          {
            candidateId: CANDIDATE_ID,
            electionId: ELECTION_ID,
            candidateName: "Brent Dishman",
            electionYear: 2026,
            officeScope: "state_upper",
            officeName: "State Senator",
            district: "47",
          },
        ],
        contributionRowsByYear: new Map([[2026, [contribution()]]]),
        sourceUrlByYear: new Map([[2026, "https://guardian.ok.gov/PublicSite/DataDownload.aspx"]]),
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        status: "linked",
        committeeId: "11954",
      },
    ]);

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ok_candidate_finance_links");
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
        autoLinkMissingOklahomaCandidateFinanceLinks({
          db,
          now: NOW,
          maxCandidates: 25,
          electionLookbackDays: 1,
          electionLookaheadDays: 730,
          candidateElections: [
            {
              candidateId: CANDIDATE_ID,
              electionId: ELECTION_ID,
              candidateName: "Brent Dishman",
              electionYear: 2026,
              officeScope: "state_upper",
              officeName: "State Senator",
              district: "47",
            },
            {
              candidateId: "33333333-3333-4333-8333-333333333333",
              electionId: "44444444-4444-4444-8444-444444444444",
              candidateName: "Brent Dishman",
              electionYear: 2026,
              officeScope: "state_upper",
              officeName: "State Senator",
              district: "47",
            },
          ],
          contributionRowsByYear: new Map([[2026, [contribution()]]]),
          sourceUrlByYear: new Map([[2026, "https://guardian.ok.gov/PublicSite/DataDownload.aspx"]]),
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
          committeeId: "11954",
        },
      ]);
    } finally {
      warnSpy.mockRestore();
    }

    expect(db.query).toHaveBeenCalledTimes(2);
  });

  it("builds a candidate-name predicate for filtered Guardian reads", () => {
    const predicate = buildOklahomaCandidateNamePredicate([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Brent Dishman",
        electionYear: 2026,
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "47",
      },
    ]);

    expect(predicate(contribution({ "Candidate Name": "DISHMAN, C. BRENT" }))).toBe(true);
    expect(predicate(contribution({ "Candidate Name": "Other Person" }))).toBe(false);
  });
});
