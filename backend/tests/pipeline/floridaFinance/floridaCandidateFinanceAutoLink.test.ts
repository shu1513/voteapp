import { describe, expect, it, vi } from "vitest";

import {
  autoLinkFloridaCandidateFinanceForCandidateElection,
  autoLinkMissingFloridaCandidateFinanceLinks,
  buildFloridaCandidateNamePredicate,
  listFloridaCandidateElectionsMissingFinanceLinks,
  resolveFloridaCandidateCommittee,
} from "../../../src/pipeline/floridaFinance/floridaCandidateFinanceAutoLink.js";
import type { FloridaContributionRow } from "../../../src/pipeline/floridaFinance/floridaCampaignFinanceRows.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2026-06-01T00:00:00.000Z");

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows, rowCount: rows.length }),
  };
}

function contribution(overrides: Partial<FloridaContributionRow> = {}): FloridaContributionRow {
  return {
    recipientName: "Friends of Jane Doe",
    contributionDate: "9/15/2026",
    amount: "100.00",
    transactionType: "CHE",
    contributorName: "Pat Person",
    address: "1 Main St",
    city: "Tallahassee",
    state: "FL",
    zip: "32301",
    occupation: "Attorney",
    inKindDescription: "",
    electionCode: "2026-GEN",
    sourceUrl: "https://dos.elections.myflorida.com/campaign-finance/contributions/",
    ...overrides,
  };
}

describe("floridaCandidateFinanceAutoLink", () => {
  it("lists eligible Florida candidate elections missing active finance links", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Jane Doe",
        election_year: 2026,
        office_scope: "state_lower",
        office_name: "State Lower Chamber Legislator",
        district: "42",
      },
    ]);

    await expect(
      listFloridaCandidateElectionsMissingFinanceLinks(db, {
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
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        district: "42",
      },
    ]);

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.candidate_elections AS candidate_election");
    expect(sql).toContain("district.state = 'FL'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($5::text[])");
    expect(sql).toContain("FROM public.fl_candidate_finance_links AS link");
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

  it("resolves one candidate committee from Florida DOS contribution recipient names", () => {
    expect(
      resolveFloridaCandidateCommittee({
        candidateName: "Jane Doe",
        electionYear: 2026,
        contributionRows: [
          contribution({ contributionDate: "12/31/2024", recipientName: "Old Jane Doe Account" }),
          contribution(),
          contribution({ recipientName: "Friends of Jane Doe" }),
          contribution({ recipientName: "Other Committee" }),
        ],
      })
    ).toEqual({
      status: "matched",
      committeeId: "FRIENDS_OF_JANE_DOE",
      committeeName: "Friends of Jane Doe",
      recipientNames: ["Friends of Jane Doe"],
      sourceUrl: "https://dos.elections.myflorida.com/campaign-finance/contributions/",
    });
  });

  it("matches DOS surname-first recipient names from a First-Last candidate name", () => {
    expect(
      resolveFloridaCandidateCommittee({
        candidateName: "Bruno Barreiro",
        electionYear: 2026,
        contributionRows: [
          contribution({ contributionDate: "01/15/2026", recipientName: "Barreiro, Bruno A. (REP)(STR)" }),
          contribution({ recipientName: "Other Committee" }),
        ],
      })
    ).toMatchObject({
      status: "matched",
      committeeName: "Barreiro, Bruno A. (REP)(STR)",
    });
  });

  it("matches around middle names and compound surnames via the short reversed key", () => {
    // App "Jane A. Doe" vs DOS "DOE, JANE" (no middle name on the DOS side).
    expect(
      resolveFloridaCandidateCommittee({
        candidateName: "Jane A. Doe",
        electionYear: 2026,
        contributionRows: [
          contribution({ contributionDate: "01/15/2026", recipientName: "DOE, JANE (DEM)(GOV)" }),
        ],
      })
    ).toMatchObject({ status: "matched" });
    // App "Jane de la Cruz" vs DOS "DE LA CRUZ, JANE".
    expect(
      resolveFloridaCandidateCommittee({
        candidateName: "Jane de la Cruz",
        electionYear: 2026,
        contributionRows: [
          contribution({ contributionDate: "01/15/2026", recipientName: "DE LA CRUZ, JANE (REP)(STR)" }),
        ],
      })
    ).toMatchObject({ status: "matched" });
  });

  it("does not resolve when more than one recipient name matches the candidate", () => {
    expect(
      resolveFloridaCandidateCommittee({
        candidateName: "Jane Doe",
        electionYear: 2026,
        contributionRows: [
          contribution(),
          contribution({ recipientName: "Jane Doe Campaign Account" }),
        ],
      })
    ).toEqual({
      status: "ambiguous",
      reason: "multiple_matching_committees",
    });
  });

  it("does not resolve embedded-token committee name matches", () => {
    expect(
      resolveFloridaCandidateCommittee({
        candidateName: "Ann Lee",
        electionYear: 2026,
        contributionRows: [contribution({ recipientName: "Friends of Joann Lee" })],
      })
    ).toEqual({
      status: "unmatched",
      reason: "no_matching_committee",
    });
  });

  it("builds short first-name and last-name keys from comma-form names", () => {
    expect(
      resolveFloridaCandidateCommittee({
        candidateName: "Doe, Jane A.",
        electionYear: 2026,
        contributionRows: [contribution({ recipientName: "Friends of Jane Doe" })],
      })
    ).toEqual({
      status: "matched",
      committeeId: "FRIENDS_OF_JANE_DOE",
      committeeName: "Friends of Jane Doe",
      recipientNames: ["Friends of Jane Doe"],
      sourceUrl: "https://dos.elections.myflorida.com/campaign-finance/contributions/",
    });
    expect(
      resolveFloridaCandidateCommittee({
        candidateName: "Doe, Jane Jr.",
        electionYear: 2026,
        contributionRows: [contribution({ recipientName: "Friends of Jane Doe" })],
      })
    ).toMatchObject({
      status: "matched",
      committeeId: "FRIENDS_OF_JANE_DOE",
    });
  });

  it("links a matched candidate election to the resolved DOS committee", async () => {
    const db = createMockDb([{ id: "link-1" }]);

    await expect(
      autoLinkFloridaCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        sourceUrl: "https://dos.elections.myflorida.com/campaign-finance/contributions/",
        contributionRows: [contribution()],
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Jane Doe",
          electionYear: 2026,
          officeScope: "state_lower",
          officeName: "State Lower Chamber Legislator",
          district: "42",
        },
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "linked",
      committeeId: "FRIENDS_OF_JANE_DOE",
    });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.fl_candidate_finance_links");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "JANE DOE",
      "State Lower Chamber Legislator",
      "42",
      "FRIENDS_OF_JANE_DOE",
      "Friends of Jane Doe",
      "active",
      "dos_export",
      "https://dos.elections.myflorida.com/campaign-finance/contributions/",
      "2026-06-01T00:00:00.000Z",
    ]);
  });

  it("uses provided candidate elections without querying when auto-linking a batch", async () => {
    const db = createMockDb([{ id: "link-1" }]);

    await expect(
      autoLinkMissingFloridaCandidateFinanceLinks({
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
            officeScope: "state_lower",
            officeName: "State Lower Chamber Legislator",
            district: "42",
          },
        ],
        contributionRowsByYear: new Map([[2026, [contribution()]]]),
        sourceUrlByYear: new Map([[2026, "https://dos.elections.myflorida.com/campaign-finance/contributions/"]]),
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        status: "linked",
        committeeId: "FRIENDS_OF_JANE_DOE",
      },
    ]);

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.fl_candidate_finance_links");
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
        autoLinkMissingFloridaCandidateFinanceLinks({
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
              officeScope: "state_lower",
              officeName: "State Lower Chamber Legislator",
              district: "42",
            },
            {
              candidateId: "33333333-3333-4333-8333-333333333333",
              electionId: "44444444-4444-4444-8444-444444444444",
              candidateName: "Jane Doe",
              electionYear: 2026,
              officeScope: "state_lower",
              officeName: "State Lower Chamber Legislator",
              district: "42",
            },
          ],
          contributionRowsByYear: new Map([[2026, [contribution()]]]),
          sourceUrlByYear: new Map([[2026, "https://dos.elections.myflorida.com/campaign-finance/contributions/"]]),
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
          committeeId: "FRIENDS_OF_JANE_DOE",
        },
      ]);
    } finally {
      warnSpy.mockRestore();
    }

    expect(db.query).toHaveBeenCalledTimes(2);
  });

  it("builds a candidate-name predicate for filtered Florida DOS contribution reads", () => {
    const predicate = buildFloridaCandidateNamePredicate([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Jane Doe",
        electionYear: 2026,
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        district: "42",
      },
    ]);

    // Surname-first is how DOS names candidate committees; it must match.
    expect(predicate(contribution({ recipientName: "DOE, JANE Campaign Account" }))).toBe(true);
    expect(predicate(contribution({ recipientName: "Friends of Jane Doe" }))).toBe(true);
    expect(predicate(contribution({ recipientName: "Friends of Janedoe" }))).toBe(false);
    expect(predicate(contribution({ contributionDate: "12/31/2024", recipientName: "Friends of Jane Doe" }))).toBe(false);
    expect(predicate(contribution({ recipientName: "Other Committee" }))).toBe(false);
  });
});
