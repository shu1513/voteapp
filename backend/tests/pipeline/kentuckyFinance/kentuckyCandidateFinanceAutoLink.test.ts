import { describe, expect, it, vi } from "vitest";

import { autoLinkKentuckyCandidateFinanceForCandidateElection } from "../../../src/pipeline/kentuckyFinance/kentuckyCandidateFinanceAutoLink.js";

const NOW = new Date("2026-06-01T00:00:00.000Z");
const SOURCE_URL = "https://secure.kentucky.gov/kref/publicsearch/ToCandidateSearch";
const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const LINK_ID = "33333333-3333-4333-8333-333333333333";

const candidateElection = {
  candidateId: CANDIDATE_ID,
  electionId: ELECTION_ID,
  candidateName: "Andy Beshear",
  electionYear: 2023,
  electionDate: "2023-11-07",
  officeScope: "statewide",
  officeName: "Governor",
  location: "Statewide",
};

describe("kentuckyCandidateFinanceAutoLink", () => {
  it("skips without a configured KREF resolver", async () => {
    const db = { query: vi.fn() };

    await expect(
      autoLinkKentuckyCandidateFinanceForCandidateElection({
        db,
        candidateElection,
        now: NOW,
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "skipped",
      reason: "resolver_not_configured",
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("does not write ambiguous KREF resolver matches", async () => {
    const db = { query: vi.fn() };
    const resolveCandidateFinanceLink = vi.fn(async () => ({
      status: "ambiguous" as const,
      reason: "multiple_kref_candidate_matches",
      matchCount: 2,
    }));

    await expect(
      autoLinkKentuckyCandidateFinanceForCandidateElection({
        db,
        candidateElection,
        now: NOW,
        resolveCandidateFinanceLink,
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "ambiguous",
      reason: "multiple_kref_candidate_matches",
      matchCount: 2,
    });
    expect(resolveCandidateFinanceLink).toHaveBeenCalledWith(candidateElection);
    expect(db.query).not.toHaveBeenCalled();
  });

  it("writes matched KREF resolver results as active public-search links", async () => {
    const db = { query: vi.fn().mockResolvedValue({ rows: [{ id: LINK_ID }], rowCount: 1 }) };
    const resolveCandidateFinanceLink = vi.fn(async () => ({
      status: "matched" as const,
      candidateKey: "andy beshear|governor|statewide|2023-11-07",
      committeeKey: "beshear campaign committee",
      committeeName: "Beshear Campaign Committee",
      sourceUrl: SOURCE_URL,
    }));

    await expect(
      autoLinkKentuckyCandidateFinanceForCandidateElection({
        db,
        candidateElection,
        now: NOW,
        resolveCandidateFinanceLink,
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "linked",
      candidateKey: "andy beshear|governor|statewide|2023-11-07",
      committeeKey: "beshear campaign committee",
    });

    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ky_candidate_finance_links");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2023,
      "ANDY BESHEAR",
      "Governor",
      "Statewide",
      "ANDY BESHEAR|GOVERNOR|STATEWIDE|2023-11-07",
      "BESHEAR CAMPAIGN COMMITTEE",
      "Beshear Campaign Committee",
      "active",
      "kref_public_search",
      SOURCE_URL,
      "2026-06-01T00:00:00.000Z",
    ]);
  });
});
