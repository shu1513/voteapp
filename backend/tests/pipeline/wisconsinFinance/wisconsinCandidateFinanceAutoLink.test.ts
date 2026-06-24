import { describe, expect, it, vi } from "vitest";

import {
  autoLinkMissingWisconsinCandidateFinanceLinks,
  autoLinkWisconsinCandidateFinanceForCandidateElection,
  listWisconsinCandidateElectionsMissingFinanceLinks,
} from "../../../src/pipeline/wisconsinFinance/wisconsinCandidateFinanceAutoLink.js";
import type { WisconsinCandidateCommitteeResolution } from "../../../src/pipeline/wisconsinFinance/wisconsinCandidateCommitteeResolver.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2026-06-01T00:00:00.000Z");
const SOURCE_URL = "https://campaignfinance.wi.gov/browse-data/registrants/16621";

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows, rowCount: rows.length }),
  };
}

function matchedResolution(overrides: Partial<Extract<WisconsinCandidateCommitteeResolution, { status: "matched" }>> = {}) {
  return {
    status: "matched" as const,
    entityId: "16621",
    committeeId: "407",
    assignedCommitteeId: "0104212",
    committeeName: "Tiffany for Wisconsin",
    confidence: "exact" as const,
    source: "sunshine_api" as const,
    sourceUrl: SOURCE_URL,
    matchedCommitteeRowCount: 1,
    ...overrides,
  };
}

describe("wisconsinCandidateFinanceAutoLink", () => {
  it("lists eligible Wisconsin candidate elections missing active finance links", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Tom Tiffany",
        election_year: 2026,
        office_scope: "statewide",
        office_name: "Governor",
        district: null,
      },
      {
        candidate_id: "33333333-3333-4333-8333-333333333333",
        election_id: "44444444-4444-4444-8444-444444444444",
        candidate_name: "Jane Doe",
        election_year: 2026,
        office_scope: "state_lower",
        office_name: "State Lower Chamber Legislator",
        district: "9",
      },
    ]);

    await expect(
      listWisconsinCandidateElectionsMissingFinanceLinks(db, {
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Tom Tiffany",
        electionYear: 2026,
        officeScope: "statewide",
        officeName: "Governor",
        district: null,
      },
      {
        candidateId: "33333333-3333-4333-8333-333333333333",
        electionId: "44444444-4444-4444-8444-444444444444",
        candidateName: "Jane Doe",
        electionYear: 2026,
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        district: "9",
      },
    ]);

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.candidate_elections AS candidate_election");
    expect(sql).toContain("district.state = 'WI'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($5::text[])");
    expect(sql).toContain("FROM public.wi_candidate_finance_links AS link");
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
    expect(db.query.mock.calls[0]?.[1]?.[4]).not.toContain("statewide::State Supreme Court Justice");
    expect(db.query.mock.calls[0]?.[1]?.[4]).not.toContain("county::District Attorney");
  });

  it("links a matched candidate election to the resolved Sunshine candidate committee", async () => {
    const db = createMockDb([{ id: "link-1" }]);
    const resolveCandidateCommittee = vi.fn(async () => matchedResolution());

    await expect(
      autoLinkWisconsinCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        resolveCandidateCommittee,
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Tom Tiffany",
          electionYear: 2026,
          officeScope: "statewide",
          officeName: "Governor",
          district: null,
        },
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "linked",
      entityId: "16621",
      committeeId: "407",
    });

    expect(resolveCandidateCommittee).toHaveBeenCalledWith(
      {
        candidateName: "Tom Tiffany",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        district: null,
      },
      undefined
    );
    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.wi_candidate_finance_links");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "TOM TIFFANY",
      "Governor",
      null,
      "16621",
      "407",
      "Tiffany for Wisconsin",
      "0104212",
      "active",
      "sunshine_api",
      SOURCE_URL,
      "2026-06-01T00:00:00.000Z",
    ]);
  });

  it("does not write a link when committee resolution is ambiguous", async () => {
    const db = createMockDb();
    const resolveCandidateCommittee = vi.fn(async (): Promise<WisconsinCandidateCommitteeResolution> => ({
      status: "ambiguous",
      reason: "multiple_matching_committees",
      candidateNameNormalized: "JANE DOE",
      officeNameNormalized: "Governor",
      matches: [matchedResolution(), matchedResolution({ entityId: "16622", committeeId: "408" })],
    }));

    await expect(
      autoLinkWisconsinCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        resolveCandidateCommittee,
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Jane Doe",
          electionYear: 2026,
          officeScope: "statewide",
          officeName: "Governor",
          district: null,
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
    const resolveCandidateCommittee = vi.fn(async () => matchedResolution());

    await expect(
      autoLinkMissingWisconsinCandidateFinanceLinks({
        db,
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
        candidateElections: [
          {
            candidateId: CANDIDATE_ID,
            electionId: ELECTION_ID,
            candidateName: "Tom Tiffany",
            electionYear: 2026,
            officeScope: "statewide",
            officeName: "Governor",
            district: null,
          },
        ],
        resolveCandidateCommittee,
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        status: "linked",
        entityId: "16621",
        committeeId: "407",
      },
    ]);

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.wi_candidate_finance_links");
  });

  it("continues auto-linking later candidates when one candidate write fails", async () => {
    const db = {
      query: vi
        .fn()
        .mockRejectedValueOnce(new Error("temporary write failure"))
        .mockResolvedValueOnce({ rows: [{ id: "link-2" }], rowCount: 1 }),
    };
    const resolveCandidateCommittee = vi.fn(async () => matchedResolution());
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

    try {
      await expect(
        autoLinkMissingWisconsinCandidateFinanceLinks({
          db,
          now: NOW,
          maxCandidates: 25,
          electionLookbackDays: 1,
          electionLookaheadDays: 730,
          candidateElections: [
            {
              candidateId: CANDIDATE_ID,
              electionId: ELECTION_ID,
              candidateName: "Tom Tiffany",
              electionYear: 2026,
              officeScope: "statewide",
              officeName: "Governor",
              district: null,
            },
            {
              candidateId: "33333333-3333-4333-8333-333333333333",
              electionId: "44444444-4444-4444-8444-444444444444",
              candidateName: "Jane Doe",
              electionYear: 2026,
              officeScope: "statewide",
              officeName: "Governor",
              district: null,
            },
          ],
          resolveCandidateCommittee,
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
          entityId: "16621",
          committeeId: "407",
        },
      ]);
      expect(resolveCandidateCommittee).toHaveBeenCalledTimes(2);
      expect(warnSpy).toHaveBeenCalledWith(
        "Wisconsin finance auto-link failed for candidate election; continuing:",
        expect.objectContaining({ error: "temporary write failure" })
      );
    } finally {
      warnSpy.mockRestore();
    }
  });
});
