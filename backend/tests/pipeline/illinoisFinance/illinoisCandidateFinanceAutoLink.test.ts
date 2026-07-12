import { describe, expect, it, vi } from "vitest";

import {
  autoLinkIllinoisCandidateFinanceForCandidateElection,
  autoLinkMissingIllinoisCandidateFinanceLinks,
  listIllinoisCandidateElectionsMissingFinanceLinks,
} from "../../../src/pipeline/illinoisFinance/illinoisCandidateFinanceAutoLink.js";
import type { IllinoisCandidateCommitteeResolution } from "../../../src/pipeline/illinoisFinance/illinoisCandidateCommitteeResolver.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2026-06-01T00:00:00.000Z");
const SOURCE_URL = "https://www.elections.il.gov/CampaignDisclosure/ContributionSearchByCandidates.aspx";

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows, rowCount: rows.length }),
  };
}

function matchedResolution(
  overrides: Partial<Extract<IllinoisCandidateCommitteeResolution, { status: "matched" }>> = {}
) {
  return {
    status: "matched" as const,
    matches: [
      {
        committeeKey: "FRIENDS OF JANE DOE",
        committeeName: "Friends of Jane Doe",
        confidence: "name_fallback" as const,
        source: "illinois_sbe" as const,
        sourceUrl: SOURCE_URL,
        matchedContributionRowCount: 2,
        sbeCandidateId: null,
        sbeCommitteeId: null,
        sbeDistrictType: null,
        sbeOffice: null,
        district: null,
        isAtLarge: null,
      },
    ],
    ...overrides,
  };
}

describe("illinoisCandidateFinanceAutoLink", () => {
  it("lists eligible Illinois candidate elections missing active finance links", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Jane Doe",
        election_year: 2026,
        office_scope: "state_lower",
        office_name: "State Lower Chamber Legislator",
        district: "12",
      },
    ]);

    await expect(
      listIllinoisCandidateElectionsMissingFinanceLinks(db, {
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
        district: "12",
      },
    ]);

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("district.state = 'IL'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($5::text[])");
    expect(sql).toContain("FROM public.il_candidate_finance_links AS link");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      25,
      1,
      730,
      expect.arrayContaining(["statewide::Governor", "state_lower::State Lower Chamber Legislator"]),
    ]);
  });

  it("links a matched Illinois candidate election to the resolved SBE committee", async () => {
    const db = createMockDb([{ id: "link-1" }]);
    const resolveCandidateCommittee = vi.fn(async () => matchedResolution());

    await expect(
      autoLinkIllinoisCandidateFinanceForCandidateElection({
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
      status: "linked",
      committeeKey: "FRIENDS OF JANE DOE",
      committeeKeys: ["FRIENDS OF JANE DOE"],
    });

    expect(resolveCandidateCommittee).toHaveBeenCalledWith(
      {
        candidateName: "Jane Doe",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2026,
        district: null,
      },
      undefined
    );
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.il_candidate_finance_links");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "JANE DOE",
      "Governor",
      null,
      null,
      null,
      null,
      null,
      "FRIENDS OF JANE DOE",
      "Friends of Jane Doe",
      "active",
      "illinois_sbe",
      SOURCE_URL,
      "2026-06-01T00:00:00.000Z",
    ]);
    expect(String(db.query.mock.calls[1]?.[0])).toContain("NOT (committee_key = ANY($4::text[]))");
  });

  it("does not write a link when committee resolution is unmatched", async () => {
    const db = createMockDb();
    const resolveCandidateCommittee = vi.fn(async (): Promise<IllinoisCandidateCommitteeResolution> => ({
      status: "unmatched",
      reason: "no_candidate_committee_match",
      candidateNameNormalized: "JANE DOE",
      officeNameNormalized: "Governor",
    }));

    await expect(
      autoLinkIllinoisCandidateFinanceForCandidateElection({
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
      status: "unmatched",
      reason: "no_candidate_committee_match",
    });
    expect(db.query).not.toHaveBeenCalled();
  });

  it("uses provided candidate elections without querying when auto-linking a batch", async () => {
    const db = createMockDb([{ id: "link-1" }]);
    const resolveCandidateCommittee = vi.fn(async () => matchedResolution());

    await expect(
      autoLinkMissingIllinoisCandidateFinanceLinks({
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
        committeeKey: "FRIENDS OF JANE DOE",
        committeeKeys: ["FRIENDS OF JANE DOE"],
      },
    ]);
    expect(resolveCandidateCommittee).toHaveBeenCalledTimes(1);
    expect(db.query).toHaveBeenCalledTimes(2);
  });
});
