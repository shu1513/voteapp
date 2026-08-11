import { describe, expect, it, vi } from "vitest";

import {
  autoLinkMissingWashingtonCandidateFinanceLinks,
  autoLinkWashingtonCandidateFinanceForCandidateElection,
  listWashingtonCandidateElectionsMissingFinanceLinks,
} from "../../../src/pipeline/washingtonFinance/washingtonCandidateFinanceAutoLink.js";
import type { WashingtonCandidateCommitteeResolution } from "../../../src/pipeline/washingtonFinance/washingtonCandidateCommitteeResolver.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2026-06-01T00:00:00.000Z");
const SOURCE_URL = "https://data.wa.gov/resource/3h9x-7bvm.json";

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows, rowCount: rows.length }),
  };
}

function matchedResolution(overrides: Partial<Extract<WashingtonCandidateCommitteeResolution, { status: "matched" }>> = {}) {
  return {
    status: "matched" as const,
    filerId: "FERGR *115",
    committeeId: "32311",
    committeeName: "Robert W. Ferguson (Bob Ferguson)",
    candidacyId: "689556",
    confidence: "exact" as const,
    source: "pdc_api" as const,
    sourceUrl: SOURCE_URL,
    matchedSummaryRowCount: 1,
    ...overrides,
  };
}

describe("washingtonCandidateFinanceAutoLink", () => {
  it("lists eligible Washington candidate elections missing active finance links", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Bob Ferguson",
        election_year: 2024,
        office_scope: "statewide",
        office_name: "Governor",
        legislative_district: null,
        district_name: "Washington",
        ballot_title: "Governor",
      },
      {
        candidate_id: "33333333-3333-4333-8333-333333333333",
        election_id: "44444444-4444-4444-8444-444444444444",
        candidate_name: "Jane Doe",
        election_year: 2026,
        office_scope: "state_lower",
        office_name: "State Lower Chamber Legislator",
        legislative_district: "9",
        district_name: "Legislative District 9, Washington",
        ballot_title: "State Representative Pos. 1, Legislative District No. 9",
      },
      {
        candidate_id: "55555555-5555-4555-8555-555555555555",
        election_id: "66666666-6666-4666-8666-666666666666",
        candidate_name: "Nilu Jenks",
        election_year: 2026,
        office_scope: "place",
        office_name: "City Council Member",
        legislative_district: null,
        district_name: "Seattle city, Washington",
        ballot_title: "City of Seattle Council District No. 5",
      },
    ]);

    await expect(
      listWashingtonCandidateElectionsMissingFinanceLinks(db, {
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Bob Ferguson",
        electionYear: 2024,
        officeScope: "statewide",
        officeName: "Governor",
        legislativeDistrict: null,
        jurisdiction: null,
        position: null,
      },
      {
        candidateId: "33333333-3333-4333-8333-333333333333",
        electionId: "44444444-4444-4444-8444-444444444444",
        candidateName: "Jane Doe",
        electionYear: 2026,
        officeScope: "state_lower",
        officeName: "State Lower Chamber Legislator",
        legislativeDistrict: "09",
        jurisdiction: null,
        position: null,
      },
      {
        candidateId: "55555555-5555-4555-8555-555555555555",
        electionId: "66666666-6666-4666-8666-666666666666",
        candidateName: "Nilu Jenks",
        electionYear: 2026,
        officeScope: "place",
        officeName: "City Council Member",
        legislativeDistrict: null,
        jurisdiction: "SEATTLE",
        position: "5",
      },
    ]);

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.candidate_elections AS candidate_election");
    expect(sql).toContain("district.state = 'WA'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($5::text[])");
    expect(sql).toContain("FROM public.wa_candidate_finance_links AS link");
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
        "place::Mayor",
        "place::City Council Member",
        "place::Municipal Attorney",
        "place::Place Level Judge",
      ]),
    ]);
    expect(db.query.mock.calls[0]?.[1]?.[4]).not.toContain("statewide::United States Senator");
  });

  it("links a matched candidate election to the resolved PDC candidate filer", async () => {
    const db = createMockDb([{ id: "link-1" }]);
    const resolveCandidateCommittee = vi.fn(async () => matchedResolution());

    await expect(
      autoLinkWashingtonCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        resolveCandidateCommittee,
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Bob Ferguson",
          electionYear: 2024,
          officeScope: "statewide",
          officeName: "Governor",
          legislativeDistrict: null,
          jurisdiction: null,
          position: null,
        },
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "linked",
      filerId: "FERGR *115",
      committeeId: "32311",
    });

    expect(resolveCandidateCommittee).toHaveBeenCalledWith(
      {
        candidateName: "Bob Ferguson",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2024,
        legislativeDistrict: null,
        jurisdiction: null,
        position: null,
      },
      undefined
    );
    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.wa_candidate_finance_links");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2024,
      "BOB FERGUSON",
      "Governor",
      null,
      "FERGR *115",
      "32311",
      "Robert W. Ferguson (Bob Ferguson)",
      "689556",
      "active",
      "pdc_api",
      SOURCE_URL,
      "2026-06-01T00:00:00.000Z",
    ]);
  });

  it("links a place-scope candidate election with the city stored as the link district", async () => {
    const db = createMockDb([{ id: "link-1" }]);
    const resolveCandidateCommittee = vi.fn(async () =>
      matchedResolution({
        filerId: "JENKN--778",
        committeeId: "40861",
        committeeName: "Neeloofar Jenks (Nilu Jenks)",
        candidacyId: "712345",
      })
    );

    await expect(
      autoLinkWashingtonCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        resolveCandidateCommittee,
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Nilu Jenks",
          electionYear: 2026,
          officeScope: "place",
          officeName: "City Council Member",
          legislativeDistrict: null,
          jurisdiction: "SEATTLE",
          position: "5",
        },
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "linked",
      filerId: "JENKN--778",
      committeeId: "40861",
    });

    expect(resolveCandidateCommittee).toHaveBeenCalledWith(
      {
        candidateName: "Nilu Jenks",
        officeScope: "place",
        officeName: "City Council Member",
        electionYear: 2026,
        legislativeDistrict: null,
        jurisdiction: "SEATTLE",
        position: "5",
      },
      undefined
    );
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "NILU JENKS",
      "City Council Member",
      "SEATTLE",
      "JENKN--778",
      "40861",
      "Neeloofar Jenks (Nilu Jenks)",
      "712345",
      "active",
      "pdc_api",
      SOURCE_URL,
      "2026-06-01T00:00:00.000Z",
    ]);
  });

  it("does not write a link when committee resolution is ambiguous", async () => {
    const db = createMockDb();
    const resolveCandidateCommittee = vi.fn(async (): Promise<WashingtonCandidateCommitteeResolution> => ({
      status: "ambiguous",
      reason: "multiple_matching_committees",
      candidateNameNormalized: "BOB FERGUSON",
      officeNameNormalized: "GOVERNOR",
      matches: [matchedResolution(), matchedResolution({ filerId: "FERGB--024", committeeId: "36704" })],
    }));

    await expect(
      autoLinkWashingtonCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        resolveCandidateCommittee,
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Bob Ferguson",
          electionYear: 2024,
          officeScope: "statewide",
          officeName: "Governor",
          legislativeDistrict: null,
          jurisdiction: null,
          position: null,
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
      autoLinkMissingWashingtonCandidateFinanceLinks({
        db,
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
        candidateElections: [
          {
            candidateId: CANDIDATE_ID,
            electionId: ELECTION_ID,
            candidateName: "Bob Ferguson",
            electionYear: 2024,
            officeScope: "statewide",
            officeName: "Governor",
            legislativeDistrict: null,
            jurisdiction: null,
            position: null,
          },
        ],
        resolveCandidateCommittee,
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        status: "linked",
        filerId: "FERGR *115",
        committeeId: "32311",
      },
    ]);

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.wa_candidate_finance_links");
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
        autoLinkMissingWashingtonCandidateFinanceLinks({
          db,
          now: NOW,
          maxCandidates: 25,
          electionLookbackDays: 1,
          electionLookaheadDays: 730,
          candidateElections: [
            {
              candidateId: CANDIDATE_ID,
              electionId: ELECTION_ID,
              candidateName: "Bob Ferguson",
              electionYear: 2024,
              officeScope: "statewide",
              officeName: "Governor",
              legislativeDistrict: null,
              jurisdiction: null,
              position: null,
            },
            {
              candidateId: "33333333-3333-4333-8333-333333333333",
              electionId: "44444444-4444-4444-8444-444444444444",
              candidateName: "Bob Ferguson",
              electionYear: 2024,
              officeScope: "statewide",
              officeName: "Governor",
              legislativeDistrict: null,
              jurisdiction: null,
              position: null,
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
          filerId: "FERGR *115",
          committeeId: "32311",
        },
      ]);

      expect(warnSpy).toHaveBeenCalledWith(
        "Washington finance auto-link failed for candidate election; continuing:",
        expect.objectContaining({
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          error: "temporary write failure",
        })
      );
    } finally {
      warnSpy.mockRestore();
    }

    expect(db.query).toHaveBeenCalledTimes(2);
  });
});
