import { describe, expect, it, vi } from "vitest";

import {
  autoLinkMassachusettsCandidateFinanceForCandidateElection,
  autoLinkMissingMassachusettsCandidateFinanceLinks,
  listMassachusettsCandidateElectionsMissingFinanceLinks,
} from "../../../src/pipeline/massachusettsFinance/massachusettsCandidateFinanceAutoLink.js";
import type { MassachusettsCandidateCommitteeResolution } from "../../../src/pipeline/massachusettsFinance/massachusettsCandidateCommitteeResolver.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2026-06-01T00:00:00.000Z");
const SOURCE_URL = "https://api.ocpf.us/filers/listings/A?searchPhrase=Maura%20Healey";

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows, rowCount: rows.length }),
  };
}

function matchedResolution(
  overrides: Partial<Extract<MassachusettsCandidateCommitteeResolution, { status: "matched" }>> = {}
): Extract<MassachusettsCandidateCommitteeResolution, { status: "matched" }> {
  return {
    status: "matched",
    candidateCpfId: "15710",
    filerName: "Healey, Maura T.",
    committeeName: "Healey Committee",
    officeSought: "Statewide, Governor",
    confidence: "exact",
    source: "ocpf_api",
    sourceUrl: SOURCE_URL,
    matchedFilerRowCount: 1,
    ...overrides,
  };
}

describe("massachusettsCandidateFinanceAutoLink", () => {
  it("lists eligible Massachusetts candidate elections missing active finance links", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Maura Healey",
        election_year: 2022,
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
      listMassachusettsCandidateElectionsMissingFinanceLinks(db, {
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Maura Healey",
        electionYear: 2022,
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
    expect(sql).toContain("district.state = 'MA'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($5::text[])");
    // Municipal offices are gated to place districts in the enabled-city
    // GEOID allowlist; non-place offices are unaffected.
    expect(sql).toContain("district.district_type = 'place' AND district.geoid_compact = ANY($6::text[])");
    expect(sql).toContain("FROM public.ma_candidate_finance_links AS link");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      25,
      1,
      730,
      expect.arrayContaining([
        "statewide::Governor",
        "statewide::Lieutenant Governor",
        "statewide::Attorney General",
        "state_upper::State Senator",
        "state_lower::State Lower Chamber Legislator",
        "place::Mayor",
        "place::City Council Member",
      ]),
      ["2507000"],
    ]);
  });

  it("maps place rows to the OCPF city token from the GEOID allowlist", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Michelle Wu",
        election_year: 2025,
        office_scope: "place",
        office_name: "Mayor",
        district: null,
        district_type: "place",
        geoid_compact: "2507000",
      },
    ]);

    await expect(
      listMassachusettsCandidateElectionsMissingFinanceLinks(db, {
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Michelle Wu",
        electionYear: 2025,
        officeScope: "place",
        officeName: "Mayor",
        district: "BOSTON",
      },
    ]);
  });

  it("links a matched candidate election to the resolved OCPF candidate CPF", async () => {
    const db = createMockDb([{ id: "link-1" }]);
    const resolveCandidateCommittee = vi.fn(async () => matchedResolution());

    await expect(
      autoLinkMassachusettsCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        resolveCandidateCommittee,
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Maura Healey",
          electionYear: 2022,
          officeScope: "statewide",
          officeName: "Governor",
          district: null,
        },
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "linked",
      candidateCpfId: "15710",
    });

    expect(resolveCandidateCommittee).toHaveBeenCalledWith(
      {
        candidateName: "Maura Healey",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2022,
        district: null,
      },
      undefined
    );
    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ma_candidate_finance_links");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2022,
      "MAURA HEALEY",
      "Governor",
      null,
      "15710",
      "Healey, Maura T.",
      "Healey Committee",
      "active",
      "ocpf_api",
      SOURCE_URL,
      "2026-06-01T00:00:00.000Z",
    ]);
  });

  it("does not write a link when committee resolution is ambiguous", async () => {
    const db = createMockDb();
    const resolveCandidateCommittee = vi.fn(async (): Promise<MassachusettsCandidateCommitteeResolution> => ({
      status: "ambiguous",
      reason: "multiple_matching_committees",
      candidateNameNormalized: "MAURA HEALEY",
      officeNameNormalized: "Statewide, Governor",
      matches: [matchedResolution(), matchedResolution({ candidateCpfId: "99999", filerName: "Healey, Maura" })],
    }));

    await expect(
      autoLinkMassachusettsCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        resolveCandidateCommittee,
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Maura Healey",
          electionYear: 2022,
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
      autoLinkMissingMassachusettsCandidateFinanceLinks({
        db,
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
        candidateElections: [
          {
            candidateId: CANDIDATE_ID,
            electionId: ELECTION_ID,
            candidateName: "Maura Healey",
            electionYear: 2022,
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
        candidateCpfId: "15710",
      },
    ]);

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.ma_candidate_finance_links");
  });
});
