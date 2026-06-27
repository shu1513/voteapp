import { describe, expect, it, vi } from "vitest";

import {
  autoLinkMissingMinnesotaCandidateFinanceLinks,
  autoLinkMinnesotaCandidateFinanceForCandidateElection,
  listMinnesotaCandidateElectionsMissingFinanceLinks,
} from "../../../src/pipeline/minnesotaFinance/minnesotaCandidateFinanceAutoLink.js";
import type { MinnesotaCampaignFinanceCsvRow } from "../../../src/pipeline/minnesotaFinance/minnesotaCampaignFinanceArtifactReader.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2026-06-01T00:00:00.000Z");

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows, rowCount: rows.length }),
  };
}

function contribution(overrides: Partial<MinnesotaCampaignFinanceCsvRow> = {}): MinnesotaCampaignFinanceCsvRow {
  return {
    "Committee ID": "1001",
    "Committee Name": "FRIENDS OF JANE DOE",
    Candidate: "Jane Doe",
    Office: "Governor",
    District: "",
    Status: "Active",
    Year: "2026",
    ...overrides,
  };
}

describe("minnesotaCandidateFinanceAutoLink", () => {
  it("lists eligible Minnesota candidate elections missing active finance links", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Jane Doe",
        election_year: 2026,
        office_scope: "statewide",
        office_name: "Governor",
        district: null,
      },
    ]);

    await expect(
      listMinnesotaCandidateElectionsMissingFinanceLinks(db, {
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
        officeScope: "statewide",
        officeName: "Governor",
        district: null,
      },
    ]);

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.candidate_elections AS candidate_election");
    expect(sql).toContain("district.state = 'MN'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($5::text[])");
    expect(sql).toContain("FROM public.mn_candidate_finance_links AS link");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      25,
      1,
      730,
      expect.arrayContaining([
        "statewide::Governor",
        "statewide::Lieutenant Governor",
        "statewide::Secretary of State",
        "statewide::Attorney General",
        "state_upper::State Senator",
        "state_lower::State Lower Chamber Legislator",
      ]),
    ]);
  });

  it("links a matched candidate election to the resolved committee", async () => {
    const db = createMockDb([{ id: "link-1" }]);

    await expect(
      autoLinkMinnesotaCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Jane Doe",
          electionYear: 2026,
          officeScope: "statewide",
          officeName: "Governor",
          district: null,
        },
        contributionRows: [contribution()],
      })
    ).resolves.toEqual({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      status: "linked",
      committeeId: "1001",
    });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.mn_candidate_finance_links");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "JANE DOE",
      "Governor",
      null,
      "1001",
      "FRIENDS OF JANE DOE",
      "active",
      "mn_board",
      null,
      "2026-06-01T00:00:00.000Z",
    ]);
  });

  it("uses provided candidate elections without querying when auto-linking a batch", async () => {
    const db = createMockDb([{ id: "link-1" }]);

    await expect(
      autoLinkMissingMinnesotaCandidateFinanceLinks({
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
        contributionRows: [contribution()],
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        status: "linked",
        committeeId: "1001",
      },
    ]);

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.mn_candidate_finance_links");
  });
});
