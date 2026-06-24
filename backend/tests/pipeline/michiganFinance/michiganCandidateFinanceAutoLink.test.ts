import { describe, expect, it, vi } from "vitest";

import {
  autoLinkMichiganCandidateFinanceForCandidateElection,
  listMichiganCandidateElectionsMissingFinanceLinks,
} from "../../../src/pipeline/michiganFinance/michiganCandidateFinanceAutoLink.js";
import type { MichiganMitnLegacyContributionRow } from "../../../src/pipeline/michiganFinance/michiganMitnLegacyArchiveReader.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2022-06-01T00:00:00.000Z");
const SOURCE_URL = "https://www.michigan.gov/sos/example/2022_mi_cfr.7z";

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows, rowCount: rows.length }),
  };
}

function contribution(
  overrides: Partial<MichiganMitnLegacyContributionRow> = {}
): MichiganMitnLegacyContributionRow {
  return {
    doc_seq_no: "100",
    page_no: "1",
    contribution_id: "200",
    cont_detail_id: "300",
    doc_stmnt_year: "2022",
    doc_type_desc: "Post-General",
    com_legal_name: "WHITMER FOR GOVERNOR",
    common_name: "Whitmer for Governor",
    cfr_com_id: "514456",
    com_type: "Candidate Committee",
    can_first_name: "GRETCHEN",
    can_last_name: "WHITMER",
    contribtype: "Individual",
    f_name: "JANE",
    l_name_or_org: "DOE",
    address: "1 Main",
    city: "Lansing",
    state: "MI",
    zip: "48901",
    occupation: "Attorney",
    employer: "Law Firm",
    received_date: "10/01/2022",
    amount: "100.00",
    aggregate: "100.00",
    extra_desc: "",
    ...overrides,
  };
}

describe("michiganCandidateFinanceAutoLink", () => {
  it("lists eligible Michigan candidate elections missing active finance links", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Gretchen Whitmer",
        election_year: 2022,
        office_scope: "statewide",
        office_name: "Governor",
        district: null,
      },
    ]);

    await expect(
      listMichiganCandidateElectionsMissingFinanceLinks(db, {
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Gretchen Whitmer",
        electionYear: 2022,
        officeScope: "statewide",
        officeName: "Governor",
        district: null,
      },
    ]);

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.candidate_elections AS candidate_election");
    expect(sql).toContain("district.state = 'MI'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($5::text[])");
    expect(sql).toContain("FROM public.mi_candidate_finance_links AS link");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2022-06-01T00:00:00.000Z",
      25,
      1,
      730,
      expect.arrayContaining(["statewide::Governor", "state_upper::State Senator"]),
    ]);
  });

  it("links a matched candidate election to the resolved MiTN committee", async () => {
    const db = createMockDb([{ id: "link-1" }]);

    await expect(
      autoLinkMichiganCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        sourceUrl: SOURCE_URL,
        contributionRows: [contribution()],
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Gretchen Whitmer",
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
      committeeId: "514456",
    });

    expect(db.query).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("INSERT INTO public.mi_candidate_finance_links");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2022,
      "GRETCHEN WHITMER",
      "Governor",
      null,
      "514456",
      "WHITMER FOR GOVERNOR",
      "active",
      "mitn_legacy",
      SOURCE_URL,
      "2022-06-01T00:00:00.000Z",
    ]);
  });

  it("does not write a link when committee resolution is ambiguous", async () => {
    const db = createMockDb();

    await expect(
      autoLinkMichiganCandidateFinanceForCandidateElection({
        db,
        now: NOW,
        sourceUrl: SOURCE_URL,
        contributionRows: [
          contribution(),
          contribution({
            cfr_com_id: "999999",
            com_legal_name: "ANOTHER WHITMER COMMITTEE",
          }),
        ],
        candidateElection: {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Gretchen Whitmer",
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
});
