import { describe, expect, it, vi } from "vitest";

import {
  autoLinkMissingOregonCandidateFinanceLinks,
  listOregonCandidateElectionsMissingFinanceLinks,
} from "../../../src/pipeline/oregonFinance/oregonCandidateFinanceAutoLink.js";

const NOW = new Date("2026-06-25T19:00:00.000Z");
const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const LINK_ID = "33333333-3333-4333-8333-333333333333";

describe("oregonCandidateFinanceAutoLink", () => {
  it("lists Oregon eligible candidate elections missing finance links", async () => {
    const db = {
      query: vi.fn(async () => ({
        rows: [
          {
            candidate_id: CANDIDATE_ID,
            election_id: ELECTION_ID,
            candidate_name: "Tina Kotek",
            election_year: 2026,
            office_scope: "statewide",
            office_name: "Governor",
            district: "Oregon",
          },
        ],
      })),
    };

    await expect(
      listOregonCandidateElectionsMissingFinanceLinks(db, {
        now: NOW,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Tina Kotek",
        electionYear: 2026,
        officeScope: "statewide",
        officeName: "Governor",
        district: "Oregon",
      },
    ]);

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("LEFT JOIN public.or_candidate_finance_links AS link");
    expect(sql).toContain("district.state = 'OR'");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-25T19:00:00.000Z",
      1,
      730,
      expect.arrayContaining(["statewide::Governor", "state_upper::State Senator"]),
      25,
    ]);
  });

  it("writes a link only when the resolver returns a unique match", async () => {
    const db = {
      query: vi.fn(async () => ({ rows: [{ id: LINK_ID }], rowCount: 1 })),
    };
    const resolveCandidateCommittee = vi.fn(async () => ({
      status: "matched" as const,
      committeeId: "4792",
      committeeName: "Friends of Tina Kotek",
      confidence: "exact" as const,
      source: "orestar_public" as const,
      sourceUrl: "https://secure.sos.state.or.us/orestar/sooDetail.do?cneCommitteeId=4792",
      matchedCommitteeRowCount: 2,
    }));
    const searchRows = [
      {
        transactionId: "4458653",
        date: "10/12/2022",
        status: "Original",
        filerCommitteeName: "Friends of Tina Kotek",
        filerCommitteeId: "4792",
        committeeUrl: "https://secure.sos.state.or.us/orestar/sooDetail.do?cneCommitteeId=4792",
        contributorPayee: "Jane Donor",
        transactionSubtype: "Cash Contribution",
        amount: 100,
        detailUrl: "https://secure.sos.state.or.us/orestar/gotoPublicTransactionDetail.do?tranRsn=4458653",
      },
    ];
    const loadCandidateSearchRows = vi.fn(async () => searchRows);

    await expect(
      autoLinkMissingOregonCandidateFinanceLinks({
        db,
        now: NOW,
        resolveCandidateCommittee,
        loadCandidateSearchRows,
        candidateElections: [
          {
            candidateId: CANDIDATE_ID,
            electionId: ELECTION_ID,
            candidateName: "Tina Kotek",
            electionYear: 2026,
            officeScope: "statewide",
            officeName: "Governor",
            district: null,
          },
        ],
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        status: "linked",
        committeeId: "4792",
        committeeName: "Friends of Tina Kotek",
        linkId: LINK_ID,
      },
    ]);

    expect(loadCandidateSearchRows).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Tina Kotek",
      })
    );
    expect(resolveCandidateCommittee).toHaveBeenCalledWith({
      candidateName: "Tina Kotek",
      searchRows,
    });
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      CANDIDATE_ID,
      ELECTION_ID,
      2026,
      "TINA KOTEK",
      "Governor",
      null,
      "4792",
      "Friends of Tina Kotek",
      "active",
      "orestar",
      "https://secure.sos.state.or.us/orestar/sooDetail.do?cneCommitteeId=4792",
      "2026-06-25T19:00:00.000Z",
    ]);
  });

  it("skips built-in resolver auto-linking when search rows are not provided", async () => {
    const db = {
      query: vi.fn(),
    };

    await expect(
      autoLinkMissingOregonCandidateFinanceLinks({
        db,
        now: NOW,
        candidateElections: [
          {
            candidateId: CANDIDATE_ID,
            electionId: ELECTION_ID,
            candidateName: "Tina Kotek",
            electionYear: 2026,
            officeScope: "statewide",
            officeName: "Governor",
            district: null,
          },
        ],
      })
    ).resolves.toEqual([
      {
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        status: "skipped",
        reason: "Oregon auto-link search rows were not provided",
      },
    ]);
    expect(db.query).not.toHaveBeenCalled();
  });
});
