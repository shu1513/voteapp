import { describe, expect, it, vi } from "vitest";

import {
  listDueOregonCandidateFinanceSyncRows,
  syncDueOregonCandidateFinance,
} from "../../../src/pipeline/oregonFinance/oregonCandidateFinanceBatchSync.js";

const NOW = new Date("2026-06-01T00:00:00.000Z");
const SOURCE_URL = "https://secure.sos.state.or.us/orestar/gotoPublicTransactionSearch.do";
const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";

function dueRow(overrides: Record<string, unknown> = {}) {
  return {
    candidate_id: CANDIDATE_ID,
    election_id: ELECTION_ID,
    candidate_name: "Tina Kotek",
    election_year: 2026,
    office_scope: "statewide",
    office_name: "Governor",
    district: null,
    committee_id: "4792",
    committee_name: "Friends of Tina Kotek",
    source_url: SOURCE_URL,
    last_synced_at: null,
    total_due_rows: "1",
    ...overrides,
  };
}

describe("oregonCandidateFinanceBatchSync", () => {
  it("lists active Oregon finance links that are due for sync", async () => {
    const db = {
      query: vi.fn(async () => ({
        rows: [
          dueRow({ total_due_rows: "2" }),
          dueRow({
            candidate_id: "33333333-3333-4333-8333-333333333333",
            election_id: "44444444-4444-4444-8444-444444444444",
            candidate_name: "Jane Doe",
            election_year: 2026,
            office_scope: "state_lower",
            office_name: "State Lower Chamber Legislator",
            district: "9",
            committee_id: "999",
            committee_name: "Jane Doe for Oregon",
            source_url: "https://secure.sos.state.or.us/orestar/cneSearch.do?candidate=999",
            last_synced_at: "2026-01-01 00:00:00+00",
            total_due_rows: "2",
          }),
        ],
        rowCount: 2,
      })),
    };

    await expect(
      listDueOregonCandidateFinanceSyncRows(db, {
        now: NOW,
        staleAfterDays: 7,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
      })
    ).resolves.toEqual({
      totalDueRows: 2,
      rows: [
        {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Tina Kotek",
          electionYear: 2026,
          officeScope: "statewide",
          officeName: "Governor",
          district: null,
          committeeId: "4792",
          committeeName: "Friends of Tina Kotek",
          sourceUrl: SOURCE_URL,
          lastSyncedAt: null,
        },
        {
          candidateId: "33333333-3333-4333-8333-333333333333",
          electionId: "44444444-4444-4444-8444-444444444444",
          candidateName: "Jane Doe",
          electionYear: 2026,
          officeScope: "state_lower",
          officeName: "State Lower Chamber Legislator",
          district: "9",
          committeeId: "999",
          committeeName: "Jane Doe for Oregon",
          sourceUrl: "https://secure.sos.state.or.us/orestar/cneSearch.do?candidate=999",
          lastSyncedAt: "2026-01-01 00:00:00+00",
        },
      ],
    });

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.or_candidate_finance_links AS link");
    expect(sql).toContain("link.link_status = 'active'");
    expect(sql).toContain("district.state = 'OR'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($6::text[])");
    expect(sql).toContain("nullif(trim(link.source_url), '') IS NOT NULL");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      7,
      25,
      1,
      730,
      expect.arrayContaining([
        "statewide::Governor",
        "statewide::Secretary of State",
        "state_upper::State Senator",
        "state_lower::State Lower Chamber Legislator",
      ]),
    ]);
  });

  it("syncs selected due candidates with injected ORESTAR transaction details and continues after failure", async () => {
    const db = {
      query: vi.fn(async () => ({
        rows: [
          dueRow({ total_due_rows: "2" }),
          dueRow({
            candidate_id: "33333333-3333-4333-8333-333333333333",
            election_id: "44444444-4444-4444-8444-444444444444",
            committee_id: "999",
            committee_name: "Jane Doe for Oregon",
            source_url: "https://secure.sos.state.or.us/orestar/cneSearch.do?candidate=999",
            total_due_rows: "2",
          }),
        ],
        rowCount: 2,
      })),
      connect: vi.fn(),
    };
    const transactionDetails = [{ transactionId: "4458653" }] as never;
    const successfulSync = {
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2026,
      dryRun: false,
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 1,
      outsideGroupsWritten: 1,
      outsideGroupBreakdownsWritten: 1,
      directContributionTotal: 100,
      outsideSupportTotal: 50,
      outsideOpposeTotal: 0,
      transactionDetailCount: 1,
      matchedDirectContributionRowCount: 1,
      includedDirectContributionRowCount: 1,
      skippedDirectContributionRowCount: 0,
      matchedExpenditureRowCount: 1,
      includedOutsideAssociationCount: 1,
      skippedOutsideAssociationCount: 0,
      matchedOutsideGroupContributionRowCount: 1,
      includedOutsideGroupContributionRowCount: 1,
      skippedOutsideGroupContributionRowCount: 0,
    };
    const loadTransactionDetails = vi.fn().mockResolvedValueOnce(transactionDetails).mockRejectedValueOnce(new Error("ORESTAR blocked"));
    const syncOregonCandidateFinanceFn = vi.fn().mockResolvedValueOnce(successfulSync);

    const result = await syncDueOregonCandidateFinance({
      db,
      now: NOW,
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 30,
      loadTransactionDetails,
      syncOregonCandidateFinanceFn: syncOregonCandidateFinanceFn as never,
    });

    expect(result).toMatchObject({
      dryRun: false,
      dueCandidateCount: 2,
      selectedCandidateCount: 2,
      syncedCandidateCount: 1,
      failedCandidateCount: 1,
      autoLinkAttemptedCount: 0,
      autoLinkLinkedCount: 0,
    });
    expect(result.results[0]).toMatchObject({ ok: true, result: successfulSync });
    expect(result.results[1]).toMatchObject({ ok: false, error: "ORESTAR blocked" });
    expect(loadTransactionDetails).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        committeeId: "4792",
      })
    );
    expect(syncOregonCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Tina Kotek",
        electionYear: 2026,
        officeName: "Governor",
        committeeId: "4792",
        committeeName: "Friends of Tina Kotek",
        sourceUrl: SOURCE_URL,
        transactionDetails,
        dryRun: false,
        now: NOW,
      })
    );
  });

  it("auto-links missing Oregon finance links before syncing linked rows when configured", async () => {
    const db = {
      query: vi
        .fn()
        .mockResolvedValueOnce({
          rows: [
            {
              candidate_id: "55555555-5555-4555-8555-555555555555",
              election_id: "66666666-6666-4666-8666-666666666666",
              candidate_name: "Tina Kotek",
              election_year: 2026,
              office_scope: "statewide",
              office_name: "Governor",
              district: "Oregon",
            },
          ],
        })
        .mockResolvedValueOnce({ rows: [{ id: "77777777-7777-4777-8777-777777777777" }], rowCount: 1 })
        .mockResolvedValueOnce({ rows: [dueRow()], rowCount: 1 }),
      connect: vi.fn(),
    };
    const loadCandidateSearchRows = vi.fn(async () => [
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
    ]);
    const loadTransactionDetails = vi.fn(async () => [{ transactionId: "4458653" }] as never);
    const syncOregonCandidateFinanceFn = vi.fn(async () => ({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2026,
      dryRun: false,
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      directContributionTotal: 0,
      outsideSupportTotal: 0,
      outsideOpposeTotal: 0,
      transactionDetailCount: 1,
      matchedDirectContributionRowCount: 0,
      includedDirectContributionRowCount: 0,
      skippedDirectContributionRowCount: 0,
      matchedExpenditureRowCount: 0,
      includedOutsideAssociationCount: 0,
      skippedOutsideAssociationCount: 0,
      matchedOutsideGroupContributionRowCount: 0,
      includedOutsideGroupContributionRowCount: 0,
      skippedOutsideGroupContributionRowCount: 0,
    }));

    const result = await syncDueOregonCandidateFinance({
      db,
      now: NOW,
      loadCandidateSearchRows,
      loadTransactionDetails,
      syncOregonCandidateFinanceFn: syncOregonCandidateFinanceFn as never,
    });

    expect(result).toMatchObject({
      autoLinkAttemptedCount: 1,
      autoLinkLinkedCount: 1,
      selectedCandidateCount: 1,
      syncedCandidateCount: 1,
    });
    expect(loadCandidateSearchRows).toHaveBeenCalledTimes(1);
    expect(String(db.query.mock.calls[0]?.[0])).toContain("LEFT JOIN public.or_candidate_finance_links AS link");
    expect(String(db.query.mock.calls[1]?.[0])).toContain("INSERT INTO public.or_candidate_finance_links");
    expect(String(db.query.mock.calls[2]?.[0])).toContain("FROM public.or_candidate_finance_links AS link");
  });

  it("reports row failures without writing snapshots when no transaction detail loader is configured", async () => {
    const db = {
      query: vi.fn(async () => ({ rows: [dueRow()], rowCount: 1 })),
      connect: vi.fn(),
    };
    const syncOregonCandidateFinanceFn = vi.fn();

    const result = await syncDueOregonCandidateFinance({
      db,
      now: NOW,
      syncOregonCandidateFinanceFn: syncOregonCandidateFinanceFn as never,
    });

    expect(result).toMatchObject({
      dueCandidateCount: 1,
      selectedCandidateCount: 1,
      syncedCandidateCount: 0,
      failedCandidateCount: 1,
    });
    expect(result.results[0]).toMatchObject({
      ok: false,
      error: "Oregon ORESTAR source URL must point to a transaction detail or populated search result",
    });
    expect(syncOregonCandidateFinanceFn).not.toHaveBeenCalled();
  });
});
