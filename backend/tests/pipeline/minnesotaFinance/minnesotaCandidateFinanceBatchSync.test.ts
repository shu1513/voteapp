import { describe, expect, it, vi } from "vitest";

import {
  listDueMinnesotaCandidateFinanceSyncRows,
  syncDueMinnesotaCandidateFinance,
} from "../../../src/pipeline/minnesotaFinance/minnesotaCandidateFinanceBatchSync.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2026-06-01T00:00:00.000Z");

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows, rowCount: rows.length }),
  };
}

function dueQueryRow(overrides: Record<string, unknown> = {}) {
  return {
    candidate_id: CANDIDATE_ID,
    election_id: ELECTION_ID,
    candidate_name: "Jane Doe",
    election_year: 2026,
    office_scope: "statewide",
    office_name: "Governor",
    district: null,
    committee_id: "1001",
    committee_name: "FRIENDS OF JANE DOE",
    source_url: "https://example.invalid/source",
    last_synced_at: null,
    total_due_rows: "1",
    ...overrides,
  };
}

describe("minnesotaCandidateFinanceBatchSync", () => {
  it("lists active Minnesota finance links that are due for sync", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Jane Doe",
        election_year: 2026,
        office_scope: "statewide",
        office_name: "Governor",
        district: null,
        committee_id: "1001",
        committee_name: "FRIENDS OF JANE DOE",
        source_url: "https://example.invalid/source",
        last_synced_at: null,
        total_due_rows: "1",
      },
    ]);

    await expect(
      listDueMinnesotaCandidateFinanceSyncRows(db, {
        now: NOW,
        staleAfterDays: 7,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
      })
    ).resolves.toEqual({
      totalDueRows: 1,
      rows: [
        {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Jane Doe",
          electionYear: 2026,
          officeScope: "statewide",
          officeName: "Governor",
          district: null,
          committeeId: "1001",
          committeeName: "FRIENDS OF JANE DOE",
          sourceUrl: "https://example.invalid/source",
          lastSyncedAt: null,
        },
      ],
    });

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.mn_candidate_finance_links AS link");
    expect(sql).toContain("link.link_status = 'active'");
    expect(sql).toContain("district.state = 'MN'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($6::text[])");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      7,
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

  it("syncs selected due candidates with trusted linked committees", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Jane Doe",
        election_year: 2026,
        office_scope: "statewide",
        office_name: "Governor",
        district: null,
        committee_id: "1001",
        committee_name: "FRIENDS OF JANE DOE",
        source_url: "https://example.invalid/source",
        last_synced_at: null,
        total_due_rows: "1",
      },
    ]);

    const syncMinnesotaCandidateFinanceFn = vi.fn().mockResolvedValue({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2026,
      dryRun: false,
      resolution: {
        status: "matched",
        committeeId: "1001",
        committeeName: "FRIENDS OF JANE DOE",
        confidence: "exact",
        source: "mn_board_viewer",
        sourceUrl: "https://example.invalid/source",
        matchedCandidateRowCount: 1,
      },
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      totalReceipts: null,
      directContributionTotal: null,
      totalDisbursements: null,
      outsideSupportTotal: null,
      outsideOpposeTotal: null,
      matchedContributionRowCount: 0,
      includedContributionRowCount: 0,
      skippedContributionRowCount: 0,
      matchedOutsideExpenditureRowCount: 0,
      includedOutsideExpenditureRowCount: 0,
      skippedOutsideExpenditureRowCount: 0,
      matchedOutsideContributionRowCount: 0,
      includedOutsideContributionRowCount: 0,
      skippedOutsideContributionRowCount: 0,
    });

    const fetchMinnesotaCandidateFinancialSummaryFn = vi.fn().mockResolvedValue({
      committeeId: "1001",
      electionYear: 2026,
      totalReceipts: 1000,
      directContributionTotal: 900,
      totalDisbursements: 400,
      sourceUrl: "https://example.invalid/financial-summary",
    });

    const result = await syncDueMinnesotaCandidateFinance({
      db,
      now: NOW,
      maxCandidates: 25,
      staleAfterDays: 7,
      electionLookbackDays: 1,
      electionLookaheadDays: 730,
      autoLinkMissingLinks: false,
      contributionRows: [],
      expenditureRows: [],
      outsideContributionRows: [],
      syncMinnesotaCandidateFinanceFn: syncMinnesotaCandidateFinanceFn as never,
      fetchMinnesotaCandidateFinancialSummaryFn,
    });

    expect(result).toMatchObject({
      dryRun: false,
      now: NOW.toISOString(),
      staleAfterDays: 7,
      maxCandidates: 25,
      dueCandidateCount: 1,
      selectedCandidateCount: 1,
      syncedCandidateCount: 1,
      failedCandidateCount: 0,
      autoLinkAttemptedCount: 0,
      autoLinkLinkedCount: 0,
    });
    expect(result.results).toHaveLength(1);
    expect(fetchMinnesotaCandidateFinancialSummaryFn).toHaveBeenCalledWith({
      committeeId: "1001",
      electionYear: 2026,
    });
    expect(syncMinnesotaCandidateFinanceFn).toHaveBeenCalledTimes(1);
    expect(syncMinnesotaCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Jane Doe",
        electionYear: 2026,
        officeScope: "statewide",
        officeName: "Governor",
        district: null,
        sourceUrl: "https://example.invalid/source",
        contributionRows: [],
        contributionSourceUrl: null,
        expenditureRows: [],
        outsideContributionRows: [],
        outsideSourceUrl: null,
        financialSummary: {
          committeeId: "1001",
          electionYear: 2026,
          totalReceipts: 1000,
          directContributionTotal: 900,
          totalDisbursements: 400,
          sourceUrl: "https://example.invalid/financial-summary",
        },
        trustedCommittee: {
          committeeId: "1001",
          committeeName: "FRIENDS OF JANE DOE",
          sourceUrl: "https://example.invalid/source",
        },
        dryRun: false,
        now: NOW,
      })
    );
  });

  it("continues the candidate sync without new direct totals when CFB is unavailable", async () => {
    const db = createMockDb([
      {
        candidate_id: CANDIDATE_ID,
        election_id: ELECTION_ID,
        candidate_name: "Jane Doe",
        election_year: 2026,
        office_scope: "statewide",
        office_name: "Governor",
        district: null,
        committee_id: "1001",
        committee_name: "FRIENDS OF JANE DOE",
        source_url: "https://example.invalid/source",
        last_synced_at: null,
        total_due_rows: "1",
      },
    ]);
    const fetchMinnesotaCandidateFinancialSummaryFn = vi.fn().mockRejectedValue(new Error("temporary outage"));
    const syncMinnesotaCandidateFinanceFn = vi.fn().mockResolvedValue({ candidateId: CANDIDATE_ID });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);

    try {
      const result = await syncDueMinnesotaCandidateFinance({
        db,
        now: NOW,
        autoLinkMissingLinks: false,
        contributionRows: [],
        expenditureRows: [],
        outsideContributionRows: [],
        syncMinnesotaCandidateFinanceFn: syncMinnesotaCandidateFinanceFn as never,
        fetchMinnesotaCandidateFinancialSummaryFn,
      });

      expect(result.syncedCandidateCount).toBe(1);
      expect(result.failedCandidateCount).toBe(0);
      expect(syncMinnesotaCandidateFinanceFn).toHaveBeenCalledWith(
        expect.objectContaining({ financialSummary: undefined })
      );
      expect(warn).toHaveBeenCalledWith(
        "Minnesota CFB financial summary unavailable for committee 1001; preserving existing direct totals:",
        "temporary outage"
      );
    } finally {
      warn.mockRestore();
    }
  });

  it("preserves existing direct totals without warning when CFB explicitly reports no data", async () => {
    const db = createMockDb([dueQueryRow()]);
    const fetchMinnesotaCandidateFinancialSummaryFn = vi.fn().mockResolvedValue(null);
    const syncMinnesotaCandidateFinanceFn = vi.fn().mockResolvedValue({ candidateId: CANDIDATE_ID });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);

    try {
      const result = await syncDueMinnesotaCandidateFinance({
        db,
        now: NOW,
        autoLinkMissingLinks: false,
        contributionRows: [],
        expenditureRows: [],
        outsideContributionRows: [],
        syncMinnesotaCandidateFinanceFn: syncMinnesotaCandidateFinanceFn as never,
        fetchMinnesotaCandidateFinancialSummaryFn,
      });

      expect(result.syncedCandidateCount).toBe(1);
      expect(result.failedCandidateCount).toBe(0);
      expect(syncMinnesotaCandidateFinanceFn).toHaveBeenCalledWith(
        expect.objectContaining({ financialSummary: undefined })
      );
      expect(warn).not.toHaveBeenCalled();
    } finally {
      warn.mockRestore();
    }
  });

  it("reuses a financial-summary request for candidates sharing a committee and election", async () => {
    const db = createMockDb([
      dueQueryRow({ total_due_rows: "2" }),
      dueQueryRow({
        candidate_id: "33333333-3333-4333-8333-333333333333",
        election_id: "44444444-4444-4444-8444-444444444444",
        candidate_name: "John Doe",
        total_due_rows: "2",
      }),
    ]);
    const fetchMinnesotaCandidateFinancialSummaryFn = vi.fn().mockResolvedValue({
      committeeId: "1001",
      electionYear: 2026,
      totalReceipts: 1000,
      directContributionTotal: 900,
      totalDisbursements: 400,
      sourceUrl: "https://example.invalid/financial-summary",
    });
    const syncMinnesotaCandidateFinanceFn = vi.fn().mockResolvedValue({ candidateId: CANDIDATE_ID });

    const result = await syncDueMinnesotaCandidateFinance({
      db,
      now: NOW,
      autoLinkMissingLinks: false,
      contributionRows: [],
      expenditureRows: [],
      outsideContributionRows: [],
      syncMinnesotaCandidateFinanceFn: syncMinnesotaCandidateFinanceFn as never,
      fetchMinnesotaCandidateFinancialSummaryFn,
    });

    expect(result.syncedCandidateCount).toBe(2);
    expect(fetchMinnesotaCandidateFinancialSummaryFn).toHaveBeenCalledTimes(1);
    expect(syncMinnesotaCandidateFinanceFn).toHaveBeenCalledTimes(2);
  });
});
