import { describe, expect, it, vi } from "vitest";

import { syncDueGeorgiaCandidateFinance } from "../../../src/pipeline/georgiaFinance/georgiaCandidateFinanceBatchSync.js";
import type { GeorgiaCandidateFinanceSyncResult } from "../../../src/pipeline/georgiaFinance/georgiaCandidateFinanceSync.js";
import type { GeorgiaEthicsTransport } from "../../../src/pipeline/georgiaFinance/georgiaEthicsClient.js";

const NOW = new Date("2026-08-07T12:00:00Z");

const dummyTransport: GeorgiaEthicsTransport = {
  postJson: async () => {
    throw new Error("test must not touch the network");
  },
};

function dueQueryRow(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    candidate_id: "candidate-1",
    election_id: "election-1",
    candidate_name: "Christopher Carr",
    election_year: 2026,
    office_scope: "statewide",
    office_name: "Governor",
    district: null,
    committee_id: "100035",
    committee_name: "Carr for Georgia, Inc.",
    link_source: "peachfile_api",
    source_url: "https://ethics.ga.gov/records-search-all/",
    last_synced_at: null,
    total_due_rows: "2",
    ...overrides,
  };
}

function createMockDb(dueRows: Record<string, unknown>[]) {
  const query = vi.fn(async (sql: string) => {
    if (typeof sql === "string" && sql.includes("WITH due AS")) {
      return { rows: dueRows, rowCount: dueRows.length };
    }
    // Auto-link candidate-election listing: none missing.
    return { rows: [], rowCount: 0 };
  });
  const client = { query, release: vi.fn() };
  return { query, connect: vi.fn().mockResolvedValue(client), client };
}

function syncResult(overrides: Partial<GeorgiaCandidateFinanceSyncResult> = {}): GeorgiaCandidateFinanceSyncResult {
  return {
    candidateId: "candidate-1",
    electionId: "election-1",
    electionYear: 2026,
    dryRun: false,
    committeeId: "100035",
    linkWritten: true,
    summaryWritten: true,
    directBreakdownsWritten: 3,
    totalReceipts: 3500,
    totalDisbursements: 1200,
    cashOnHand: 2300,
    syncedRowSum: 3500,
    reconciliationDifference: 0,
    reconciliationTolerance: 2500,
    archiveRegistrationGuids: [],
    archiveRegistrationSource: "none",
    reportInventorySize: 2,
    peachfile: {
      fetchedRowCount: 2,
      includedRowCount: 2,
      supersededRowCount: 0,
      unassignedRowCount: 0,
      windowFilterIneffectiveCount: 0,
      sweepOnlyRowCount: 0,
      sweepMissedRowCount: 0,
      filterIneffective: false,
    },
    archive: {
      fetchedRowCount: 0,
      includedRowCount: 0,
      supersededRowCount: 0,
      unassignedRowCount: 0,
      windowFilterIneffectiveCount: 0,
      sweepOnlyRowCount: 0,
      sweepMissedRowCount: 0,
      filterIneffective: false,
    },
    aggregation: {
      syncedRowSum: 3500,
      totalRowCount: 2,
      bucketedRowCount: 2,
      occupationCoveredAmount: 3500,
      occupationUnknownAmount: 0,
      unitemizedAmount: 0,
      inKindAmount: 0,
      anonymousAmount: 0,
      returnedRowCount: 0,
      returnedAmount: 0,
      unpinnedSubTypeRowCount: 0,
      unpinnedSubTypeAmount: 0,
      unrecognizedStatusRowCount: 0,
      unrecognizedStatusAmount: 0,
    },
    ...overrides,
  };
}

describe("syncDueGeorgiaCandidateFinance", () => {
  it("syncs every due row through the injected sync fn and passes the link identity through", async () => {
    const dueRows = [dueQueryRow(), dueQueryRow({ candidate_id: "candidate-2", committee_id: "100200", link_source: "manual" })];
    const db = createMockDb(dueRows);
    const syncFn = vi.fn(async () => syncResult());
    const result = await syncDueGeorgiaCandidateFinance({
      db,
      transport: dummyTransport,
      now: NOW,
      syncGeorgiaCandidateFinanceFn: syncFn,
    });

    expect(result.dueCandidateCount).toBe(2);
    expect(result.selectedCandidateCount).toBe(2);
    expect(result.syncedCandidateCount).toBe(2);
    expect(result.failedCandidateCount).toBe(0);
    expect(syncFn).toHaveBeenCalledTimes(2);
    expect(syncFn).toHaveBeenCalledWith(
      expect.objectContaining({
        db,
        transport: dummyTransport,
        candidateId: "candidate-1",
        committee: expect.objectContaining({ committeeId: "100035", linkSource: "peachfile_api" }),
      })
    );
    expect(syncFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: "candidate-2",
        committee: expect.objectContaining({ committeeId: "100200", linkSource: "manual" }),
      })
    );
  });

  it("isolates per-candidate failures and keeps going", async () => {
    const dueRows = [dueQueryRow(), dueQueryRow({ candidate_id: "candidate-2", committee_id: "100200" })];
    const db = createMockDb(dueRows);
    const syncFn = vi
      .fn(async () => syncResult())
      .mockRejectedValueOnce(new Error("reconciliation failed"));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const result = await syncDueGeorgiaCandidateFinance({
        db,
        transport: dummyTransport,
        now: NOW,
        syncGeorgiaCandidateFinanceFn: syncFn,
      });
      expect(result.syncedCandidateCount).toBe(1);
      expect(result.failedCandidateCount).toBe(1);
      expect(result.results[0]).toMatchObject({ ok: false, error: "reconciliation failed" });
      expect(result.results[1]).toMatchObject({ ok: true });
    } finally {
      warn.mockRestore();
    }
  });

  it("dry run skips auto-link and passes dryRun through", async () => {
    const db = createMockDb([dueQueryRow()]);
    const syncFn = vi.fn(async () => syncResult({ dryRun: true, linkWritten: false }));
    const result = await syncDueGeorgiaCandidateFinance({
      db,
      transport: dummyTransport,
      now: NOW,
      dryRun: true,
      syncGeorgiaCandidateFinanceFn: syncFn,
    });
    expect(result.dryRun).toBe(true);
    expect(result.autoLinkAttemptedCount).toBe(0);
    expect(syncFn).toHaveBeenCalledWith(expect.objectContaining({ dryRun: true }));
    // The only db call was the due list — auto-link never queried.
    const sqls = db.query.mock.calls.map(([sql]) => sql as string);
    expect(sqls.every((sql) => sql.includes("WITH due AS"))).toBe(true);
  });
});
