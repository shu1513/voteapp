import { describe, expect, it, vi } from "vitest";

import { syncDueNorthDakotaCandidateFinance } from "../../../src/pipeline/northDakotaFinance/northDakotaCandidateFinanceBatchSync.js";
import type { NorthDakotaCandidateFinanceDueRow } from "../../../src/pipeline/northDakotaFinance/northDakotaCandidateFinanceDueList.js";

function dueRow(overrides: Partial<NorthDakotaCandidateFinanceDueRow>): NorthDakotaCandidateFinanceDueRow {
  return {
    candidateId: "candidate-1",
    electionId: "election-1",
    candidateName: "Jane Doe",
    electionYear: 2026,
    officeScope: "state_upper",
    officeName: "State Senator",
    district: "State Senate District 11 (2024); North Dakota",
    entityId: "1010001478",
    committeeName: "Friends of Jane Doe",
    linkSource: "cfrs_registry",
    sourceUrl: null,
    lastSyncedAt: null,
    ...overrides,
  };
}

describe("syncDueNorthDakotaCandidateFinance", () => {
  it("lists due rows with the default window, syncs each, and records failures without stopping", async () => {
    const rows = [dueRow({}), dueRow({ candidateId: "candidate-2", candidateName: "Bad Row", entityId: "1010009999" })];
    const listDueRowsFn = vi.fn(async () => ({ rows, totalDueRows: 7 }));
    const syncCandidateFn = vi.fn(async (input: { candidateId: string; dryRun?: boolean }) => {
      if (input.candidateId === "candidate-2") throw new Error("2026 contributions do not reconcile");
      return { status: "synced", dryRun: input.dryRun === true, totalReceipts: 5390 } as never;
    });
    const log = vi.fn();
    const now = new Date("2026-09-02T00:00:00Z");
    const result = await syncDueNorthDakotaCandidateFinance({
      db: { query: vi.fn(), connect: vi.fn() } as never,
      now,
      dryRun: true,
      log,
      listDueRowsFn: listDueRowsFn as never,
      syncCandidateFn: syncCandidateFn as never,
    });
    expect(listDueRowsFn).toHaveBeenCalledWith(expect.anything(), {
      now,
      staleAfterDays: 7,
      maxCandidates: 25,
      electionLookbackDays: 103,
      electionLookaheadDays: 730,
    });
    expect(syncCandidateFn).toHaveBeenCalledTimes(2);
    expect(syncCandidateFn.mock.calls[0]![0]).toMatchObject({
      candidateId: "candidate-1",
      link: { entityId: "1010001478", committeeName: "Friends of Jane Doe", linkSource: "cfrs_registry", sourceUrl: null },
      dryRun: true,
      now,
    });
    expect(result).toMatchObject({ dryRun: true, totalDueRows: 7, attempted: 2, succeeded: 1, failed: 1 });
    expect(result.candidates[0]).toMatchObject({ ok: true, result: { totalReceipts: 5390 } });
    expect(result.candidates[1]).toMatchObject({ ok: false, error: "2026 contributions do not reconcile" });
    expect(log).toHaveBeenCalledWith(
      "North Dakota finance sync failed for Bad Row (entityId 1010009999): 2026 contributions do not reconcile"
    );
  });
});
