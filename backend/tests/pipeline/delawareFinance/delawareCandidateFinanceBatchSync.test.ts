import { describe, expect, it, vi } from "vitest";

import { syncDueDelawareCandidateFinance } from "../../../src/pipeline/delawareFinance/delawareCandidateFinanceBatchSync.js";
import type { DelawareCandidateFinanceDueRow } from "../../../src/pipeline/delawareFinance/delawareCandidateFinanceDueList.js";

function dueRow(overrides: Partial<DelawareCandidateFinanceDueRow> = {}): DelawareCandidateFinanceDueRow {
  return {
    candidateId: "c1",
    electionId: "e1",
    candidateName: "Jane Example",
    electionYear: 2026,
    electionDate: "2026-11-03",
    officeScope: "statewide",
    officeName: "Attorney General",
    district: null,
    cfId: "01009999",
    committeeName: "Jane Example for Delaware",
    linkSource: "cfrs_portal",
    sourceUrl: null,
    lastSyncedAt: null,
    ...overrides,
  };
}

const db = { query: vi.fn(), connect: vi.fn() } as never;

describe("syncDueDelawareCandidateFinance", () => {
  it("auto-links, syncs each due row, and isolates per-candidate failures", async () => {
    const autoLinkFn = vi.fn().mockResolvedValue([{ candidateId: "cX", electionId: "eX", status: "linked" }]);
    const syncCandidateFn = vi
      .fn()
      .mockResolvedValueOnce({ dryRun: false, totalReceipts: 700 })
      .mockRejectedValueOnce(new Error("No Delaware CFRS artifact bundle cached for CF_ID 01008888"));
    const listDueRowsFn = vi.fn().mockResolvedValue({
      rows: [dueRow(), dueRow({ candidateId: "c2", cfId: "01008888" })],
      totalDueRows: 2,
    });

    const result = await syncDueDelawareCandidateFinance({
      db,
      now: new Date("2026-08-28T00:00:00.000Z"),
      autoLinkFn,
      listDueRowsFn,
      syncCandidateFn,
      log: () => {},
    });

    expect(autoLinkFn).toHaveBeenCalledTimes(1);
    expect(result.attempted).toBe(2);
    expect(result.succeeded).toBe(1);
    expect(result.failed).toBe(1);
    expect(result.candidates[1]?.error).toContain("No Delaware CFRS artifact bundle cached");
    expect(syncCandidateFn.mock.calls[0]?.[0]).toMatchObject({ candidateId: "c1", dryRun: false });
  });

  it("skips auto-link on dry runs and continues when the auto-link pass fails", async () => {
    const autoLinkFn = vi.fn().mockRejectedValue(new Error("portal down"));
    const listDueRowsFn = vi.fn().mockResolvedValue({ rows: [], totalDueRows: 0 });

    const dry = await syncDueDelawareCandidateFinance({ db, dryRun: true, autoLinkFn, listDueRowsFn, log: () => {} });
    expect(autoLinkFn).not.toHaveBeenCalled();
    expect(dry.attempted).toBe(0);

    const live = await syncDueDelawareCandidateFinance({ db, autoLinkFn, listDueRowsFn, log: () => {} });
    expect(autoLinkFn).toHaveBeenCalledTimes(1);
    expect(live.autoLinkResults).toEqual([]);
  });
});
