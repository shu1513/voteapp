import { describe, expect, it, vi } from "vitest";

import { syncDueSouthCarolinaCandidateFinance } from "../../../src/pipeline/southCarolinaFinance/southCarolinaCandidateFinanceBatchSync.js";
import type { SouthCarolinaCandidateFinanceDueRow } from "../../../src/pipeline/southCarolinaFinance/southCarolinaCandidateFinanceDueList.js";

function dueRow(overrides: Partial<SouthCarolinaCandidateFinanceDueRow>): SouthCarolinaCandidateFinanceDueRow {
  return {
    candidateId: "candidate-1",
    electionId: "election-1",
    candidateName: "Pamela Evette",
    electionYear: 2026,
    electionDate: "2026-11-03",
    officeScope: "statewide",
    officeName: "Governor",
    district: null,
    candidateFilerId: 54395,
    filerName: "Evette, Pamela S",
    linkSource: "ethics_filer_search",
    sourceUrl: null,
    lastSyncedAt: null,
    ...overrides,
  };
}

const db = { query: vi.fn(), connect: vi.fn() } as never;

describe("syncDueSouthCarolinaCandidateFinance", () => {
  it("runs the auto-link pass, then syncs due rows, recording per-candidate failures", async () => {
    const autoLinkFn = vi.fn().mockResolvedValue([{ candidateId: "c", electionId: "e", status: "linked" }]);
    const listDueRowsFn = vi.fn().mockResolvedValue({
      rows: [dueRow({}), dueRow({ candidateId: "candidate-2", candidateName: "Boom" })],
      totalDueRows: 5,
    });
    const syncCandidateFn = vi.fn(async (input: { candidateName: string }) => {
      if (input.candidateName === "Boom") {
        throw new Error("aggregation failed");
      }
      return { status: "synced" } as never;
    });
    const log = vi.fn();

    const result = await syncDueSouthCarolinaCandidateFinance({
      db,
      now: new Date("2026-08-27T00:00:00.000Z"),
      maxCandidates: 2,
      autoLinkFn,
      listDueRowsFn,
      syncCandidateFn,
      log,
    });

    expect(autoLinkFn).toHaveBeenCalledWith(
      expect.objectContaining({ db, maxCandidates: 2, electionLookbackDays: 76, electionLookaheadDays: 730 })
    );
    expect(listDueRowsFn).toHaveBeenCalledWith(
      db,
      expect.objectContaining({ staleAfterDays: 7, maxCandidates: 2 })
    );
    expect(syncCandidateFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: "candidate-1",
        electionDate: "2026-11-03",
        filer: expect.objectContaining({ candidateFilerId: 54395, linkSource: "ethics_filer_search" }),
      })
    );
    expect(result).toMatchObject({
      dryRun: false,
      totalDueRows: 5,
      attempted: 2,
      succeeded: 1,
      failed: 1,
    });
    expect(result.autoLinkResults).toHaveLength(1);
    expect(result.candidates[1]).toMatchObject({ ok: false, error: "aggregation failed" });
    expect(log).toHaveBeenCalledWith(expect.stringContaining("Boom"));
  });

  it("skips auto-link in dry-run mode and when disabled, and survives auto-link failure", async () => {
    const listDueRowsFn = vi.fn().mockResolvedValue({ rows: [], totalDueRows: 0 });
    const autoLinkFn = vi.fn().mockRejectedValue(new Error("portal down"));
    const log = vi.fn();

    const dryRun = await syncDueSouthCarolinaCandidateFinance({
      db,
      dryRun: true,
      autoLinkFn,
      listDueRowsFn,
      syncCandidateFn: vi.fn(),
      log,
    });
    expect(autoLinkFn).not.toHaveBeenCalled();
    expect(dryRun).toMatchObject({ dryRun: true, attempted: 0 });

    await syncDueSouthCarolinaCandidateFinance({
      db,
      autoLinkMissingLinks: false,
      autoLinkFn,
      listDueRowsFn,
      syncCandidateFn: vi.fn(),
      log,
    });
    expect(autoLinkFn).not.toHaveBeenCalled();

    const survived = await syncDueSouthCarolinaCandidateFinance({
      db,
      autoLinkFn,
      listDueRowsFn,
      syncCandidateFn: vi.fn(),
      log,
    });
    expect(survived.autoLinkResults).toEqual([]);
    expect(log).toHaveBeenCalledWith(expect.stringContaining("portal down"));
  });
});
