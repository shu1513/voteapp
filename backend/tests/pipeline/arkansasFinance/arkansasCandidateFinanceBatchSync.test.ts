import { describe, expect, it, vi } from "vitest";

import { syncDueArkansasCandidateFinance } from "../../../src/pipeline/arkansasFinance/arkansasCandidateFinanceBatchSync.js";
import type { ArkansasCandidateFinanceDueRow } from "../../../src/pipeline/arkansasFinance/arkansasCandidateFinanceDueList.js";

const NOW = new Date("2026-09-02T12:00:00Z");

function dueRow(overrides: Partial<ArkansasCandidateFinanceDueRow> = {}): ArkansasCandidateFinanceDueRow {
  return {
    candidateId: "11111111-1111-4111-8111-111111111111",
    electionId: "22222222-2222-4222-8222-222222222222",
    candidateName: "Jane Doe",
    electionYear: 2026,
    officeScope: "state_lower",
    officeName: "State Lower Chamber Legislator",
    district: "State House District 59",
    filingEntityId: 7968,
    filerName: "Doe, Jane A.",
    linkSource: "cfis_registration",
    sourceUrl: null,
    lastSyncedAt: null,
    ...overrides,
  };
}

describe("syncDueArkansasCandidateFinance", () => {
  it("auto-links, lists due rows, and syncs each with one shared registry loader", async () => {
    const db = { query: vi.fn(), connect: vi.fn() };
    const autoLinkFn = vi.fn(async () => []);
    const rows = [dueRow(), dueRow({ candidateId: "44444444-4444-4444-8444-444444444444", candidateName: "Rick Roe", filingEntityId: 7526, filerName: "Roe, Rick" })];
    const listDueRowsFn = vi.fn(async () => ({ rows, totalDueRows: 5 }));
    const syncCandidateFn = vi.fn(async (input: { link: { filingEntityId: number } }) => {
      if (input.link.filingEntityId === 7526) throw new Error("Arkansas CFIS carries entity 7526 2 times for the 2026 cycle");
      return { summaryWritten: true } as never;
    });
    const log = vi.fn();

    const result = await syncDueArkansasCandidateFinance({
      db: db as never,
      now: NOW,
      maxCandidates: 2,
      autoLinkFn,
      listDueRowsFn,
      syncCandidateFn,
      log,
    });

    expect(autoLinkFn).toHaveBeenCalledWith(
      expect.objectContaining({ now: NOW, maxCandidates: 2, electionLookbackDays: 38, electionLookaheadDays: 730 })
    );
    expect(listDueRowsFn).toHaveBeenCalledWith(db, {
      now: NOW,
      staleAfterDays: 7,
      maxCandidates: 2,
      electionLookbackDays: 38,
      electionLookaheadDays: 730,
    });
    const sharedLoader = autoLinkFn.mock.calls[0]![0].loadRegistrations;
    expect(typeof sharedLoader).toBe("function");
    expect(syncCandidateFn).toHaveBeenCalledTimes(2);
    for (const call of syncCandidateFn.mock.calls) {
      expect((call[0] as { loadRegistrations: unknown }).loadRegistrations).toBe(sharedLoader);
    }
    expect(syncCandidateFn.mock.calls[0]![0]).toMatchObject({
      candidateId: rows[0]!.candidateId,
      electionYear: 2026,
      officeName: "State Lower Chamber Legislator",
      district: "State House District 59",
      link: { filingEntityId: 7968, filerName: "Doe, Jane A.", linkSource: "cfis_registration", sourceUrl: null },
      dryRun: false,
      now: NOW,
    });
    expect(result).toMatchObject({ dryRun: false, totalDueRows: 5, attempted: 2, succeeded: 1, failed: 1 });
    expect(result.candidates[1]).toMatchObject({ ok: false, error: expect.stringContaining("2 times") });
    expect(log).toHaveBeenCalledWith(expect.stringContaining("Rick Roe (filer entity 7526)"));
  });

  it("skips auto-link on dry run or when disabled and survives an auto-link failure", async () => {
    const db = { query: vi.fn(), connect: vi.fn() };
    const listDueRowsFn = vi.fn(async () => ({ rows: [], totalDueRows: 0 }));
    const autoLinkFn = vi.fn(async () => {
      throw new Error("sweep failed");
    });
    const log = vi.fn();

    const dry = await syncDueArkansasCandidateFinance({ db: db as never, dryRun: true, autoLinkFn, listDueRowsFn, log });
    expect(dry).toMatchObject({ dryRun: true, autoLinkResults: [], attempted: 0 });
    expect(autoLinkFn).not.toHaveBeenCalled();

    await syncDueArkansasCandidateFinance({ db: db as never, autoLinkMissingLinks: false, autoLinkFn, listDueRowsFn, log });
    expect(autoLinkFn).not.toHaveBeenCalled();

    const live = await syncDueArkansasCandidateFinance({ db: db as never, autoLinkFn, listDueRowsFn, log });
    expect(autoLinkFn).toHaveBeenCalledTimes(1);
    expect(live.autoLinkResults).toEqual([]);
    expect(log).toHaveBeenCalledWith(expect.stringContaining("auto-link pass failed"));
  });
});
