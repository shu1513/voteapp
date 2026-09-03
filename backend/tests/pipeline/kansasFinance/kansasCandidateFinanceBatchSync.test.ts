import { describe, expect, it, vi } from "vitest";

import { syncDueKansasCandidateFinance } from "../../../src/pipeline/kansasFinance/kansasCandidateFinanceBatchSync.js";
import type { KansasCandidateFinanceDueRow } from "../../../src/pipeline/kansasFinance/kansasCandidateFinanceDueList.js";

const NOW = new Date("2026-09-03T12:00:00.000Z");

function dueRow(overrides: Partial<KansasCandidateFinanceDueRow> = {}): KansasCandidateFinanceDueRow {
  return {
    candidateId: "11111111-1111-4111-8111-111111111111",
    electionId: "22222222-2222-4222-8222-222222222222",
    candidateName: "Margaret Holloway",
    electionYear: 2026,
    officeScope: "state_lower",
    officeName: "State Lower Chamber Legislator",
    district: "85",
    committeeId: "7:85:HOLLOWAY:MARGARET",
    committeeName: "HOLLOWAY MARGARET",
    linkSource: "cfr_viewer",
    sourceUrl: null,
    lastSyncedAt: null,
    ...overrides,
  };
}

describe("syncDueKansasCandidateFinance", () => {
  it("lists due rows and syncs each with one shared filing pool and KPDC loader, recording failures", async () => {
    const db = { query: vi.fn(), connect: vi.fn() };
    const rows = [dueRow(), dueRow({ candidateId: "44444444-4444-4444-8444-444444444444", candidateName: "Daniel Muir", committeeId: "7:2:MUIR:DANIEL", committeeName: "MUIR DANIEL", linkSource: "manual" })];
    const listDueRowsFn = vi.fn(async () => ({ rows, totalDueRows: 5 }));
    const syncCandidateFn = vi.fn(async (input: { link: { committeeId: string } }) => {
      if (input.link.committeeId === "7:2:MUIR:DANIEL") throw new Error("unpublishable: 2025-annual: no opened cover for the canonical paper version");
      return { status: "synced", summaryWritten: true } as never;
    });
    const log = vi.fn();

    const result = await syncDueKansasCandidateFinance({ db: db as never, now: NOW, maxCandidates: 2, listDueRowsFn, syncCandidateFn, log });

    expect(listDueRowsFn).toHaveBeenCalledWith(db, { now: NOW, staleAfterDays: 7, maxCandidates: 2, electionLookbackDays: 78, electionLookaheadDays: 730 });
    expect(syncCandidateFn).toHaveBeenCalledTimes(2);
    const first = syncCandidateFn.mock.calls[0]![0] as Record<string, unknown>;
    const second = syncCandidateFn.mock.calls[1]![0] as Record<string, unknown>;
    expect(typeof first.loadFilingPool).toBe("function");
    expect(second.loadFilingPool).toBe(first.loadFilingPool);
    expect(second.loadKpdcRows).toBe(first.loadKpdcRows);
    expect(first).toMatchObject({
      db,
      candidateId: rows[0]!.candidateId,
      electionId: rows[0]!.electionId,
      candidateName: "Margaret Holloway",
      electionYear: 2026,
      officeScope: "state_lower",
      officeName: "State Lower Chamber Legislator",
      district: "85",
      link: { committeeId: "7:85:HOLLOWAY:MARGARET", committeeName: "HOLLOWAY MARGARET", linkSource: "cfr_viewer", sourceUrl: null },
      now: NOW,
      dryRun: false,
    });
    expect(second).toMatchObject({ link: { committeeId: "7:2:MUIR:DANIEL", linkSource: "manual" } });
    expect(result).toMatchObject({ dryRun: false, totalDueRows: 5, attempted: 2, succeeded: 1, failed: 1 });
    expect(result.candidates[0]).toMatchObject({ ok: true, result: { status: "synced" } });
    expect(result.candidates[1]).toMatchObject({ ok: false, error: expect.stringContaining("no opened cover") });
    expect(log).toHaveBeenCalledWith(expect.stringContaining("Daniel Muir (7:2:MUIR:DANIEL)"));
  });

  it("passes dry run and explicit windows through", async () => {
    const db = { query: vi.fn(), connect: vi.fn() };
    const listDueRowsFn = vi.fn(async () => ({ rows: [dueRow()], totalDueRows: 1 }));
    const syncCandidateFn = vi.fn(async () => ({ status: "no_filed_report" }) as never);
    const result = await syncDueKansasCandidateFinance({
      db: db as never,
      now: NOW,
      dryRun: true,
      staleAfterDays: 1,
      electionLookbackDays: 40,
      electionLookaheadDays: 800,
      listDueRowsFn,
      syncCandidateFn,
      log: vi.fn(),
    });
    expect(listDueRowsFn).toHaveBeenCalledWith(db, { now: NOW, staleAfterDays: 1, maxCandidates: 25, electionLookbackDays: 40, electionLookaheadDays: 800 });
    expect(syncCandidateFn.mock.calls[0]![0]).toMatchObject({ dryRun: true });
    expect(result).toMatchObject({ dryRun: true, attempted: 1, succeeded: 1, failed: 0 });
  });
});
