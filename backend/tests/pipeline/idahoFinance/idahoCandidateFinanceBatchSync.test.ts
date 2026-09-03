import { describe, expect, it, vi } from "vitest";

import { syncDueIdahoCandidateFinance } from "../../../src/pipeline/idahoFinance/idahoCandidateFinanceBatchSync.js";
import type { IdahoCandidateFinanceDueRow } from "../../../src/pipeline/idahoFinance/idahoCandidateFinanceDueList.js";
import type { IdahoCfsDataClient } from "../../../src/pipeline/idahoFinance/idahoCandidateFinanceSync.js";
import { GUID_A, GUID_B, independentExpenditure, registration } from "./idahoTestFixtures.js";

const NOW = new Date("2026-09-03T00:00:00.000Z");
const GRID = [registration({ registrationGuid: GUID_A })];
const IE_ROWS = [independentExpenditure()];

function dueRow(overrides: Partial<IdahoCandidateFinanceDueRow> = {}): IdahoCandidateFinanceDueRow {
  return {
    candidateId: "candidate-1",
    electionId: "election-1",
    candidateName: "Todd Achilles",
    electionYear: 2026,
    electionDate: "2026-11-03",
    officeScope: "state_upper",
    officeName: "State Senator",
    district: "16",
    registrationGuid: GUID_A,
    filerName: "Achilles, Todd Baker",
    linkSource: "sunshine_grid",
    sourceUrl: null,
    lastSyncedAt: null,
    ...overrides,
  };
}

function createClient(overrides: Partial<IdahoCfsDataClient> = {}): IdahoCfsDataClient {
  return {
    getRegistrations: vi.fn().mockResolvedValue(GRID),
    getContributionPage: vi.fn(),
    getIndependentExpenditures: vi.fn().mockResolvedValue(IE_ROWS),
    ...overrides,
  };
}

const db = { query: vi.fn(), connect: vi.fn() } as never;

describe("syncDueIdahoCandidateFinance", () => {
  it("pulls the grid and the IE list once, shares them with auto-link and every sync, and applies the defaults", async () => {
    const client = createClient();
    const autoLink = vi.fn().mockResolvedValue([{ status: "linked" }]);
    const listDueRows = vi.fn().mockResolvedValue({
      rows: [dueRow(), dueRow({ candidateId: "candidate-2", registrationGuid: GUID_B })],
      totalDueRows: 2,
    });
    const sync = vi.fn().mockResolvedValue({ directCoverageNote: null });
    const result = await syncDueIdahoCandidateFinance({
      db,
      now: NOW,
      cfsClient: client,
      autoLinkFn: autoLink,
      listDueRowsFn: listDueRows,
      syncCandidateFn: sync,
      log: () => {},
    });

    expect(client.getRegistrations).toHaveBeenCalledTimes(1);
    expect(client.getIndependentExpenditures).toHaveBeenCalledTimes(1);
    expect(autoLink).toHaveBeenCalledWith(
      expect.objectContaining({ registrations: GRID, maxCandidates: null, electionLookbackDays: 98, electionLookaheadDays: 730 })
    );
    expect(listDueRows).toHaveBeenCalledWith(db, {
      now: NOW,
      staleAfterDays: 7,
      maxCandidates: 25,
      electionLookbackDays: 98,
      electionLookaheadDays: 730,
    });
    expect(sync).toHaveBeenCalledTimes(2);
    expect(sync.mock.calls[0]![0]).toMatchObject({
      candidateId: "candidate-1",
      link: { registrationGuid: GUID_A, filerName: "Achilles, Todd Baker", linkSource: "sunshine_grid", sourceUrl: null },
      registrations: GRID,
      expenditureRows: IE_ROWS,
      dryRun: false,
    });
    expect(result).toMatchObject({
      dryRun: false,
      autoLinkResults: [{ status: "linked" }],
      totalDueRows: 2,
      attempted: 2,
      succeeded: 2,
      failed: 0,
      registrationCount: 1,
      independentExpenditureRowCount: 1,
      independentExpenditureError: null,
    });
  });

  it("skips the outside leg for every link when the IE pull fails, and records per-link failures without stopping", async () => {
    const client = createClient({ getIndependentExpenditures: vi.fn().mockRejectedValue(new Error("ie down")) });
    const sync = vi.fn().mockRejectedValueOnce(new Error("partial page")).mockResolvedValueOnce({ directCoverageNote: "note" });
    const logs: string[] = [];
    const result = await syncDueIdahoCandidateFinance({
      db,
      now: NOW,
      cfsClient: client,
      autoLinkMissingLinks: false,
      listDueRowsFn: vi.fn().mockResolvedValue({ rows: [dueRow(), dueRow({ candidateId: "candidate-2" })], totalDueRows: 2 }),
      syncCandidateFn: sync,
      log: (message) => logs.push(message),
    });
    expect(sync.mock.calls[0]![0]).toMatchObject({ expenditureRows: null });
    expect(result).toMatchObject({
      succeeded: 1,
      failed: 1,
      independentExpenditureRowCount: null,
      independentExpenditureError: "ie down",
    });
    expect(result.candidates[0]).toMatchObject({ ok: false, error: "partial page" });
    expect(result.candidates[1]).toMatchObject({ ok: true });
    expect(logs).toEqual([
      "Idaho IE list pull failed (direct sync continues, outside leg skipped): ie down",
      `Idaho finance sync failed for Todd Achilles (${GUID_A}): partial page`,
      `Idaho coverage for Todd Achilles (${GUID_A}): note`,
    ]);
  });

  it("continues with existing links when the auto-link pass fails, and pulls nothing when nothing is due", async () => {
    const client = createClient({ getRegistrations: vi.fn().mockRejectedValue(new Error("grid down")) });
    const result = await syncDueIdahoCandidateFinance({
      db,
      now: NOW,
      cfsClient: client,
      listDueRowsFn: vi.fn().mockResolvedValue({ rows: [], totalDueRows: 0 }),
      syncCandidateFn: vi.fn(),
      log: () => {},
    });
    expect(result.autoLinkResults).toEqual([]);
    expect(result.attempted).toBe(0);
    expect(result.registrationCount).toBeNull();
    expect(client.getIndependentExpenditures).not.toHaveBeenCalled();
  });

  it("skips the auto-link pass in dry-run mode and passes dryRun to every sync", async () => {
    const client = createClient();
    const autoLink = vi.fn();
    const sync = vi.fn().mockResolvedValue({ directCoverageNote: null });
    await syncDueIdahoCandidateFinance({
      db,
      now: NOW,
      dryRun: true,
      cfsClient: client,
      autoLinkFn: autoLink,
      listDueRowsFn: vi.fn().mockResolvedValue({ rows: [dueRow()], totalDueRows: 1 }),
      syncCandidateFn: sync,
      log: () => {},
    });
    expect(autoLink).not.toHaveBeenCalled();
    expect(sync.mock.calls[0]![0]).toMatchObject({ dryRun: true });
  });

  it("rejects non-positive batch limits", async () => {
    await expect(syncDueIdahoCandidateFinance({ db, now: NOW, maxCandidates: 0, log: () => {} })).rejects.toThrow(
      "Invalid Idaho finance batch maxCandidates: 0"
    );
  });
});
