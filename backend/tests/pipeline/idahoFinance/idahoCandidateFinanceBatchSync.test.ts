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
    getIndependentExpenditurePage: vi.fn().mockResolvedValue({ items: IE_ROWS, totalItems: IE_ROWS.length }),
    ...overrides,
  };
}

const db = { query: vi.fn(), connect: vi.fn() } as never;
const storeRun = () => vi.fn().mockResolvedValue({ sha256: "run" });

describe("syncDueIdahoCandidateFinance", () => {
  it("pulls the grid and the IE list once, stores them as the run artifact, shares them with auto-link and every sync, and applies the defaults", async () => {
    const client = createClient();
    const autoLink = vi.fn().mockResolvedValue([{ status: "linked" }]);
    const listDueRows = vi.fn().mockResolvedValue({
      rows: [dueRow(), dueRow({ candidateId: "candidate-2", registrationGuid: GUID_B })],
      totalDueRows: 2,
    });
    const sync = vi.fn().mockResolvedValue({ directCoverageNote: null });
    const storeRunArtifact = storeRun();
    const result = await syncDueIdahoCandidateFinance({
      db,
      now: NOW,
      cfsClient: client,
      runCacheDir: "/tmp/runs",
      autoLinkFn: autoLink,
      listDueRowsFn: listDueRows,
      syncCandidateFn: sync,
      storeRunArtifactFn: storeRunArtifact,
      log: () => {},
    });

    expect(client.getRegistrations).toHaveBeenCalledTimes(1);
    expect(client.getIndependentExpenditurePage).toHaveBeenCalledTimes(1);
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
    expect(storeRunArtifact).toHaveBeenCalledWith({
      cacheDir: "/tmp/runs",
      retrievedAt: NOW,
      registrations: GRID,
      independentExpenditures: IE_ROWS,
    });
    // Evidence is stored before any link is written.
    expect(storeRunArtifact.mock.invocationCallOrder[0]!).toBeLessThan(sync.mock.invocationCallOrder[0]!);
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
      runArtifact: { sha256: "run" },
    });
  });

  it("fails the batch before any write when the IE list cannot be pulled or is partial", async () => {
    const sync = vi.fn();
    const storeRunArtifact = storeRun();
    const base = {
      db,
      now: NOW,
      autoLinkMissingLinks: false,
      listDueRowsFn: vi.fn().mockResolvedValue({ rows: [dueRow()], totalDueRows: 1 }),
      syncCandidateFn: sync,
      storeRunArtifactFn: storeRunArtifact,
      log: () => {},
    };
    await expect(
      syncDueIdahoCandidateFinance({
        ...base,
        cfsClient: createClient({ getIndependentExpenditurePage: vi.fn().mockRejectedValue(new Error("ie down")) }),
      })
    ).rejects.toThrow("ie down");
    await expect(
      syncDueIdahoCandidateFinance({
        ...base,
        cfsClient: createClient({
          getIndependentExpenditurePage: vi.fn().mockResolvedValue({ items: IE_ROWS, totalItems: 12 }),
        }),
      })
    ).rejects.toThrow("served 1 of 12 rows");
    expect(sync).not.toHaveBeenCalled();
    expect(storeRunArtifact).not.toHaveBeenCalled();
  });

  it("records per-link failures without stopping, and logs coverage notes", async () => {
    const sync = vi.fn().mockRejectedValueOnce(new Error("partial page")).mockResolvedValueOnce({ directCoverageNote: "note" });
    const logs: string[] = [];
    const result = await syncDueIdahoCandidateFinance({
      db,
      now: NOW,
      cfsClient: createClient(),
      autoLinkMissingLinks: false,
      listDueRowsFn: vi.fn().mockResolvedValue({ rows: [dueRow(), dueRow({ candidateId: "candidate-2" })], totalDueRows: 2 }),
      syncCandidateFn: sync,
      storeRunArtifactFn: storeRun(),
      log: (message) => logs.push(message),
    });
    expect(result).toMatchObject({ succeeded: 1, failed: 1 });
    expect(result.candidates[0]).toMatchObject({ ok: false, error: "partial page" });
    expect(result.candidates[1]).toMatchObject({ ok: true });
    expect(logs).toEqual([
      `Idaho finance sync failed for Todd Achilles (${GUID_A}): partial page`,
      `Idaho coverage for Todd Achilles (${GUID_A}): note`,
    ]);
  });

  it("requests the grid once even when it fails: the auto-link pass is skipped and the batch fails, without a retry per link", async () => {
    const client = createClient({ getRegistrations: vi.fn().mockRejectedValue(new Error("grid down")) });
    const logs: string[] = [];
    await expect(
      syncDueIdahoCandidateFinance({
        db,
        now: NOW,
        cfsClient: client,
        listDueRowsFn: vi.fn().mockResolvedValue({ rows: [dueRow(), dueRow({ candidateId: "candidate-2" })], totalDueRows: 2 }),
        syncCandidateFn: vi.fn(),
        storeRunArtifactFn: storeRun(),
        log: (message) => logs.push(message),
      })
    ).rejects.toThrow("grid down");
    expect(client.getRegistrations).toHaveBeenCalledTimes(1);
    expect(logs).toEqual(["Idaho auto-link pass failed (continuing with existing links): grid down"]);
  });

  it("continues with existing links when the auto-link pass fails, and pulls nothing more when nothing is due", async () => {
    const client = createClient();
    const result = await syncDueIdahoCandidateFinance({
      db,
      now: NOW,
      cfsClient: client,
      autoLinkFn: vi.fn().mockRejectedValue(new Error("db down")),
      listDueRowsFn: vi.fn().mockResolvedValue({ rows: [], totalDueRows: 0 }),
      syncCandidateFn: vi.fn(),
      storeRunArtifactFn: storeRun(),
      log: () => {},
    });
    expect(result).toMatchObject({
      autoLinkResults: [],
      attempted: 0,
      registrationCount: 1,
      independentExpenditureRowCount: null,
      runArtifact: null,
    });
    expect(client.getIndependentExpenditurePage).not.toHaveBeenCalled();
  });

  it("skips the auto-link pass and the run artifact in dry-run mode and passes dryRun to every sync", async () => {
    const autoLink = vi.fn();
    const sync = vi.fn().mockResolvedValue({ directCoverageNote: null });
    const storeRunArtifact = storeRun();
    const result = await syncDueIdahoCandidateFinance({
      db,
      now: NOW,
      dryRun: true,
      cfsClient: createClient(),
      autoLinkFn: autoLink,
      listDueRowsFn: vi.fn().mockResolvedValue({ rows: [dueRow()], totalDueRows: 1 }),
      syncCandidateFn: sync,
      storeRunArtifactFn: storeRunArtifact,
      log: () => {},
    });
    expect(autoLink).not.toHaveBeenCalled();
    expect(storeRunArtifact).not.toHaveBeenCalled();
    expect(result.runArtifact).toBeNull();
    expect(sync.mock.calls[0]![0]).toMatchObject({ dryRun: true });
  });

  it("rejects non-positive batch limits", async () => {
    await expect(syncDueIdahoCandidateFinance({ db, now: NOW, maxCandidates: 0, log: () => {} })).rejects.toThrow(
      "Invalid Idaho finance batch maxCandidates: 0"
    );
  });
});
