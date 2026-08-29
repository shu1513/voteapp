import { afterEach, describe, expect, it, vi } from "vitest";

import { syncDueMontanaCandidateFinance } from "../../../src/pipeline/montanaFinance/montanaCandidateFinanceBatchSync.js";
import type { MontanaCandidateFinanceDueRow } from "../../../src/pipeline/montanaFinance/montanaCandidateFinanceDueList.js";

function dueRow(overrides: Partial<MontanaCandidateFinanceDueRow>): MontanaCandidateFinanceDueRow {
  return {
    candidateId: "candidate-1",
    electionId: "election-1",
    candidateName: "David Bedey",
    electionYear: 2026,
    electionDate: "2026-11-03",
    officeScope: "state_upper",
    officeName: "State Senator",
    district: "43",
    committeeId: "21020",
    committeeName: "Bedey, David F.",
    linkSource: "cers_portal",
    sourceUrl: null,
    lastSyncedAt: null,
    ...overrides,
  };
}

const db = { query: vi.fn(), connect: vi.fn() } as never;

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("syncDueMontanaCandidateFinance", () => {
  it("skips acquisition when the raw-refresh flag is off and syncs from cache", async () => {
    const acquire = vi.fn();
    const sync = vi.fn().mockResolvedValue({ status: "synced" });
    const result = await syncDueMontanaCandidateFinance({
      db,
      now: new Date("2026-08-28T00:00:00.000Z"),
      autoLinkMissingLinks: false,
      listDueRowsFn: vi.fn().mockResolvedValue({ rows: [dueRow({})], totalDueRows: 1 }),
      acquireArtifactsFn: acquire,
      syncCandidateFn: sync,
      log: () => {},
    });
    expect(result.rawDataRefreshEnabled).toBe(false);
    expect(acquire).not.toHaveBeenCalled();
    expect(sync).toHaveBeenCalledTimes(1);
    expect(result.succeeded).toBe(1);
  });

  it("acquires fresh artifacts per candidate when forced, and records failures without stopping", async () => {
    vi.stubEnv("MONTANA_CAMPAIGN_FINANCE_ENABLED", "true");
    const acquire = vi.fn().mockResolvedValue({});
    const sync = vi
      .fn()
      .mockRejectedValueOnce(new Error("chain failed"))
      .mockResolvedValueOnce({ status: "synced" });
    const result = await syncDueMontanaCandidateFinance({
      db,
      now: new Date("2026-08-28T00:00:00.000Z"),
      forceRawDataRefresh: true,
      autoLinkMissingLinks: false,
      listDueRowsFn: vi.fn().mockResolvedValue({
        rows: [dueRow({}), dueRow({ candidateId: "candidate-2", committeeId: "21021" })],
        totalDueRows: 2,
      }),
      acquireArtifactsFn: acquire,
      syncCandidateFn: sync,
      log: () => {},
    });
    expect(result.rawDataRefreshEnabled).toBe(true);
    expect(acquire).toHaveBeenCalledTimes(2);
    expect(acquire).toHaveBeenCalledWith(expect.objectContaining({ candidateId: 21020, year: 2026 }));
    expect(result.succeeded).toBe(1);
    expect(result.failed).toBe(1);
    expect(result.candidates[0]).toMatchObject({ ok: false, error: "chain failed" });
  });

  it("continues with existing links when the auto-link pass fails", async () => {
    const result = await syncDueMontanaCandidateFinance({
      db,
      now: new Date("2026-08-28T00:00:00.000Z"),
      autoLinkFn: vi.fn().mockRejectedValue(new Error("registration list down")),
      listDueRowsFn: vi.fn().mockResolvedValue({ rows: [], totalDueRows: 0 }),
      log: () => {},
    });
    expect(result.autoLinkResults).toEqual([]);
    expect(result.attempted).toBe(0);
  });
});
