import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("oregonCandidateFinanceSyncScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.OREGON_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.OREGON_CAMPAIGN_FINANCE_SYNC_ENABLED;
  });

  afterEach(() => {
    vi.useRealTimers();
    process.env = { ...originalEnv };
    vi.resetModules();
  });

  it("returns a disabled no-op result when the sync flag is off", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.OREGON_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.OREGON_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runOregonCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/oregonCandidateFinanceSyncScheduler.js"
    );

    const result = await runOregonCandidateFinanceSyncJob({
      triggeredBy: "daily",
      maxCandidates: 10,
      staleAfterDays: 7,
    });

    expect(result).toEqual({
      enabled: false,
      force: false,
      triggeredBy: "daily",
      dryRun: false,
      now: "2026-06-01T00:00:00.000Z",
      staleAfterDays: 7,
      maxCandidates: 10,
      dueCandidateCount: 0,
      selectedCandidateCount: 0,
      syncedCandidateCount: 0,
      failedCandidateCount: 0,
      autoLinkAttemptedCount: 0,
      autoLinkLinkedCount: 0,
      results: [],
    });
    expect(Pool).not.toHaveBeenCalled();
  });

  it("does not let force bypass the master Oregon finance flag", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
    process.env.OREGON_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.OREGON_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runOregonCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/oregonCandidateFinanceSyncScheduler.js"
    );

    const result = await runOregonCandidateFinanceSyncJob({
      triggeredBy: "manual",
      force: true,
      maxCandidates: 10,
      staleAfterDays: 7,
    });

    expect(result).toMatchObject({
      enabled: false,
      force: true,
      triggeredBy: "manual",
      dueCandidateCount: 0,
      selectedCandidateCount: 0,
      syncedCandidateCount: 0,
    });
    expect(Pool).not.toHaveBeenCalled();
  });
});
