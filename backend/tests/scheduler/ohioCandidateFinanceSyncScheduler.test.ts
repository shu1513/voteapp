import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("ohioCandidateFinanceSyncScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.OHIO_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.OHIO_CAMPAIGN_FINANCE_SYNC_ENABLED;
  });

  afterEach(() => {
    process.env = { ...originalEnv };
    vi.resetModules();
  });

  it("rejects unsafe integer job options that Number() rounding could smuggle in", async () => {
    process.env.OHIO_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.OHIO_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runOhioCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/ohioCandidateFinanceSyncScheduler.js"
    );

    await expect(
      runOhioCandidateFinanceSyncJob({ triggeredBy: "manual", maxCandidates: 2 ** 53 })
    ).rejects.toThrow("Invalid Ohio finance sync scheduler maxCandidates");
    expect(Pool).not.toHaveBeenCalled();
  });
});
