import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("northCarolinaCandidateFinanceSyncScheduler", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env = { ...originalEnv };
    delete process.env.NORTH_CAROLINA_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.NORTH_CAROLINA_CAMPAIGN_FINANCE_SYNC_ENABLED;
  });

  afterEach(() => {
    process.env = { ...originalEnv };
    vi.resetModules();
  });

  it("rejects unsafe integer job options that Number() rounding could smuggle in", async () => {
    process.env.NORTH_CAROLINA_CAMPAIGN_FINANCE_ENABLED = "true";
    process.env.NORTH_CAROLINA_CAMPAIGN_FINANCE_SYNC_ENABLED = "true";
    const Pool = vi.fn();
    vi.doMock("pg", () => ({ Pool }));

    const { runNorthCarolinaCandidateFinanceSyncJob } = await import(
      "../../src/scheduler/northCarolinaCandidateFinanceSyncScheduler.js"
    );

    await expect(
      runNorthCarolinaCandidateFinanceSyncJob({ triggeredBy: "manual", maxCandidates: 2 ** 53 })
    ).rejects.toThrow("Invalid North Carolina finance sync scheduler maxCandidates");
    expect(Pool).not.toHaveBeenCalled();
  });
});
