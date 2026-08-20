import { afterEach, describe, expect, it, vi } from "vitest";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.resetModules();
});

describe("missouriCandidateFinanceSyncScheduler", () => {
  it("rejects unsafe integer job options before any service access", async () => {
    const { runMissouriCandidateFinanceSyncJob } = await import("../../src/scheduler/missouriCandidateFinanceSyncScheduler.js");
    await expect(runMissouriCandidateFinanceSyncJob({ maxCandidates: 2 ** 53 })).rejects.toThrow("Invalid Missouri finance sync scheduler maxCandidates");
  });

  it("does not let force bypass the master flag", async () => {
    vi.stubEnv("MISSOURI_CAMPAIGN_FINANCE_ENABLED", "false");
    vi.stubEnv("MISSOURI_CAMPAIGN_FINANCE_SYNC_ENABLED", "true");
    const { runMissouriCandidateFinanceSyncJob } = await import("../../src/scheduler/missouriCandidateFinanceSyncScheduler.js");
    expect(await runMissouriCandidateFinanceSyncJob({ force: true, triggeredBy: "manual" })).toMatchObject({
      enabled: false, force: true, selectedCandidateCount: 0,
    });
  });
});
