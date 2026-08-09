import { afterEach, describe, expect, it, vi } from "vitest";
import { runSanFranciscoFinanceJob } from "../../src/scheduler/sanFranciscoCandidateFinanceSyncScheduler.js";
afterEach(() => vi.unstubAllEnvs());
describe("San Francisco finance scheduler", () => {
  it("master flag cannot be bypassed by force", async () => {
    vi.stubEnv("SAN_FRANCISCO_CAMPAIGN_FINANCE_ENABLED", "false");
    vi.stubEnv("SAN_FRANCISCO_CAMPAIGN_FINANCE_SYNC_ENABLED", "true");
    await expect(
      runSanFranciscoFinanceJob({ force: true }),
    ).resolves.toMatchObject({ enabled: false, force: true });
  });

  it("rejects unsafe integer job options loudly", async () => {
    await expect(
      runSanFranciscoFinanceJob({ maxCandidates: 0 }),
    ).rejects.toThrow(/Invalid San Francisco finance scheduler maxCandidates/);
    await expect(
      runSanFranciscoFinanceJob({ staleAfterDays: 9007199254740993 }),
    ).rejects.toThrow(/Invalid San Francisco finance scheduler staleAfterDays/);
  });
});
