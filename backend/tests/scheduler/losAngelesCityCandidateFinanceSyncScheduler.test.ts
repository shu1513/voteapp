import { afterEach, describe, expect, it, vi } from "vitest";
import { runLosAngelesCityFinanceJob } from "../../src/scheduler/losAngelesCityCandidateFinanceSyncScheduler.js";
afterEach(() => vi.unstubAllEnvs());
describe("Los Angeles City finance scheduler", () => {
  it("master flag cannot be bypassed by force", async () => {
    vi.stubEnv("LOS_ANGELES_CITY_CAMPAIGN_FINANCE_ENABLED", "false");
    vi.stubEnv("LOS_ANGELES_CITY_CAMPAIGN_FINANCE_SYNC_ENABLED", "true");
    await expect(
      runLosAngelesCityFinanceJob({ force: true }),
    ).resolves.toMatchObject({ enabled: false, force: true });
  });
});
