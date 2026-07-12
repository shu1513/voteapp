import { afterEach, describe, expect, it } from "vitest";

import { runNewYorkCityFinanceSyncJob } from "../../src/scheduler/newYorkCityCandidateFinanceSyncScheduler.js";

const originalMaster = process.env.NEW_YORK_CITY_CAMPAIGN_FINANCE_ENABLED;
const originalSync = process.env.NEW_YORK_CITY_CAMPAIGN_FINANCE_SYNC_ENABLED;

afterEach(() => {
  if (originalMaster === undefined) delete process.env.NEW_YORK_CITY_CAMPAIGN_FINANCE_ENABLED;
  else process.env.NEW_YORK_CITY_CAMPAIGN_FINANCE_ENABLED = originalMaster;
  if (originalSync === undefined) delete process.env.NEW_YORK_CITY_CAMPAIGN_FINANCE_SYNC_ENABLED;
  else process.env.NEW_YORK_CITY_CAMPAIGN_FINANCE_SYNC_ENABLED = originalSync;
});
describe("newYorkCityCandidateFinanceSyncScheduler", () => {
  it("returns a disabled no-op without DB or Redis", async () => {
    process.env.NEW_YORK_CITY_CAMPAIGN_FINANCE_ENABLED = "false";
    process.env.NEW_YORK_CITY_CAMPAIGN_FINANCE_SYNC_ENABLED = "false";
    await expect(runNewYorkCityFinanceSyncJob({ triggeredBy: "daily" })).resolves.toEqual({
      enabled: false,
      triggeredBy: "daily",
      dryRun: false,
      dueCandidateCount: 0,
      selectedCandidateCount: 0,
      syncedCandidateCount: 0,
      deferredCandidateCount: 0,
      failedCandidateCount: 0,
      results: [],
    });
  });

  it("rejects invalid numeric options before external work", async () => {
    await expect(runNewYorkCityFinanceSyncJob({ maxCandidates: 0 })).rejects.toThrow("Invalid NYC finance scheduler maxCandidates");
  });
});
