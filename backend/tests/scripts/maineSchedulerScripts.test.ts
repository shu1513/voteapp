import { describe, expect, it } from "vitest";

import { parseMaineCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerMaineCandidateFinanceSync.js";
import { parseMaineCfisRawDataRefreshTriggerArgs } from "../../src/scripts/triggerMaineCfisRawDataRefresh.js";
import { parseUpsertMaineCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertMaineCandidateFinanceSyncScheduler.js";
import { parseUpsertMaineCfisRawDataRefreshSchedulerArgs } from "../../src/scripts/upsertMaineCfisRawDataRefreshScheduler.js";

describe("Maine scheduler scripts", () => {
  it("rejects duplicate value flags for candidate sync trigger and scheduler upsert", () => {
    expect(() =>
      parseMaineCandidateFinanceSyncTriggerArgs(["--stale-after-days=7", "--stale-after-days", "30"])
    ).toThrow("Provide --stale-after-days at most once");
    expect(() =>
      parseUpsertMaineCandidateFinanceSyncSchedulerArgs(["--max-candidates=50", "--max-candidates=10"])
    ).toThrow("Provide --max-candidates at most once");
  });

  it("rejects unknown flags for candidate sync trigger and scheduler upsert", () => {
    expect(() => parseMaineCandidateFinanceSyncTriggerArgs(["--stale-days=7"])).toThrow(
      "Unknown Maine candidate finance sync flag: --stale-days"
    );
    expect(() => parseUpsertMaineCandidateFinanceSyncSchedulerArgs(["--max-canddates=10"])).toThrow(
      "Unknown Maine candidate finance sync scheduler flag: --max-canddates"
    );
  });

  it("rejects duplicate value flags for raw refresh trigger and scheduler upsert", () => {
    expect(() => parseMaineCfisRawDataRefreshTriggerArgs(["--year=2025", "--year", "2026"])).toThrow(
      "Provide --year at most once"
    );
    expect(() =>
      parseUpsertMaineCfisRawDataRefreshSchedulerArgs(["--timeout-ms=5000", "--timeout-ms", "1000"])
    ).toThrow("Provide --timeout-ms at most once");
  });

  it("rejects unknown flags for raw refresh trigger and scheduler upsert", () => {
    expect(() => parseMaineCfisRawDataRefreshTriggerArgs(["--artifact-knd=expenditures"])).toThrow(
      "Unknown Maine CFIS raw data refresh flag: --artifact-knd"
    );
    expect(() => parseUpsertMaineCfisRawDataRefreshSchedulerArgs(["--cache-directory=/tmp/cfis"])).toThrow(
      "Unknown Maine CFIS raw data refresh scheduler flag: --cache-directory"
    );
  });
});
