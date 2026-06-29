import { describe, expect, it } from "vitest";

import { parseUpsertNebraskaCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertNebraskaCandidateFinanceSyncScheduler.js";

describe("upsertNebraskaCandidateFinanceSyncScheduler script", () => {
  it("parses recurring scheduler options", () => {
    expect(
      parseUpsertNebraskaCandidateFinanceSyncSchedulerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days=365",
        "--raw-cache-dir=/tmp/nadc",
        "--raw-zip=/tmp/nadc.zip",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 365,
      rawDataCacheDir: "/tmp/nadc",
      rawDataZipPath: "/tmp/nadc.zip",
    });
  });

  it("rejects unknown flags instead of silently ignoring typos", () => {
    expect(() => parseUpsertNebraskaCandidateFinanceSyncSchedulerArgs(["--max-canddates=5"])).toThrow(
      "Unknown Nebraska candidate finance scheduler upsert flag: --max-canddates"
    );
  });

  it("rejects boolean flags with explicit values", () => {
    expect(() => parseUpsertNebraskaCandidateFinanceSyncSchedulerArgs(["--dry-run=true"])).toThrow(
      "Boolean flag must not include a value: --dry-run"
    );
    expect(() => parseUpsertNebraskaCandidateFinanceSyncSchedulerArgs(["--force=false"])).toThrow(
      "Boolean flag must not include a value: --force"
    );
  });
});
