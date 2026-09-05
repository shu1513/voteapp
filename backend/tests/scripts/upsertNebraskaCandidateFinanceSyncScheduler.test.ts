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
      "Unknown Nebraska candidate finance sync scheduler flag: --max-canddates"
    );
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() => parseUpsertNebraskaCandidateFinanceSyncSchedulerArgs(["--max-candidates=9007199254740993"])).toThrow(
      "Invalid --max-candidates value: 9007199254740993"
    );
  });

  it("rejects boolean flags with explicit values", () => {
    expect(() => parseUpsertNebraskaCandidateFinanceSyncSchedulerArgs(["--dry-run=true"])).toThrow(
      "Boolean flag does not accept a value: --dry-run"
    );
    expect(() => parseUpsertNebraskaCandidateFinanceSyncSchedulerArgs(["--force=false"])).toThrow(
      "Boolean flag does not accept a value: --force"
    );
  });
});
