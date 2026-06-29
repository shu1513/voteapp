import { describe, expect, it } from "vitest";

import { parseNebraskaCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerNebraskaCandidateFinanceSync.js";

describe("triggerNebraskaCandidateFinanceSync script", () => {
  it("parses manual trigger options", () => {
    expect(
      parseNebraskaCandidateFinanceSyncTriggerArgs([
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
    expect(() => parseNebraskaCandidateFinanceSyncTriggerArgs(["--dryrun"])).toThrow(
      "Unknown Nebraska candidate finance sync trigger flag: --dryrun"
    );
  });
});
