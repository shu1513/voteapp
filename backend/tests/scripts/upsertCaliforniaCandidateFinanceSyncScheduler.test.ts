import { describe, expect, it } from "vitest";

import { parseUpsertCaliforniaCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertCaliforniaCandidateFinanceSyncScheduler.js";

describe("upsertCaliforniaCandidateFinanceSyncScheduler script", () => {
  it("parses scheduler job data flags", () => {
    expect(
      parseUpsertCaliforniaCandidateFinanceSyncSchedulerArgs([
        "--dry-run",
        "--force",
        "--skip-outside",
        "--max-candidates",
        "5",
        "--stale-after-days=3",
        "--lookback-days=2",
        "--lookahead-days=30",
        "--timeout-ms=5000",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      includeOutside: false,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 30,
      timeoutMs: 5000,
    });
  });

  it("rejects numeric flags without values", () => {
    expect(() => parseUpsertCaliforniaCandidateFinanceSyncSchedulerArgs(["--max-candidates"])).toThrow(
      "Missing --max-candidates value"
    );
    expect(() =>
      parseUpsertCaliforniaCandidateFinanceSyncSchedulerArgs(["--stale-after-days", "--timeout-ms=5000"])
    ).toThrow("Missing --stale-after-days value");
  });
});
