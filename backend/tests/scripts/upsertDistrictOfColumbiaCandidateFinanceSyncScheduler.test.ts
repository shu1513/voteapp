import { describe, expect, it } from "vitest";

import { parseUpsertDistrictOfColumbiaCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertDistrictOfColumbiaCandidateFinanceSyncScheduler.js";

describe("upsertDistrictOfColumbiaCandidateFinanceSyncScheduler script", () => {
  it("parses recurring scheduler options", () => {
    expect(
      parseUpsertDistrictOfColumbiaCandidateFinanceSyncSchedulerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days",
        "3",
        "--lookback-days=2",
        "--lookahead-days=365",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 365,
    });
  });

  it("rejects malformed integer and missing flags", () => {
    expect(() => parseUpsertDistrictOfColumbiaCandidateFinanceSyncSchedulerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseUpsertDistrictOfColumbiaCandidateFinanceSyncSchedulerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
  });

  it("rejects duplicate flag values", () => {
    expect(() =>
      parseUpsertDistrictOfColumbiaCandidateFinanceSyncSchedulerArgs(["--max-candidates=5", "--max-candidates", "10"])
    ).toThrow("Provide --max-candidates at most once");
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() => parseUpsertDistrictOfColumbiaCandidateFinanceSyncSchedulerArgs(["--max-candidates=9007199254740993"])).toThrow(
      "Invalid --max-candidates value: 9007199254740993"
    );
  });

  it("rejects unknown flags", () => {
    expect(() => parseUpsertDistrictOfColumbiaCandidateFinanceSyncSchedulerArgs(["--dry-runn"])).toThrow(
      "Unknown District of Columbia candidate finance sync scheduler flag: --dry-runn"
    );
  });
});
