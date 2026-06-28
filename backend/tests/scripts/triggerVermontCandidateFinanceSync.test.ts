import { describe, expect, it } from "vitest";

import { parseVermontCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerVermontCandidateFinanceSync.js";
import { parseUpsertVermontCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertVermontCandidateFinanceSyncScheduler.js";

describe("Vermont candidate finance sync scheduler scripts", () => {
  it("parses manual trigger flags", () => {
    expect(
      parseVermontCandidateFinanceSyncTriggerArgs([
        "--dry-run",
        "--force",
        "--max-candidates=5",
        "--stale-after-days=3",
        "--lookback-days=2",
        "--lookahead-days=400",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 2,
      electionLookaheadDays: 400,
    });
  });

  it("parses recurring scheduler flags", () => {
    expect(parseUpsertVermontCandidateFinanceSyncSchedulerArgs(["--max-candidates", "5"])).toMatchObject({
      maxCandidates: 5,
      dryRun: false,
      force: false,
    });
  });

  it("rejects invalid trigger flags", () => {
    expect(() => parseVermontCandidateFinanceSyncTriggerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseUpsertVermontCandidateFinanceSyncSchedulerArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
  });
});
