import { describe, expect, it } from "vitest";

import { parseSyncDueNewJerseyCandidateFinanceScriptArgs } from "../../src/scripts/syncDueNewJerseyCandidateFinance.js";
import { parseNewJerseyCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerNewJerseyCandidateFinanceSync.js";
import { parseUpsertNewJerseyCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertNewJerseyCandidateFinanceSyncScheduler.js";

describe("New Jersey candidate finance scripts", () => {
  it("parses supported shared scheduler flags", () => {
    expect(
      parseSyncDueNewJerseyCandidateFinanceScriptArgs([
        "--dry-run",
        "--force",
        "--max-candidates=2",
        "--stale-after-days",
        "7",
        "--lookback-days=1",
        "--lookahead-days=365",
      ])
    ).toEqual({
      dryRun: true,
      force: true,
      maxCandidates: 2,
      staleAfterDays: 7,
      electionLookbackDays: 1,
      electionLookaheadDays: 365,
    });

    expect(parseNewJerseyCandidateFinanceSyncTriggerArgs(["--max-candidates", "3"])).toMatchObject({
      maxCandidates: 3,
    });
    expect(parseUpsertNewJerseyCandidateFinanceSyncSchedulerArgs(["--stale-after-days=4"])).toMatchObject({
      staleAfterDays: 4,
    });
  });

  it("rejects unknown or malformed arguments so dry-run typos do not write", () => {
    expect(() => parseSyncDueNewJerseyCandidateFinanceScriptArgs(["--dryrun"])).toThrow(
      "Unknown New Jersey campaign finance argument: --dryrun"
    );
    expect(() => parseNewJerseyCandidateFinanceSyncTriggerArgs(["--dry-run", "true"])).toThrow(
      "Unknown New Jersey campaign finance argument: true"
    );
    expect(() => parseNewJerseyCandidateFinanceSyncTriggerArgs(["--dry-run=true"])).toThrow(
      "Unexpected --dry-run value"
    );
    expect(() => parseUpsertNewJerseyCandidateFinanceSyncSchedulerArgs(["--max-candidates", "--force"])).toThrow(
      "Missing --max-candidates value"
    );
  });
});
