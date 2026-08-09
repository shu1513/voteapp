import { describe, expect, it } from "vitest";

import { parseGeorgiaCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerGeorgiaCandidateFinanceSync.js";
import { parseUpsertGeorgiaCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertGeorgiaCandidateFinanceSyncScheduler.js";

// Both Georgia scheduler CLI parsers share the same strict contract: an
// operator typo must fail loudly, never silently enqueue or run a REAL
// sync in place of the intended dry run.
const PARSERS = [
  ["trigger", parseGeorgiaCandidateFinanceSyncTriggerArgs],
  ["scheduler-upsert", parseUpsertGeorgiaCandidateFinanceSyncSchedulerArgs],
] as const;

describe.each(PARSERS)("Georgia finance scheduler CLI args (%s)", (_label, parse) => {
  it("parses the full flag set", () => {
    const options = parse([
      "--dry-run",
      "--force",
      "--max-candidates=5",
      "--stale-after-days",
      "3",
      "--lookback-days=14",
      "--lookahead-days",
      "365",
    ]);
    expect(options).toMatchObject({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 14,
      electionLookaheadDays: 365,
    });
  });

  it("rejects an unknown flag such as the --dryrun typo", () => {
    expect(() => parse(["--dryrun"])).toThrow(/Unknown Georgia candidate finance/);
  });

  it("rejects a boolean flag with an inline value", () => {
    // --dry-run=true would pass a name-only check yet fail the
    // args.includes("--dry-run") test and run a real sync.
    expect(() => parse(["--dry-run=true"])).toThrow(/Boolean flag does not accept a value/);
    expect(() => parse(["--force=1"])).toThrow(/Boolean flag does not accept a value/);
  });

  it("rejects a repeated value flag", () => {
    expect(() => parse(["--max-candidates=5", "--max-candidates=9"])).toThrow(/at most once/);
  });

  it("rejects a non-positive-integer value", () => {
    expect(() => parse(["--max-candidates=0"])).toThrow(/Invalid --max-candidates value/);
    expect(() => parse(["--stale-after-days", "x"])).toThrow(/Invalid --stale-after-days value/);
  });

  it("rejects a missing value", () => {
    expect(() => parse(["--max-candidates"])).toThrow(/Missing --max-candidates value/);
    expect(() => parse(["--lookback-days="])).toThrow(/Missing --lookback-days value/);
  });
});
