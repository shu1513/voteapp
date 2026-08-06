import { describe, expect, it } from "vitest";

import { parseSyncDueOhioCandidateFinanceScriptArgs } from "../../src/scripts/syncDueOhioCandidateFinance.js";
import { parseOhioCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerOhioCandidateFinanceSync.js";
import { parseUpsertOhioCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertOhioCandidateFinanceSyncScheduler.js";

// All three Ohio finance CLI parsers share the same strict contract: an
// operator typo must fail loudly, never silently enqueue or run a REAL
// sync in place of the intended dry run.
const PARSERS = [
  ["sync-due", parseSyncDueOhioCandidateFinanceScriptArgs],
  ["trigger", parseOhioCandidateFinanceSyncTriggerArgs],
  ["scheduler-upsert", parseUpsertOhioCandidateFinanceSyncSchedulerArgs],
] as const;

describe.each(PARSERS)("Ohio finance CLI args (%s)", (_label, parse) => {
  it("parses the full flag set", () => {
    const options = parse([
      "--dry-run",
      "--force",
      "--max-candidates=5",
      "--stale-after-days",
      "3",
      "--raw-cache-dir=/tmp/cache",
    ]);
    expect(options).toMatchObject({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
    });
  });

  it("rejects an unknown flag such as the --dryrun typo", () => {
    expect(() => parse(["--dryrun"])).toThrow(/Unknown Ohio candidate finance/);
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
  });
});
