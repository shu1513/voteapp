import { describe, expect, it } from "vitest";

import { parseSyncDueNewYorkCityCandidateFinanceScriptArgs } from "../../src/scripts/syncDueNewYorkCityCandidateFinance.js";
import { parseNewYorkCityCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerNewYorkCityCandidateFinanceSync.js";

// Both NYC finance CLI parsers share the same strict contract: an operator
// typo must fail loudly, never silently enqueue or run a REAL sync in
// place of the intended dry run. They are tested separately because the
// trigger takes only boolean flags while sync-due also takes value flags.

describe("New York City finance sync-due CLI args", () => {
  const parse = parseSyncDueNewYorkCityCandidateFinanceScriptArgs;

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
      "--cache-dir=/tmp/nyc-cache",
    ]);
    expect(options).toMatchObject({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      electionLookbackDays: 14,
      electionLookaheadDays: 365,
      cacheDir: "/tmp/nyc-cache",
    });
  });

  it("rejects an unknown flag such as the --dryrun typo", () => {
    expect(() => parse(["--dryrun"])).toThrow(/Unknown New York City candidate finance/);
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

  it("rejects positional tokens that are not value-flag values", () => {
    // npm eats the first "--" separator, so a dash-less typo arrives as a
    // bare positional — it must fail loudly, never silently run a real
    // sync.
    expect(() => parse(["dry-run"])).toThrow(/Unexpected positional argument: dry-run/);
    expect(() => parse(["--dry-run", "force"])).toThrow(/Unexpected positional argument: force/);
    // An extra token after a consumed value is a stray positional too.
    expect(() => parse(["--max-candidates", "5", "7"])).toThrow(/Unexpected positional argument: 7/);
    // The inline "=" form consumes no following token.
    expect(() => parse(["--lookback-days=14", "7"])).toThrow(/Unexpected positional argument: 7/);
  });

  it("rejects an unsafe integer that Number() would silently round", () => {
    expect(() => parse(["--max-candidates", "9007199254740993"])).toThrow(
      /Invalid --max-candidates value: 9007199254740993/
    );
  });
});

describe("New York City finance trigger CLI args", () => {
  const parse = parseNewYorkCityCandidateFinanceSyncTriggerArgs;

  it("parses the boolean flags", () => {
    expect(parse(["--dry-run", "--force"])).toMatchObject({ dryRun: true, force: true });
    expect(parse([])).toMatchObject({ dryRun: false, force: false });
  });

  it("rejects unknown flags, inline values, and positionals", () => {
    expect(() => parse(["--dryrun"])).toThrow(/Unknown New York City candidate finance sync flag: --dryrun/);
    // --dry-run=true would silently enqueue a REAL sync under the old
    // args.includes check.
    expect(() => parse(["--dry-run=true"])).toThrow(/Unknown New York City candidate finance sync flag/);
    // npm eats the first "--" separator, so "-- dry-run" arrives bare.
    expect(() => parse(["dry-run"])).toThrow(/Unknown New York City candidate finance sync flag: dry-run/);
  });
});
