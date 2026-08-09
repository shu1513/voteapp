import { describe, expect, it } from "vitest";

import { assertKnownCliFlags } from "../../src/scripts/financeCliFlagGuard.js";
import { parseSyncDueCandidateFinanceScriptArgs } from "../../src/scripts/syncDueCandidateFinance.js";
import { parseTexasCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerTexasCandidateFinanceSync.js";
import { parseRefreshTexasTecRawDataScriptArgs } from "../../src/scripts/refreshTexasTecRawData.js";

describe("assertKnownCliFlags", () => {
  const BOOL = new Set(["--dry-run", "--force"]);
  const VALUE = new Set(["--max-candidates", "--cache-dir"]);
  const check = (args: string[]) => assertKnownCliFlags(args, "test finance", BOOL, VALUE);

  it("accepts known flags in boolean, space, and inline forms", () => {
    expect(() =>
      check(["--dry-run", "--force", "--max-candidates", "5", "--cache-dir=/tmp/x"])
    ).not.toThrow();
    expect(() => check([])).not.toThrow();
  });

  it("rejects an unknown flag such as the --dryrun typo", () => {
    expect(() => check(["--dryrun"])).toThrow(/Unknown test finance flag: --dryrun/);
    expect(() => check(["--max-candidate=5"])).toThrow(/Unknown test finance flag: --max-candidate/);
  });

  it("rejects positional tokens that are not value-flag values", () => {
    // npm eats the first "--" separator, so a dash-less typo arrives as a
    // bare positional — it must fail loudly, never silently run a real
    // sync.
    expect(() => check(["dry-run"])).toThrow(/Unexpected positional argument: dry-run/);
    expect(() => check(["--dry-run", "force"])).toThrow(/Unexpected positional argument: force/);
    // An extra token after a consumed value is a stray positional too.
    expect(() => check(["--max-candidates", "5", "7"])).toThrow(/Unexpected positional argument: 7/);
    // The inline "=" form consumes no following token.
    expect(() => check(["--cache-dir=/tmp/x", "7"])).toThrow(/Unexpected positional argument: 7/);
  });

  it("rejects a boolean flag with an inline value", () => {
    expect(() => check(["--dry-run=true"])).toThrow(/Boolean flag does not accept a value/);
  });
});

// Representative integration through the three ported shapes: a state
// trigger, the federal sync-due, and a raw-data refresh CLI. Every other
// ported script wires the same helper the same way.
describe("ported finance CLI parsers reject silent typos", () => {
  it("Texas trigger", () => {
    expect(parseTexasCandidateFinanceSyncTriggerArgs(["--dry-run"])).toMatchObject({ dryRun: true });
    expect(() => parseTexasCandidateFinanceSyncTriggerArgs(["dry-run"])).toThrow(
      /Unexpected positional argument: dry-run/
    );
    expect(() => parseTexasCandidateFinanceSyncTriggerArgs(["--dryrun"])).toThrow(
      /Unknown Texas candidate finance sync flag: --dryrun/
    );
    expect(() => parseTexasCandidateFinanceSyncTriggerArgs(["--dry-run=true"])).toThrow(
      /Boolean flag does not accept a value/
    );
  });

  it("federal sync-due", () => {
    expect(parseSyncDueCandidateFinanceScriptArgs(["--dry-run"])).toMatchObject({ dryRun: true });
    expect(() => parseSyncDueCandidateFinanceScriptArgs(["dry-run"])).toThrow(
      /Unexpected positional argument: dry-run/
    );
  });

  it("Texas TEC raw-data refresh", () => {
    expect(() => parseRefreshTexasTecRawDataScriptArgs(["force"])).toThrow(
      /Unexpected positional argument: force/
    );
    expect(() => parseRefreshTexasTecRawDataScriptArgs(["--froce"])).toThrow(
      /Unknown Texas TEC raw data refresh flag: --froce/
    );
  });
});
