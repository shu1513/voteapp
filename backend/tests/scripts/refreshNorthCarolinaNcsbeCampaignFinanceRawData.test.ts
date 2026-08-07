import { describe, expect, it } from "vitest";

import {
  parseNcsbeCommitteeArg,
  parseRefreshNorthCarolinaNcsbeRawDataScriptArgs,
  runRefreshNorthCarolinaNcsbeRawDataScript,
  DEFAULT_NCSBE_CACHE_DIR,
} from "../../src/scripts/refreshNorthCarolinaNcsbeCampaignFinanceRawData.js";

describe("parseNcsbeCommitteeArg", () => {
  it("parses <SBoEID>:<OrgGroupID> and upper-cases the id", () => {
    expect(parseNcsbeCommitteeArg("sta-jv516o-c-001:57190")).toEqual({
      sboeId: "STA-JV516O-C-001",
      orgGroupId: 57190,
    });
  });

  it("rejects ids outside the pinned SBoEID pattern and bad group ids", () => {
    expect(() => parseNcsbeCommitteeArg("No Id:57190")).toThrow(/pinned pattern/);
    expect(() => parseNcsbeCommitteeArg("STA-JV516O-C-001")).toThrow(/<SBoEID>:<OrgGroupID>/);
    expect(() => parseNcsbeCommitteeArg("STA-JV516O-C-001:zero")).toThrow(/positive integer/);
  });
});

describe("parseRefreshNorthCarolinaNcsbeRawDataScriptArgs", () => {
  it("parses a full invocation", () => {
    const options = parseRefreshNorthCarolinaNcsbeRawDataScriptArgs([
      "--cycle-year=2026",
      "--committee",
      "STA-JV516O-C-001:57190",
      "--committee=STA-C0854N-C-001:31610",
      "--spacing-ms=1500",
      "--force",
      "--dry-run",
    ]);
    expect(options.cycleYear).toBe(2026);
    expect(options.committees).toEqual([
      { sboeId: "STA-JV516O-C-001", orgGroupId: 57190 },
      { sboeId: "STA-C0854N-C-001", orgGroupId: 31610 },
    ]);
    expect(options.includeIe).toBe(true);
    expect(options.spacingMs).toBe(1500);
    expect(options.force).toBe(true);
    expect(options.dryRun).toBe(true);
    expect(options.cacheDir.endsWith(DEFAULT_NCSBE_CACHE_DIR.split("/").pop()!)).toBe(true);
  });

  it("rejects unknown options — a misspelled flag must never start a paced pull", () => {
    expect(() => parseRefreshNorthCarolinaNcsbeRawDataScriptArgs(["--dryrun"])).toThrow(/Unknown option/);
    expect(() => parseRefreshNorthCarolinaNcsbeRawDataScriptArgs(["--dry-run=true"])).toThrow(
      /does not take a value/
    );
  });

  it("rejects duplicate committees and an empty run", () => {
    expect(() =>
      parseRefreshNorthCarolinaNcsbeRawDataScriptArgs([
        "--committee=STA-JV516O-C-001:57190",
        "--committee=STA-JV516O-C-001:57190",
      ])
    ).toThrow(/Duplicate --committee/);
    expect(() => parseRefreshNorthCarolinaNcsbeRawDataScriptArgs(["--skip-ie"])).toThrow(/Nothing to fetch/);
  });

  it("accepts an IE-only run with no committees", () => {
    const options = parseRefreshNorthCarolinaNcsbeRawDataScriptArgs(["--cycle-year=2026"]);
    expect(options.committees).toEqual([]);
    expect(options.includeIe).toBe(true);
  });

  it("refuses --cycle-year and --year together", () => {
    expect(() =>
      parseRefreshNorthCarolinaNcsbeRawDataScriptArgs(["--cycle-year=2026", "--year=2026"])
    ).toThrow(/not both/);
  });
});

describe("runRefreshNorthCarolinaNcsbeRawDataScript", () => {
  it("dry run reports the plan without any portal request", async () => {
    const options = parseRefreshNorthCarolinaNcsbeRawDataScriptArgs([
      "--cycle-year=2026",
      "--committee=STA-JV516O-C-001:57190",
      "--dry-run",
    ]);
    const output = await runRefreshNorthCarolinaNcsbeRawDataScript({
      options,
      now: new Date("2026-08-07T17:00:00Z"),
    });
    expect(output).toMatchObject({
      type: "north_carolina_ncsbe_raw_data_refresh",
      cycle_year: 2026,
      dry_run: true,
      include_ie: true,
      committees: [{ sboeId: "STA-JV516O-C-001", orgGroupId: 57190 }],
    });
  });
});
