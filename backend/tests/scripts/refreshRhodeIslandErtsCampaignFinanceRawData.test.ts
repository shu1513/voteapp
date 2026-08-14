import { describe, expect, it } from "vitest";

import {
  parseErtsOrganizationArg,
  parseRefreshRhodeIslandErtsRawDataScriptArgs,
  runRefreshRhodeIslandErtsRawDataScript,
} from "../../src/scripts/refreshRhodeIslandErtsCampaignFinanceRawData.js";

describe("parseErtsOrganizationArg", () => {
  it("splits <OrgID>:<searchLastName>:<organizationName>, keeping colons in the name", () => {
    expect(parseErtsOrganizationArg("2235:McKee:DANIEL J MCKEE")).toEqual({
      orgId: "2235",
      searchLastName: "McKee",
      organizationName: "DANIEL J MCKEE",
    });
    expect(parseErtsOrganizationArg("7:Smith:FRIENDS OF: SMITH").organizationName).toBe("FRIENDS OF: SMITH");
  });

  it("rejects malformed entries", () => {
    expect(() => parseErtsOrganizationArg("2235:McKee")).toThrow(/--organization must be/);
    expect(() => parseErtsOrganizationArg("abc:McKee:X")).toThrow(/Invalid ERTS organization key/);
    expect(() => parseErtsOrganizationArg("2235::X")).toThrow(/non-empty/);
  });
});

describe("parseRefreshRhodeIslandErtsRawDataScriptArgs", () => {
  it("parses a full invocation", () => {
    const options = parseRefreshRhodeIslandErtsRawDataScriptArgs([
      "--cycle-year=2026",
      "--organization",
      "2235:McKee:DANIEL J MCKEE",
      "--spacing-ms=3000",
      "--force",
    ]);
    expect(options.cycleYear).toBe(2026);
    expect(options.organizations).toHaveLength(1);
    expect(options.includeCf8).toBe(true);
    expect(options.spacingMs).toBe(3_000);
    expect(options.force).toBe(true);
    expect(options.dryRun).toBe(false);
  });

  it("defaults the cycle year to the cycle containing today", () => {
    const options = parseRefreshRhodeIslandErtsRawDataScriptArgs([]);
    expect(options.cycleYear % 2).toBe(0);
  });

  it("rejects unknown flags — a misspelled --dry-run must not start a live pull", () => {
    expect(() => parseRefreshRhodeIslandErtsRawDataScriptArgs(["--dryrun"])).toThrow(/Unknown option/);
    expect(() => parseRefreshRhodeIslandErtsRawDataScriptArgs(["--force=yes"])).toThrow(/does not take a value/);
  });

  it("rejects duplicate organizations and an empty fetch set", () => {
    expect(() =>
      parseRefreshRhodeIslandErtsRawDataScriptArgs([
        "--organization=2235:McKee:DANIEL J MCKEE",
        "--organization=2235:McKee:DANIEL J MCKEE",
      ])
    ).toThrow(/Duplicate --organization/);
    expect(() => parseRefreshRhodeIslandErtsRawDataScriptArgs(["--skip-cf8"])).toThrow(/Nothing to fetch/);
  });
});

describe("runRefreshRhodeIslandErtsRawDataScript", () => {
  it("previews a dry run without touching the portal", async () => {
    const options = parseRefreshRhodeIslandErtsRawDataScriptArgs([
      "--cycle-year=2026",
      "--organization=2235:McKee:DANIEL J MCKEE",
      "--dry-run",
    ]);
    const output = await runRefreshRhodeIslandErtsRawDataScript({ options, now: new Date("2026-08-13T18:00:00Z") });
    expect(output).toMatchObject({
      type: "rhode_island_erts_raw_data_refresh",
      dry_run: true,
      cycle_year: 2026,
      cycle_begin: "01/01/2025",
      cycle_end: "12/31/2026",
      include_cf8: true,
      organizations: [{ org_id: "2235", search_last_name: "McKee", organization_name: "DANIEL J MCKEE" }],
    });
  });
});
