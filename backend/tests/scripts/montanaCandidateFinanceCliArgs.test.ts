import { describe, expect, it } from "vitest";

import { parseReportMontanaOutsideSpendingScriptArgs } from "../../src/scripts/reportMontanaOutsideSpending.js";
import { parseSyncDueMontanaCandidateFinanceScriptArgs } from "../../src/scripts/syncDueMontanaCandidateFinance.js";

describe("Montana finance CLI flag validation", () => {
  it("keeps flag sets per script — report-only flags are rejected by the due sync", () => {
    // "--year" on the due sync would read as scoping and silently sync
    // every due year; strict validation must refuse it.
    expect(() => parseSyncDueMontanaCandidateFinanceScriptArgs(["--year", "2026"])).toThrow("Unknown option: --year");
    expect(() => parseSyncDueMontanaCandidateFinanceScriptArgs(["--refresh"])).toThrow("Unknown option: --refresh");
    expect(
      parseSyncDueMontanaCandidateFinanceScriptArgs(["--dry-run", "--max-candidates", "5"])
    ).toMatchObject({ dryRun: true, maxCandidates: 5 });
  });

  it("accepts exactly the report flags and requires --year", () => {
    expect(parseReportMontanaOutsideSpendingScriptArgs(["--year", "2026", "--refresh"])).toEqual({
      year: 2026,
      refresh: true,
    });
    expect(parseReportMontanaOutsideSpendingScriptArgs(["--year=2026"])).toEqual({ year: 2026, refresh: false });
    expect(() => parseReportMontanaOutsideSpendingScriptArgs([])).toThrow("--year is required");
    expect(() => parseReportMontanaOutsideSpendingScriptArgs(["--year", "2026", "--max-candidates", "5"])).toThrow(
      "Unknown option: --max-candidates"
    );
  });

  it("rejects the = form on boolean flags instead of silently ignoring it", () => {
    // The parsers read booleans via args.includes("--flag"); an = form
    // would validate and then never activate.
    expect(() => parseReportMontanaOutsideSpendingScriptArgs(["--year", "2026", "--refresh=true"])).toThrow(
      "--refresh takes no value"
    );
    expect(() => parseSyncDueMontanaCandidateFinanceScriptArgs(["--dry-run=true"])).toThrow("--dry-run takes no value");
  });
});
