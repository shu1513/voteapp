import { describe, expect, it } from "vitest";

import {
  parseSyncCandidateFinanceScriptArgs,
  toSyncCandidateFinanceScriptOutput,
} from "../../src/scripts/syncCandidateFinance.js";

describe("syncCandidateFinance script", () => {
  it("parses required and optional flags", () => {
    expect(
      parseSyncCandidateFinanceScriptArgs([
        "--fec-id=p80001571",
        "--year",
        "2024",
        "--dry-run",
        "--include-outside",
        "--per-page=25",
        "--top-groups",
        "12",
        "--timeout-ms=5000",
      ])
    ).toEqual({
      fecCandidateId: "P80001571",
      electionYear: 2024,
      dryRun: true,
      includeOutside: true,
      perPage: 25,
      outsideGroupLimit: 12,
      timeoutMs: 5000,
    });
  });

  it("rejects malformed flags strictly", () => {
    expect(() => parseSyncCandidateFinanceScriptArgs(["--year=2024"])).toThrow("Missing required --fec-id flag");
    expect(() => parseSyncCandidateFinanceScriptArgs(["--fec-id=X00000001", "--year=2024"])).toThrow(
      "Invalid --fec-id value"
    );
    expect(() => parseSyncCandidateFinanceScriptArgs(["--fec-id=P80001571", "--year=20x4"])).toThrow(
      "Invalid --year value"
    );
    expect(() => parseSyncCandidateFinanceScriptArgs(["--fec-id=P80001571", "--year=2024", "--per-page=10abc"])).toThrow(
      "Invalid --per-page value"
    );
  });

  it("formats script output", () => {
    const output = toSyncCandidateFinanceScriptOutput({
      startedAt: new Date("2026-01-02T03:04:05.000Z"),
      options: {
        fecCandidateId: "P80001571",
        electionYear: 2024,
        dryRun: true,
        includeOutside: true,
        perPage: 10,
        outsideGroupLimit: 5,
      },
      result: {
        fecCandidateId: "P80001571",
        electionYear: 2024,
        dryRun: true,
        directCommitteeCount: 1,
        summaryWritten: false,
        directBreakdownsWritten: 0,
        industryBreakdownsWritten: 0,
        classificationsWritten: 0,
        outsideIncluded: true,
        outsideGroupsWritten: 0,
        outsideGroupBreakdownsWritten: 0,
        outsideSupportTotal: 100,
        outsideOpposeTotal: 50,
      },
    });

    expect(output).toMatchObject({
      type: "candidate_finance_sync",
      started_at: "2026-01-02T03:04:05.000Z",
      fec_candidate_id: "P80001571",
      election_year: 2024,
      dry_run: true,
      include_outside: true,
      result: {
        outsideSupportTotal: 100,
        outsideOpposeTotal: 50,
      },
    });
    expect(typeof output.ts).toBe("string");
  });
});
