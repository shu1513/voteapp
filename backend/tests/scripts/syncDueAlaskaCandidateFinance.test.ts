import { describe, expect, it } from "vitest";

import {
  parseSyncDueAlaskaCandidateFinanceScriptArgs,
  toSyncDueAlaskaCandidateFinanceScriptOutput,
} from "../../src/scripts/syncDueAlaskaCandidateFinance.js";

describe("syncDueAlaskaCandidateFinance script", () => {
  it("parses optional flags", () => {
    expect(
      parseSyncDueAlaskaCandidateFinanceScriptArgs([
        "--write",
        "--force",
        "--auto-link",
        "--max-candidates=12",
        "--stale-after-days",
        "3",
        "--lookback-days=10",
        "--lookahead-days=365",
        "--csv",
        "--income-csv=/tmp/alaska-income.csv",
        "--ie-expenditures-csv=/tmp/alaska-ie-exp.csv",
        "--ie-contributions-csv=/tmp/alaska-ie-con.csv",
        "--income-url=https://example.test/income.csv",
        "--timeout-ms=1000",
        "--retry-count=1",
        "--retry-delay-ms=0",
        "--request-spacing-ms=0",
      ])
    ).toEqual({
      dryRun: false,
      force: true,
      autoLinkMissingLinks: true,
      maxCandidates: 12,
      staleAfterDays: 3,
      electionLookbackDays: 10,
      electionLookaheadDays: 365,
      dataSourceMode: "csv",
      incomeCsvPath: "/tmp/alaska-income.csv",
      independentExpendituresCsvPath: "/tmp/alaska-ie-exp.csv",
      independentContributionsCsvPath: "/tmp/alaska-ie-con.csv",
      incomeUrl: "https://example.test/income.csv",
      independentExpendituresUrl: undefined,
      independentContributionsUrl: undefined,
      timeoutMs: 1000,
      retryCount: 1,
      retryDelayMs: 0,
      requestSpacingMs: 0,
    });
  });

  it("defaults to dry-run with auto-link disabled", () => {
    expect(parseSyncDueAlaskaCandidateFinanceScriptArgs([])).toMatchObject({
      dryRun: true,
      force: false,
      autoLinkMissingLinks: false,
      dataSourceMode: "csv",
    });
  });

  it("rejects conflicting write and auto-link flags", () => {
    expect(() => parseSyncDueAlaskaCandidateFinanceScriptArgs(["--dry-run", "--write"])).toThrow(
      "Provide either --dry-run or --write, not both"
    );
    expect(() => parseSyncDueAlaskaCandidateFinanceScriptArgs(["--auto-link", "--no-auto-link"])).toThrow(
      "Provide either --auto-link or --no-auto-link, not both"
    );
    expect(() => parseSyncDueAlaskaCandidateFinanceScriptArgs(["--live", "--csv"])).toThrow(
      "Provide either --live or --csv, not both"
    );
    expect(() => parseSyncDueAlaskaCandidateFinanceScriptArgs(["--live", "--data-source=csv"])).toThrow(
      "Provide --data-source or --live/--csv, not both"
    );
    expect(() =>
      parseSyncDueAlaskaCandidateFinanceScriptArgs(["--live", "--income-csv=/tmp/alaska-income.csv"])
    ).toThrow("Do not provide --income-csv, --ie-expenditures-csv, or --ie-contributions-csv when using live mode");
  });

  it("rejects malformed numeric flags strictly", () => {
    expect(() => parseSyncDueAlaskaCandidateFinanceScriptArgs(["--max-candidates=10abc"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseSyncDueAlaskaCandidateFinanceScriptArgs(["--stale-after-days=0"])).toThrow(
      "Invalid --stale-after-days value"
    );
    expect(() => parseSyncDueAlaskaCandidateFinanceScriptArgs(["--lookahead-days=5.5"])).toThrow(
      "Invalid --lookahead-days value"
    );
    expect(() => parseSyncDueAlaskaCandidateFinanceScriptArgs(["--retry-count=1.5"])).toThrow(
      "Invalid --retry-count value"
    );
  });

  it("rejects missing and duplicate option values", () => {
    expect(() => parseSyncDueAlaskaCandidateFinanceScriptArgs(["--stale-after-days"])).toThrow(
      "Missing --stale-after-days value"
    );
    expect(() => parseSyncDueAlaskaCandidateFinanceScriptArgs(["--lookback-days", "--force"])).toThrow(
      "Missing --lookback-days value"
    );
    expect(() =>
      parseSyncDueAlaskaCandidateFinanceScriptArgs(["--income-csv=/tmp/a.csv", "--income-csv=/tmp/b.csv"])
    ).toThrow("Provide --income-csv at most once");
  });

  it("formats script output", () => {
    const output = toSyncDueAlaskaCandidateFinanceScriptOutput({
      startedAt: new Date("2026-01-02T03:04:05.000Z"),
      options: {
        dryRun: true,
        force: false,
        autoLinkMissingLinks: true,
        dataSourceMode: "csv",
        maxCandidates: 2,
      },
      dataSource: {
        mode: "csv",
        income_source_url: "https://example.test/income.csv",
        independent_expenditure_source_url: null,
        independent_contribution_source_url: null,
        income_csv_path: "/tmp/income.csv",
        independent_expenditures_csv_path: null,
        independent_contributions_csv_path: null,
        timeout_ms: null,
        retry_count: null,
        retry_delay_ms: null,
        request_spacing_ms: null,
      },
      result: {
        dryRun: true,
        now: "2026-01-02T03:04:05.000Z",
        staleAfterDays: 7,
        maxCandidates: 2,
        dueCandidateCount: 3,
        selectedCandidateCount: 2,
        syncedCandidateCount: 1,
        failedCandidateCount: 1,
        autoLinkAttemptedCount: 2,
        autoLinkLinkedCount: 1,
        autoLinkResults: [],
        results: [],
      },
    });

    expect(output).toMatchObject({
      type: "alaska_candidate_finance_due_sync",
      started_at: "2026-01-02T03:04:05.000Z",
      dry_run: true,
      auto_link_missing_links: true,
      data_source: {
        mode: "csv",
        income_source_url: "https://example.test/income.csv",
      },
      result: {
        dueCandidateCount: 3,
        selectedCandidateCount: 2,
        autoLinkAttemptedCount: 2,
        autoLinkLinkedCount: 1,
      },
    });
    expect(typeof output.ts).toBe("string");
  });
});
