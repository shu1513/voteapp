import { describe, expect, it, vi } from "vitest";

import {
  parseRefreshNorthDakotaCampaignFinanceRawDataScriptArgs as parseArgs,
  runRefreshNorthDakotaCampaignFinanceRawDataScript,
} from "../../src/scripts/refreshNorthDakotaCampaignFinanceRawData.js";
import { parseSyncDueNorthDakotaCandidateFinanceScriptArgs as parseSyncArgs } from "../../src/scripts/syncDueNorthDakotaCandidateFinance.js";

const REPS_HEADER = "ElectionName,ReportingCycle,ReportingPeriodDescription,FormType,ReportType,BeginDate,Enddate,DueDate";
const SCHEDULE_CSV: Record<number, string> = {
  2026: `${REPS_HEADER}\r\n2026 Election - Statewide,2025 REPORTING CYCLE,2025 Year End Report,Campaign Financial Statement,Year End,2025-01-01,2025-12-31,2026-01-31\r\n`,
  2027: `${REPS_HEADER}\r\n2026 Election - Statewide,2026 Reporting Cycle,2026 Year End Report,Campaign Financial Statement,Year End,2026-01-01,2026-12-31,2027-01-31\r\n`,
};

describe("refreshNorthDakotaCampaignFinanceRawData script", () => {
  it("parses the narrow raw-refresh contract", () => {
    expect(
      parseArgs(["--election-year=2026", "--election-year=2028", "--cache-dir=/cache", "--timeout-ms=5000", "--force"])
    ).toEqual({
      electionYears: [2026, 2028],
      cacheDir: "/cache",
      timeoutMs: 5000,
      force: true,
      acceptEmpty: false,
    });
    expect(parseArgs([])).toMatchObject({ electionYears: [new Date().getUTCFullYear()], force: false, acceptEmpty: false });
    expect(parseArgs(["--election-year=2026", "--election-year=2026"])).toMatchObject({ electionYears: [2026] });
  });

  it("rejects malformed and unknown arguments", () => {
    expect(() => parseArgs(["--election-year=26"])).toThrow("Invalid --election-year value");
    expect(() => parseArgs(["--election-year=2024"])).toThrow("Invalid North Dakota CFRS artifact year");
    expect(() => parseArgs(["--timeout-ms=0"])).toThrow("Invalid --timeout-ms value");
    expect(() => parseArgs(["--year=2026"])).toThrow("Unknown North Dakota CFRS raw data refresh flag");
    expect(() => parseArgs(["--force=true"])).toThrow("Boolean flag does not accept a value");
    expect(() => parseArgs(["--cache-dir=/a", "--cache-dir=/b"])).toThrow("Provide --cache-dir at most once");
  });

  it("rejects a flag given without a value instead of crashing on the missing token", () => {
    expect(() => parseArgs(["--cache-dir"])).toThrow("Missing --cache-dir value");
    expect(() => parseArgs(["--cache-dir", "--dry-run"])).toThrow("Missing --cache-dir value");
  });

  it("refreshes both schedule files, resolves the window, then every window year's data artifacts", async () => {
    const refreshed: string[] = [];
    const fakeRefresh = (kind: string, year: number) => {
      refreshed.push(`${kind}:${year}`);
      return Promise.resolve({
        status: "downloaded" as const,
        cacheDir: "/cache",
        filePath: `/cache/${kind}_${year}`,
        metadataPath: `/cache/${kind}_${year}.metadata.json`,
        previous: null,
        current: {
          version: 1 as const,
          artifact: { kind, year },
          filePath: `/cache/${kind}_${year}`,
          metadataPath: `/cache/${kind}_${year}.metadata.json`,
          downloadedAt: "2026-09-02T00:00:00.000Z",
          source: { catalogId: null, s3ReportFilePath: null, dataType: null },
          bytes: 1,
          sha256: "0".repeat(64),
          recordCount: 1,
          recoveredRowCount: 0,
        },
      } as never);
    };
    const result = await runRefreshNorthDakotaCampaignFinanceRawDataScript({
      options: { electionYears: [2026], cacheDir: "/cache", force: true, acceptEmpty: false, timeoutMs: undefined },
      fetchCatalog: vi.fn(async () => []),
      refreshBulk: vi.fn(async (input: { kind: string; year: number }) => fakeRefresh(input.kind, input.year)) as never,
      refreshApi: vi.fn(async (input: { year: number }) => fakeRefresh("api_contributions", input.year)) as never,
      refreshIe: vi.fn(async (input: { year: number }) => fakeRefresh("api_independent_expenditures", input.year)) as never,
      refreshRegistry: vi.fn(async (input: { electionYear: number }) => fakeRefresh("api_registry", input.electionYear)) as never,
      readBulk: vi.fn(async (input: { year: number }) => ({ csvText: SCHEDULE_CSV[input.year]! })) as never,
      pauseMs: 0,
    });
    expect(result.windows).toEqual([
      {
        election_year: 2026,
        election: "2026 Election - Statewide",
        window_start: "2025-01-01",
        window_end: "2026-12-31",
        window_years: [2025, 2026],
      },
    ]);
    expect(refreshed).toEqual([
      "reporting_schedules:2026",
      "reporting_schedules:2027",
      "contributions:2025",
      "api_contributions:2025",
      "api_independent_expenditures:2025",
      "contributions:2026",
      "api_contributions:2026",
      "api_independent_expenditures:2026",
      "api_registry:2026",
    ]);
    expect(result.artifacts).toHaveLength(9);
  });
});

describe("syncDueNorthDakotaCandidateFinance script", () => {
  it("parses the sync flags, allowing a zero staleness for a deliberate full pass", () => {
    expect(parseSyncArgs(["--dry-run", "--max-candidates=60", "--stale-after-days=0", "--cache-dir=/cache"])).toEqual({
      dryRun: true,
      maxCandidates: 60,
      staleAfterDays: 0,
      electionLookbackDays: undefined,
      electionLookaheadDays: undefined,
      cacheDir: "/cache",
    });
    expect(parseSyncArgs([])).toEqual({
      dryRun: false,
      maxCandidates: undefined,
      staleAfterDays: undefined,
      electionLookbackDays: undefined,
      electionLookaheadDays: undefined,
      cacheDir: undefined,
    });
  });

  it("rejects --force (no sync sub-gate exists), zero limits and unknown flags", () => {
    expect(() => parseSyncArgs(["--force"])).toThrow("Unknown North Dakota candidate finance due sync flag");
    expect(() => parseSyncArgs(["--max-candidates=0"])).toThrow("Invalid --max-candidates value: 0");
    expect(() => parseSyncArgs(["--stale-after-days=-1"])).toThrow("Invalid --stale-after-days value");
    expect(() => parseSyncArgs(["--dry-run=true"])).toThrow("Boolean flag does not accept a value");
    expect(() => parseSyncArgs(["dry-run"])).toThrow("Unexpected positional argument");
  });
});
