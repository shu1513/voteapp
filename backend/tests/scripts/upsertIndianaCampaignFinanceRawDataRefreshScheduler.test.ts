import { describe, expect, it } from "vitest";

import { parseUpsertIndianaCampaignFinanceRawDataRefreshSchedulerArgs } from "../../src/scripts/upsertIndianaCampaignFinanceRawDataRefreshScheduler.js";

describe("upsertIndianaCampaignFinanceRawDataRefreshScheduler script", () => {
  it("parses raw-data refresh scheduler options", () => {
    expect(
      parseUpsertIndianaCampaignFinanceRawDataRefreshSchedulerArgs([
        "--force",
        "--year=2026",
        "--artifact-kind=expenditure",
        "--url=https://example.test/2026_ExpenditureData.csv.zip",
        "--cache-dir=/cache/indiana",
        "--timeout-ms=5000",
      ])
    ).toEqual({
      force: true,
      year: 2026,
      artifactKind: "expenditure",
      url: "https://example.test/2026_ExpenditureData.csv.zip",
      cacheDir: "/cache/indiana",
      timeoutMs: 5000,
    });
  });

  it("rejects malformed flags", () => {
    expect(() => parseUpsertIndianaCampaignFinanceRawDataRefreshSchedulerArgs(["--year=202x"])).toThrow(
      "Invalid --year value: 202x"
    );
    expect(() =>
      parseUpsertIndianaCampaignFinanceRawDataRefreshSchedulerArgs(["--artifact-kind=filing"])
    ).toThrow("Invalid Indiana campaign finance artifact kind: filing");
    expect(() => parseUpsertIndianaCampaignFinanceRawDataRefreshSchedulerArgs(["--timeout-ms=5x"])).toThrow(
      "Invalid --timeout-ms value: 5x"
    );
    expect(() => parseUpsertIndianaCampaignFinanceRawDataRefreshSchedulerArgs(["--cache-dir"])).toThrow(
      "Missing --cache-dir value"
    );
    expect(() => parseUpsertIndianaCampaignFinanceRawDataRefreshSchedulerArgs(["--cache-dir="])).toThrow(
      "Missing --cache-dir value"
    );
    expect(() => parseUpsertIndianaCampaignFinanceRawDataRefreshSchedulerArgs(["--url", "   "])).toThrow(
      "Missing --url value"
    );
    expect(() => parseUpsertIndianaCampaignFinanceRawDataRefreshSchedulerArgs(["--bogus"])).toThrow(
      "Unknown Indiana campaign finance flag: --bogus"
    );
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() => parseUpsertIndianaCampaignFinanceRawDataRefreshSchedulerArgs(["--timeout-ms=9007199254740993"])).toThrow(
      "Invalid --timeout-ms value: 9007199254740993"
    );
  });
});
