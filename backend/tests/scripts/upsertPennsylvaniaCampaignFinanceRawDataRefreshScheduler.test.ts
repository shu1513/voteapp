import { describe, expect, it } from "vitest";

import { parseUpsertPennsylvaniaCampaignFinanceRawDataRefreshSchedulerArgs } from "../../src/scripts/upsertPennsylvaniaCampaignFinanceRawDataRefreshScheduler.js";

const MOCK_PA_EXPORT_URL = "https://www.pa.gov/example/2026.zip";

describe("upsertPennsylvaniaCampaignFinanceRawDataRefreshScheduler script", () => {
  it("parses raw-data refresh scheduler options", () => {
    expect(
      parseUpsertPennsylvaniaCampaignFinanceRawDataRefreshSchedulerArgs([
        "--year=2026",
        "--force",
        `--url=${MOCK_PA_EXPORT_URL}`,
        "--cache-dir=/cache/pa",
        "--timeout-ms=5000",
      ])
    ).toEqual({
      year: 2026,
      force: true,
      url: MOCK_PA_EXPORT_URL,
      cacheDir: "/cache/pa",
      timeoutMs: 5000,
    });
  });

  it("rejects malformed flags", () => {
    expect(() => parseUpsertPennsylvaniaCampaignFinanceRawDataRefreshSchedulerArgs(["--year=20x6"])).toThrow(
      "Invalid --year value: 20x6"
    );
    expect(() => parseUpsertPennsylvaniaCampaignFinanceRawDataRefreshSchedulerArgs(["--timeout-ms=5x"])).toThrow(
      "Invalid --timeout-ms value: 5x"
    );
    expect(() => parseUpsertPennsylvaniaCampaignFinanceRawDataRefreshSchedulerArgs(["--cache-dir"])).toThrow(
      "Missing --cache-dir value"
    );
    expect(() => parseUpsertPennsylvaniaCampaignFinanceRawDataRefreshSchedulerArgs(["--url="])).toThrow(
      "Missing --url value"
    );
  });

  it("rejects a value flag given more than once instead of taking the first", () => {
    expect(() => parseUpsertPennsylvaniaCampaignFinanceRawDataRefreshSchedulerArgs(["--timeout-ms=10", "--timeout-ms", "20"])).toThrow(
      "Provide --timeout-ms at most once"
    );
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() => parseUpsertPennsylvaniaCampaignFinanceRawDataRefreshSchedulerArgs(["--timeout-ms=9007199254740993"])).toThrow(
      "Invalid --timeout-ms value: 9007199254740993"
    );
  });
});
