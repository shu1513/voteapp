import { describe, expect, it } from "vitest";

import { parseUpsertCaliforniaCampaignFinanceRawDataRefreshSchedulerArgs } from "../../src/scripts/upsertCaliforniaCampaignFinanceRawDataRefreshScheduler.js";

describe("upsertCaliforniaCampaignFinanceRawDataRefreshScheduler script", () => {
  it("parses raw-data refresh scheduler options", () => {
    expect(
      parseUpsertCaliforniaCampaignFinanceRawDataRefreshSchedulerArgs([
        "--force",
        "--url=https://example.test/db.zip",
        "--cache-dir=/cache/calaccess",
        "--timeout-ms=5000",
      ])
    ).toEqual({
      force: true,
      url: "https://example.test/db.zip",
      cacheDir: "/cache/calaccess",
      timeoutMs: 5000,
    });
  });

  it("rejects malformed numeric flags", () => {
    expect(() => parseUpsertCaliforniaCampaignFinanceRawDataRefreshSchedulerArgs(["--timeout-ms=5x"])).toThrow(
      "Invalid --timeout-ms value: 5x"
    );
    expect(() => parseUpsertCaliforniaCampaignFinanceRawDataRefreshSchedulerArgs(["--cache-dir"])).toThrow(
      "Missing --cache-dir value"
    );
    expect(() => parseUpsertCaliforniaCampaignFinanceRawDataRefreshSchedulerArgs(["--cache-dir="])).toThrow(
      "Missing --cache-dir value"
    );
    expect(() => parseUpsertCaliforniaCampaignFinanceRawDataRefreshSchedulerArgs(["--url="])).toThrow(
      "Missing --url value"
    );
    expect(() => parseUpsertCaliforniaCampaignFinanceRawDataRefreshSchedulerArgs(["--url", "   "])).toThrow(
      "Missing --url value"
    );
  });

  it("rejects a value flag given more than once instead of taking the first", () => {
    expect(() => parseUpsertCaliforniaCampaignFinanceRawDataRefreshSchedulerArgs(["--timeout-ms=10", "--timeout-ms", "20"])).toThrow(
      "Provide --timeout-ms at most once"
    );
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() => parseUpsertCaliforniaCampaignFinanceRawDataRefreshSchedulerArgs(["--timeout-ms=9007199254740993"])).toThrow(
      "Invalid --timeout-ms value: 9007199254740993"
    );
  });
});
