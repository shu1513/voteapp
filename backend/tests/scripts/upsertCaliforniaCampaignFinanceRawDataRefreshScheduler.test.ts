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
  });
});
