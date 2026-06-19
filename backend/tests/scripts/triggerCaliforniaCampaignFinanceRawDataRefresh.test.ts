import { describe, expect, it } from "vitest";

import { parseCaliforniaCampaignFinanceRawDataRefreshTriggerArgs } from "../../src/scripts/triggerCaliforniaCampaignFinanceRawDataRefresh.js";

describe("triggerCaliforniaCampaignFinanceRawDataRefresh script", () => {
  it("parses raw-data refresh trigger options", () => {
    expect(
      parseCaliforniaCampaignFinanceRawDataRefreshTriggerArgs([
        "--force",
        "--url",
        "https://example.test/db.zip",
        "--cache-dir",
        "/cache/calaccess",
        "--timeout-ms",
        "5000",
      ])
    ).toEqual({
      force: true,
      url: "https://example.test/db.zip",
      cacheDir: "/cache/calaccess",
      timeoutMs: 5000,
    });
  });

  it("rejects malformed numeric flags", () => {
    expect(() => parseCaliforniaCampaignFinanceRawDataRefreshTriggerArgs(["--timeout-ms=5x"])).toThrow(
      "Invalid --timeout-ms value: 5x"
    );
    expect(() => parseCaliforniaCampaignFinanceRawDataRefreshTriggerArgs(["--url"])).toThrow("Missing --url value");
  });
});
