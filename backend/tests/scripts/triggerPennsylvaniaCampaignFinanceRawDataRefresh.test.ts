import { describe, expect, it } from "vitest";

import { parsePennsylvaniaCampaignFinanceRawDataRefreshTriggerArgs } from "../../src/scripts/triggerPennsylvaniaCampaignFinanceRawDataRefresh.js";

const MOCK_PA_EXPORT_URL = "https://www.pa.gov/example/2026.zip";

describe("triggerPennsylvaniaCampaignFinanceRawDataRefresh script", () => {
  it("parses raw-data refresh trigger options", () => {
    expect(
      parsePennsylvaniaCampaignFinanceRawDataRefreshTriggerArgs([
        "--year",
        "2026",
        "--force",
        "--url",
        MOCK_PA_EXPORT_URL,
        "--cache-dir",
        "/cache/pa",
        "--timeout-ms",
        "5000",
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
    expect(() => parsePennsylvaniaCampaignFinanceRawDataRefreshTriggerArgs(["--year=20x6"])).toThrow(
      "Invalid --year value: 20x6"
    );
    expect(() => parsePennsylvaniaCampaignFinanceRawDataRefreshTriggerArgs(["--timeout-ms=5x"])).toThrow(
      "Invalid --timeout-ms value: 5x"
    );
    expect(() => parsePennsylvaniaCampaignFinanceRawDataRefreshTriggerArgs(["--cache-dir"])).toThrow(
      "Missing --cache-dir value"
    );
    expect(() => parsePennsylvaniaCampaignFinanceRawDataRefreshTriggerArgs(["--url="])).toThrow("Missing --url value");
  });
});
