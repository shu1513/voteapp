import { describe, expect, it } from "vitest";

import { parseIndianaCampaignFinanceRawDataRefreshTriggerArgs } from "../../src/scripts/triggerIndianaCampaignFinanceRawDataRefresh.js";

describe("triggerIndianaCampaignFinanceRawDataRefresh script", () => {
  it("parses raw-data refresh trigger options", () => {
    expect(
      parseIndianaCampaignFinanceRawDataRefreshTriggerArgs([
        "--force",
        "--year",
        "2026",
        "--artifact-kind",
        "contribution",
        "--url",
        "https://example.test/2026_ContributionData.csv.zip",
        "--cache-dir",
        "/cache/indiana",
        "--timeout-ms",
        "5000",
      ])
    ).toEqual({
      force: true,
      year: 2026,
      artifactKind: "contribution",
      url: "https://example.test/2026_ContributionData.csv.zip",
      cacheDir: "/cache/indiana",
      timeoutMs: 5000,
    });
  });

  it("rejects malformed flags", () => {
    expect(() => parseIndianaCampaignFinanceRawDataRefreshTriggerArgs(["--year=202x"])).toThrow(
      "Invalid --year value: 202x"
    );
    expect(() => parseIndianaCampaignFinanceRawDataRefreshTriggerArgs(["--artifact-kind=summary"])).toThrow(
      "Invalid Indiana campaign finance artifact kind: summary"
    );
    expect(() => parseIndianaCampaignFinanceRawDataRefreshTriggerArgs(["--timeout-ms=5x"])).toThrow(
      "Invalid --timeout-ms value: 5x"
    );
    expect(() => parseIndianaCampaignFinanceRawDataRefreshTriggerArgs(["--url"])).toThrow("Missing --url value");
    expect(() => parseIndianaCampaignFinanceRawDataRefreshTriggerArgs(["--url="])).toThrow("Missing --url value");
    expect(() => parseIndianaCampaignFinanceRawDataRefreshTriggerArgs(["--cache-dir", "   "])).toThrow(
      "Missing --cache-dir value"
    );
  });
});
