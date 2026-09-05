import { describe, expect, it } from "vitest";

import { parseRefreshIndianaCampaignFinanceRawDataScriptArgs as parseArgs } from "../../src/scripts/refreshIndianaCampaignFinanceRawData.js";

describe("refreshIndianaCampaignFinanceRawData script", () => {
  it("parses contribution refresh options with defaults", () => {
    expect(parseArgs(["--year=2026"])).toMatchObject({
      year: 2026,
      artifactKind: "contribution",
      url: "https://campaignfinance.in.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionData.csv.zip",
      force: false,
      timeoutMs: 300_000,
    });
  });

  it("parses expenditure refresh options", () => {
    expect(parseArgs(["--year", "2026", "--artifact-kind=expenditure", "--force", "--timeout-ms", "5000"])).toMatchObject({
      year: 2026,
      artifactKind: "expenditure",
      url: "https://campaignfinance.in.gov/PublicSite/Docs/BulkDataDownloads/2026_ExpenditureData.csv.zip",
      force: true,
      timeoutMs: 5000,
    });
  });

  it("rejects invalid flags", () => {
    expect(() => parseArgs(["--year=20x6"])).toThrow("Invalid --year value");
    expect(() => parseArgs(["--artifact-kind=unknown"])).toThrow("Invalid Indiana campaign finance artifact kind");
    expect(() => parseArgs(["--cache-dir", "   "])).toThrow("Missing --cache-dir value");
    expect(() => parseArgs(["--url=http://example.test/file.zip"])).toThrow("Only https is allowed");
    expect(() => parseArgs(["--year=2026", "--year=2027"])).toThrow("Provide --year at most once");
    expect(() => parseArgs(["--year=2026", "--bogus"])).toThrow("Unknown Indiana campaign finance flag: --bogus");
    expect(() => parseArgs(["--force=true"])).toThrow("Flag --force does not accept a value");
    expect(() => parseArgs(["--year=2026", "extra"])).toThrow("Unexpected Indiana campaign finance argument: extra");
  });
});
