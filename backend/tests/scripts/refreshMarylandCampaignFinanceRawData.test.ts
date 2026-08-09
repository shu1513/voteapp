import { describe, expect, it } from "vitest";

import { parseRefreshMarylandCampaignFinanceRawDataScriptArgs as parseArgs } from "../../src/scripts/refreshMarylandCampaignFinanceRawData.js";

describe("refreshMarylandCampaignFinanceRawData script", () => {
  it("parses CFS raw-data refresh options", () => {
    expect(
      parseArgs([
        "--filing-year=2026",
        "--artifact-kind=expenditures",
        "--url=https://example.test/api/export",
        "--cache-dir=/cache",
        "--timeout-ms=5000",
        "--force",
      ])
    ).toEqual({
      filingYear: 2026,
      artifactKind: "expenditures",
      url: "https://example.test/api/export",
      cacheDir: "/cache",
      timeoutMs: 5000,
      force: true,
    });
  });

  it("defaults to contribution artifacts for the official public export endpoint", () => {
    expect(parseArgs(["--year=2026"])).toMatchObject({
      filingYear: 2026,
      artifactKind: "contributions",
      url: "https://api-campaignfinance.maryland.gov/api/ExportPublicData/GetExportPublicDownloadData",
    });
  });

  it("parses Maryland artifact aliases", () => {
    expect(parseArgs(["--year=2026", "--artifact-kind=TEXP"])).toMatchObject({
      artifactKind: "expenditures",
    });
    expect(parseArgs(["--year=2026", "--artifact-kind=TCMD"])).toMatchObject({
      artifactKind: "committees",
    });
  });

  it("rejects malformed values", () => {
    expect(() => parseArgs(["--year=2026x"])).toThrow("Invalid --filing-year value: 2026x");
    expect(() => parseArgs(["--timeout-ms=5x"])).toThrow("Invalid --timeout-ms value: 5x");
    expect(() => parseArgs(["--artifact-kind=receipts"])).toThrow("Invalid Maryland CFS artifact kind");
    expect(() => parseArgs(["--url=http://example.test/export"])).toThrow("Only https is allowed");
  });

  it("rejects missing or duplicate values", () => {
    expect(() => parseArgs(["--url"])).toThrow("Missing --url value");
    expect(() => parseArgs(["--year=2026", "--filing-year=2025"])).toThrow("Provide --filing-year at most once");
  });
});
