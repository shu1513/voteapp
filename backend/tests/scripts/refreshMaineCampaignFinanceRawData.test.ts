import { describe, expect, it } from "vitest";

import { parseRefreshMaineCampaignFinanceRawDataScriptArgs as parseArgs } from "../../src/scripts/refreshMaineCampaignFinanceRawData.js";

describe("refreshMaineCampaignFinanceRawData script", () => {
  it("parses CFIS raw-data refresh options", () => {
    expect(
      parseArgs([
        "--filing-year=2026",
        "--artifact-kind=expenditures",
        "--url=https://mainecampaignfinance.com/api/export",
        "--cache-dir=/cache",
        "--timeout-ms=5000",
        "--force",
      ])
    ).toEqual({
      filingYear: 2026,
      artifactKind: "expenditures",
      url: "https://mainecampaignfinance.com/api/export",
      cacheDir: "/cache",
      timeoutMs: 5000,
      force: true,
    });
  });

  it("defaults to contribution artifacts for the official CSV endpoint", () => {
    expect(parseArgs(["--year=2026"])).toMatchObject({
      filingYear: 2026,
      artifactKind: "contributions",
      url: "https://mainecampaignfinance.com/api/DataDownload/CSVDownloadReport",
    });
  });

  it("parses Maine artifact aliases", () => {
    expect(parseArgs(["--year=2026", "--artifact-kind=CON"])).toMatchObject({
      artifactKind: "contributions",
    });
    expect(parseArgs(["--year=2026", "--artifact-kind=EXP"])).toMatchObject({
      artifactKind: "expenditures",
    });
  });

  it("rejects malformed values", () => {
    expect(() => parseArgs(["--year=2026x"])).toThrow("Invalid --filing-year value: 2026x");
    expect(() => parseArgs(["--timeout-ms=5x"])).toThrow("Invalid --timeout-ms value: 5x");
    expect(() => parseArgs(["--artifact-kind=committees"])).toThrow("Invalid Maine CFIS artifact kind");
    expect(() => parseArgs(["--url=http://example.test/export"])).toThrow("Only https is allowed");
    expect(() => parseArgs(["--url=https://example.test/export"])).toThrow("Invalid --url host");
  });

  it("rejects missing or duplicate values", () => {
    expect(() => parseArgs(["--url"])).toThrow("Missing value for --url");
    expect(() => parseArgs(["--year=2026", "--filing-year=2025"])).toThrow("Provide --filing-year at most once");
  });

  it("rejects unknown flags before refreshing raw data", () => {
    expect(() => parseArgs(["--artifact-knd=expenditures"])).toThrow("Unknown Maine CFIS raw data refresh flag");
  });
});
