import { describe, expect, it } from "vitest";

import { parseRefreshNebraskaNadcRawDataScriptArgs as parseArgs } from "../../src/scripts/refreshNebraskaNadcRawData.js";

describe("refreshNebraskaNadcRawData script", () => {
  it("parses NADC raw-data refresh options", () => {
    expect(
      parseArgs([
        "--year=2026",
        "--artifact-kind=expenditure",
        "--url=https://example.test/2026.zip",
        "--cache-dir=/cache",
        "--timeout-ms=5000",
        "--force",
      ])
    ).toEqual({
      year: 2026,
      artifactKind: "expenditure",
      url: "https://example.test/2026.zip",
      cacheDir: "/cache",
      timeoutMs: 5000,
      force: true,
    });
  });

  it("defaults to contribution loan artifact", () => {
    expect(parseArgs(["--year=2026"])).toMatchObject({
      year: 2026,
      artifactKind: "contribution_loan",
      url: "https://nadc-e.nebraska.gov/PublicSite/Docs/BulkDataDownloads/2026_ContributionLoanExtract.csv.zip",
    });
  });

  it("rejects malformed values", () => {
    expect(() => parseArgs(["--year=2026x"])).toThrow("Invalid --year value: 2026x");
    expect(() => parseArgs(["--timeout-ms=5x"])).toThrow("Invalid --timeout-ms value: 5x");
    expect(() => parseArgs(["--artifact-kind=receipts"])).toThrow("Invalid --artifact-kind value");
    expect(() => parseArgs(["--url=http://example.test/2026.zip"])).toThrow("Only https is allowed");
  });

  it("rejects missing or duplicate values", () => {
    expect(() => parseArgs(["--url"])).toThrow("Missing value for --url");
    expect(() => parseArgs(["--year=2026", "--year=2025"])).toThrow("Provide --year at most once");
  });
});
