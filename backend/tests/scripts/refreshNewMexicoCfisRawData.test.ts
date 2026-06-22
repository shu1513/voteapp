import { describe, expect, it } from "vitest";

import { parseRefreshNewMexicoCfisRawDataScriptArgs as parseArgs } from "../../src/scripts/refreshNewMexicoCfisRawData.js";

describe("refreshNewMexicoCfisRawData script", () => {
  it("parses CFIS raw-data refresh options", () => {
    expect(
      parseArgs([
        "--year=2026",
        "--artifact-kind=expenditures",
        "--url=https://example.test/EXP_2026.csv",
        "--cache-dir=/cache",
        "--timeout-ms=5000",
        "--force",
      ])
    ).toEqual({
      year: 2026,
      artifactKind: "expenditures",
      url: "https://example.test/EXP_2026.csv",
      cacheDir: "/cache",
      timeoutMs: 5000,
      force: true,
    });
  });

  it("defaults to contribution artifact", () => {
    expect(parseArgs(["--year=2026"])).toMatchObject({
      year: 2026,
      artifactKind: "contributions",
      url: "https://login.cfis.sos.state.nm.us/api/DataDownload/GetCSVDownloadReport?year=2026&transactionType=CON&reportFormat=csv&fileName=CON_2026.csv",
    });
  });

  it("builds default expenditure artifact URLs", () => {
    expect(parseArgs(["--year=2026", "--artifact-kind=expenditures"])).toMatchObject({
      year: 2026,
      artifactKind: "expenditures",
      url: "https://login.cfis.sos.state.nm.us/api/DataDownload/GetCSVDownloadReport?year=2026&transactionType=EXP&reportFormat=csv&fileName=EXP_2026.csv",
    });
  });

  it("rejects malformed values", () => {
    expect(() => parseArgs(["--year=2026x"])).toThrow("Invalid --year value: 2026x");
    expect(() => parseArgs(["--timeout-ms=5x"])).toThrow("Invalid --timeout-ms value: 5x");
    expect(() => parseArgs(["--artifact-kind=receipts"])).toThrow("Invalid --artifact-kind value");
    expect(() => parseArgs(["--url=http://example.test/CON_2026.csv"])).toThrow("Only https is allowed");
  });

  it("rejects missing or duplicate values", () => {
    expect(() => parseArgs(["--url"])).toThrow("Missing value for --url");
    expect(() => parseArgs(["--year=2026", "--year=2025"])).toThrow("Provide --year at most once");
  });
});
