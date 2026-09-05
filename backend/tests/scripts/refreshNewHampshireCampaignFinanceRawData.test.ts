import { describe, expect, it } from "vitest";

import { parseRefreshNewHampshireCampaignFinanceRawDataScriptArgs as parseArgs } from "../../src/scripts/refreshNewHampshireCampaignFinanceRawData.js";

describe("refreshNewHampshireCampaignFinanceRawData script", () => {
  it("parses the narrow raw-refresh contract and aliases", () => {
    expect(
      parseArgs([
        "--filing-year=2026",
        "--artifact-kind=TEXP",
        "--cache-dir=/cache",
        "--timeout-ms=5000",
        "--force",
      ])
    ).toEqual({
      filingYear: 2026,
      artifactKind: "expenditures",
      cacheDir: "/cache",
      timeoutMs: 5000,
      force: true,
    });
    expect(parseArgs(["--year=2024", "--artifact-kind=receipts"])).toMatchObject({
      filingYear: 2024,
      artifactKind: "contributions",
    });
  });

  it("rejects malformed, duplicate, and unknown arguments", () => {
    expect(() => parseArgs(["--year=2026x"])).toThrow("Invalid --filing-year value");
    expect(() => parseArgs(["--year=2015"])).toThrow("Invalid New Hampshire CFS filing year");
    expect(() => parseArgs(["--year=2026", "--filing-year=2024"])).toThrow(
      "Provide --filing-year at most once"
    );
    expect(() => parseArgs(["--artifact-kind=committees"])).toThrow(
      "Invalid New Hampshire CFS artifact kind"
    );
    expect(() => parseArgs(["--artifact-knd=expenditures"])).toThrow(
      "Unknown New Hampshire CFS raw data refresh flag"
    );
    expect(() => parseArgs(["--force=true"])).toThrow("Boolean flag does not accept a value");
  });

  it("rejects a flag given without a value instead of crashing on the missing token", () => {
    expect(() => parseArgs(["--cache-dir"])).toThrow("Missing --cache-dir value");
    expect(() => parseArgs(["--cache-dir", "--dry-run"])).toThrow("Missing --cache-dir value");
  });
});
