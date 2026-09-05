import { describe, expect, it } from "vitest";

import { parseRefreshAlabamaCampaignFinanceRawDataScriptArgs as parseArgs } from "../../src/scripts/refreshAlabamaCampaignFinanceRawData.js";

describe("refreshAlabamaCampaignFinanceRawData script", () => {
  it("parses the narrow raw-refresh contract", () => {
    expect(
      parseArgs([
        "--year=2024",
        "--year=2026",
        "--artifact-kind=Cash Contribution",
        "--cache-dir=/cache",
        "--timeout-ms=5000",
        "--force",
      ])
    ).toEqual({
      years: [2024, 2026],
      artifactKind: "cash",
      cacheDir: "/cache",
      timeoutMs: 5000,
      force: true,
      acceptEmpty: false,
    });
    expect(parseArgs(["--artifact-kind=expenditures"])).toMatchObject({
      years: [new Date().getUTCFullYear()],
      artifactKind: "expenditure",
      timeoutMs: undefined,
      force: false,
      acceptEmpty: false,
    });
  });

  it("keeps gate bypass and empty-extract acceptance separate", () => {
    expect(parseArgs(["--force"])).toMatchObject({ force: true, acceptEmpty: false });
    expect(parseArgs(["--accept-empty"])).toMatchObject({ force: false, acceptEmpty: true });
  });

  it("dedupes repeated years preserving order", () => {
    expect(parseArgs(["--year=2026", "--year=2024", "--year=2026"])).toMatchObject({
      years: [2026, 2024],
    });
  });

  it("rejects malformed and unknown arguments", () => {
    expect(() => parseArgs(["--year=26"])).toThrow("Invalid --year value");
    expect(() => parseArgs(["--year=2012"])).toThrow("Invalid Alabama FCPA extract year");
    expect(() => parseArgs(["--artifact-kind=committees"])).toThrow(
      "Invalid Alabama FCPA extract kind"
    );
    expect(() => parseArgs(["--timeout-ms=0"])).toThrow("Invalid --timeout-ms value");
    expect(() => parseArgs(["--yaer=2026"])).toThrow("Unknown Alabama FCPA raw data refresh flag");
    expect(() => parseArgs(["--force=true"])).toThrow("Boolean flag does not accept a value");
    expect(() => parseArgs(["--cache-dir=/a", "--cache-dir=/b"])).toThrow(
      "Provide --cache-dir at most once"
    );
  });

  it("rejects a flag given without a value instead of crashing on the missing token", () => {
    expect(() => parseArgs(["--cache-dir"])).toThrow("Missing --cache-dir value");
    expect(() => parseArgs(["--cache-dir", "--dry-run"])).toThrow("Missing --cache-dir value");
  });
});
