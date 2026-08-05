import { resolve } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { parseRefreshOhioSosCampaignFinanceRawDataScriptArgs as parseArgs } from "../../src/scripts/refreshOhioSosCampaignFinanceRawData.js";

const originalCacheDirEnv = process.env.OHIO_SOS_RAW_DATA_CACHE_DIR;

afterEach(() => {
  if (originalCacheDirEnv === undefined) {
    delete process.env.OHIO_SOS_RAW_DATA_CACHE_DIR;
  } else {
    process.env.OHIO_SOS_RAW_DATA_CACHE_DIR = originalCacheDirEnv;
  }
});

describe("refreshOhioSosCampaignFinanceRawData script", () => {
  it("parses every raw-data refresh option", () => {
    expect(
      parseArgs([
        "--cycle-year=2026",
        "--cache-dir=/cache/ohio",
        "--chrome-debug-url=http://127.0.0.1:9333",
        "--spacing-ms=12000",
        "--download-timeout-ms=600000",
        "--force",
        "--skip-31u-details",
        "--dry-run",
      ])
    ).toEqual({
      cycleYear: 2026,
      cacheDir: "/cache/ohio",
      chromeDebugUrl: "http://127.0.0.1:9333",
      spacingMs: 12_000,
      downloadTimeoutMs: 600_000,
      force: true,
      skipDetails: true,
      dryRun: true,
    });
  });

  it("defaults to the spike-proven 8s spacing and the standard cache dir", () => {
    delete process.env.OHIO_SOS_RAW_DATA_CACHE_DIR;
    expect(parseArgs(["--year=2026"])).toEqual({
      cycleYear: 2026,
      cacheDir: resolve("scratch/ohio-campaign-finance/sos"),
      chromeDebugUrl: "http://127.0.0.1:9222",
      spacingMs: 8_000,
      downloadTimeoutMs: 300_000,
      force: false,
      skipDetails: false,
      dryRun: false,
    });
  });

  it("takes the cache directory from the environment when no flag is given", () => {
    process.env.OHIO_SOS_RAW_DATA_CACHE_DIR = "/env/ohio-cache";
    expect(parseArgs(["--cycle-year=2026"]).cacheDir).toBe("/env/ohio-cache");
  });

  it("defaults the cycle to the current year", () => {
    delete process.env.OHIO_SOS_RAW_DATA_CACHE_DIR;
    expect(parseArgs([]).cycleYear).toBe(new Date().getUTCFullYear());
  });

  it("rejects a repeated, malformed, or implausible cycle year", () => {
    expect(() => parseArgs(["--cycle-year=2026", "--cycle-year=2025"])).toThrow(/at most once/);
    expect(() => parseArgs(["--cycle-year=twenty"])).toThrow(/Invalid --cycle-year value/);
    expect(() => parseArgs(["--cycle-year=1899"])).toThrow(/Invalid Ohio SoS transaction year/);
  });

  it("rejects a flag given without a value", () => {
    expect(() => parseArgs(["--cache-dir"])).toThrow(/Missing value for --cache-dir/);
    expect(() => parseArgs(["--spacing-ms="])).toThrow(/Missing value for --spacing-ms/);
  });
});
