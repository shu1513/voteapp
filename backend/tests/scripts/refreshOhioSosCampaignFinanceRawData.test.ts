import { resolve } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  parseRefreshOhioSosCampaignFinanceRawDataScriptArgs as parseArgs,
  selectOhioSosDownloadSkips,
} from "../../src/scripts/refreshOhioSosCampaignFinanceRawData.js";
import type { OhioSosDownloadPlanEntry } from "../../src/pipeline/ohioFinance/ohioSosArtifactAcquisition.js";
import type {
  OhioSosArtifactCacheStatus,
  OhioSosArtifactManifest,
} from "../../src/pipeline/ohioFinance/ohioSosArtifactCache.js";

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

  it("accepts space-separated flag values", () => {
    expect(parseArgs(["--cycle-year", "2026", "--cache-dir", "/cache/ohio"]).cacheDir).toBe("/cache/ohio");
  });

  // A silently ignored token could turn an intended dry run into a real
  // paced pull against the rate-limited portal.
  it("rejects unknown options and stray tokens", () => {
    expect(() => parseArgs(["--dryrun"])).toThrow(/Unknown option: --dryrun/);
    expect(() => parseArgs(["--cycle-year=2026", "extra"])).toThrow(/Unknown option: extra/);
  });

  it("rejects value-bearing boolean flags", () => {
    expect(() => parseArgs(["--dry-run=true"])).toThrow(/--dry-run does not take a value/);
    expect(() => parseArgs(["--force=1"])).toThrow(/--force does not take a value/);
  });

  it("rejects both cycle-year aliases together", () => {
    expect(() => parseArgs(["--cycle-year=2026", "--year=2026"])).toThrow(/not both/);
  });
});

describe("selectOhioSosDownloadSkips", () => {
  function entry(fileName: string, dateModified: string | null): OhioSosDownloadPlanEntry {
    return { productKey: "candidate_list", transactionYear: undefined, fileName, downloadId: "1", dateModified };
  }

  function status(
    fileName: string,
    state: OhioSosArtifactCacheStatus["status"],
    portalDateModified: string | null
  ): OhioSosArtifactCacheStatus {
    return {
      productKey: "candidate_list",
      transactionYear: null,
      fileName,
      filePath: `/cache/${fileName}`,
      status: state,
      manifest:
        state === "missing" ? null : ({ portalDateModified } as OhioSosArtifactManifest),
    };
  }

  function statuses(...entries: OhioSosArtifactCacheStatus[]): Map<string, OhioSosArtifactCacheStatus> {
    return new Map(entries.map((s) => [s.fileName, s]));
  }

  it("skips a ready artifact whose portal date matches the manifest", () => {
    const skip = selectOhioSosDownloadSkips({
      force: false,
      planEntries: [entry("A.CSV", "08/04/2026 10:30 AM")],
      statusByFileName: statuses(status("A.CSV", "ready", "08/04/2026 10:30 AM")),
    });
    expect(skip).toEqual(new Set(["A.CSV"]));
  });

  // The refresh must notice a newer portal file — a cache that only checks
  // its own integrity would never pull an update.
  it("re-downloads when the portal date changed or is unknown", () => {
    const skip = selectOhioSosDownloadSkips({
      force: false,
      planEntries: [
        entry("CHANGED.CSV", "08/05/2026 09:00 AM"),
        entry("NO_LISTING_DATE.CSV", null),
        entry("NO_MANIFEST_DATE.CSV", "08/04/2026 10:30 AM"),
      ],
      statusByFileName: statuses(
        status("CHANGED.CSV", "ready", "08/04/2026 10:30 AM"),
        status("NO_LISTING_DATE.CSV", "ready", "08/04/2026 10:30 AM"),
        status("NO_MANIFEST_DATE.CSV", "ready", null)
      ),
    });
    expect(skip.size).toBe(0);
  });

  it("never skips stale or missing artifacts", () => {
    const skip = selectOhioSosDownloadSkips({
      force: false,
      planEntries: [entry("S.CSV", "08/04/2026 10:30 AM"), entry("M.CSV", "08/04/2026 10:30 AM")],
      statusByFileName: statuses(
        status("S.CSV", "stale", "08/04/2026 10:30 AM"),
        status("M.CSV", "missing", null)
      ),
    });
    expect(skip.size).toBe(0);
  });

  it("skips nothing under --force", () => {
    const skip = selectOhioSosDownloadSkips({
      force: true,
      planEntries: [entry("A.CSV", "08/04/2026 10:30 AM")],
      statusByFileName: statuses(status("A.CSV", "ready", "08/04/2026 10:30 AM")),
    });
    expect(skip.size).toBe(0);
  });
});
