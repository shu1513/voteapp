import { describe, expect, it } from "vitest";

import { parseUpsertNewMexicoCfisRawDataRefreshSchedulerArgs } from "../../src/scripts/upsertNewMexicoCfisRawDataRefreshScheduler.js";

describe("upsertNewMexicoCfisRawDataRefreshScheduler script", () => {
  it("parses recurring scheduler options", () => {
    expect(
      parseUpsertNewMexicoCfisRawDataRefreshSchedulerArgs([
        "--force",
        "--year=2026",
        "--artifact-kind=contributions",
        "--cache-dir=/tmp/cfis",
        "--timeout-ms=5000",
      ])
    ).toEqual({
      force: true,
      year: 2026,
      artifactKind: "contributions",
      url: undefined,
      cacheDir: "/tmp/cfis",
      timeoutMs: 5000,
    });
  });

  it("rejects malformed flags", () => {
    expect(() => parseUpsertNewMexicoCfisRawDataRefreshSchedulerArgs(["--year=2026x"])).toThrow(
      "Invalid --year value"
    );
    expect(() => parseUpsertNewMexicoCfisRawDataRefreshSchedulerArgs(["--artifact-kind=bad"])).toThrow(
      "Invalid --artifact-kind value"
    );
  });

  it("rejects a value flag given more than once instead of taking the first", () => {
    expect(() => parseUpsertNewMexicoCfisRawDataRefreshSchedulerArgs(["--timeout-ms=10", "--timeout-ms", "20"])).toThrow(
      "Provide --timeout-ms at most once"
    );
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() => parseUpsertNewMexicoCfisRawDataRefreshSchedulerArgs(["--timeout-ms=9007199254740993"])).toThrow(
      "Invalid --timeout-ms value: 9007199254740993"
    );
  });
});
