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
});
