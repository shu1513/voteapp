import { describe, expect, it } from "vitest";

import { parseUpsertMichiganMitnRawDataRefreshSchedulerArgs } from "../../src/scripts/upsertMichiganMitnRawDataRefreshScheduler.js";

describe("upsertMichiganMitnRawDataRefreshScheduler script", () => {
  it("parses recurring scheduler options", () => {
    expect(
      parseUpsertMichiganMitnRawDataRefreshSchedulerArgs([
        "--year=2022",
        "--force",
        "--url=https://example.test/2022_mi_cfr.7z",
        "--cache-dir=/tmp/michigan-cache",
        "--timeout-ms=5000",
      ])
    ).toEqual({
      year: 2022,
      force: true,
      url: "https://example.test/2022_mi_cfr.7z",
      cacheDir: "/tmp/michigan-cache",
      timeoutMs: 5000,
    });
  });

  it("rejects malformed flags", () => {
    expect(() => parseUpsertMichiganMitnRawDataRefreshSchedulerArgs(["--year=2022x"])).toThrow(
      "Invalid --year value"
    );
    expect(() => parseUpsertMichiganMitnRawDataRefreshSchedulerArgs(["--timeout-ms=5x"])).toThrow(
      "Invalid --timeout-ms value"
    );
    expect(() => parseUpsertMichiganMitnRawDataRefreshSchedulerArgs(["--url="])).toThrow("Missing --url value");
    expect(() => parseUpsertMichiganMitnRawDataRefreshSchedulerArgs(["--cache-dir", "   "])).toThrow(
      "Missing --cache-dir value"
    );
  });
});
