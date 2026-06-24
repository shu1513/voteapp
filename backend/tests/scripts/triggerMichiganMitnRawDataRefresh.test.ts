import { describe, expect, it } from "vitest";

import { parseMichiganMitnRawDataRefreshTriggerArgs } from "../../src/scripts/triggerMichiganMitnRawDataRefresh.js";

describe("triggerMichiganMitnRawDataRefresh script", () => {
  it("parses manual trigger options", () => {
    expect(
      parseMichiganMitnRawDataRefreshTriggerArgs([
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
    expect(() => parseMichiganMitnRawDataRefreshTriggerArgs(["--year=2022x"])).toThrow("Invalid --year value");
    expect(() => parseMichiganMitnRawDataRefreshTriggerArgs(["--timeout-ms=5x"])).toThrow(
      "Invalid --timeout-ms value"
    );
    expect(() => parseMichiganMitnRawDataRefreshTriggerArgs(["--url="])).toThrow("Missing --url value");
    expect(() => parseMichiganMitnRawDataRefreshTriggerArgs(["--cache-dir", "   "])).toThrow(
      "Missing --cache-dir value"
    );
  });
});
