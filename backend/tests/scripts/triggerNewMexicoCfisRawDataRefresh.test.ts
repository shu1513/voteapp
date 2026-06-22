import { describe, expect, it } from "vitest";

import { parseNewMexicoCfisRawDataRefreshTriggerArgs } from "../../src/scripts/triggerNewMexicoCfisRawDataRefresh.js";

describe("triggerNewMexicoCfisRawDataRefresh script", () => {
  it("parses manual trigger options", () => {
    expect(
      parseNewMexicoCfisRawDataRefreshTriggerArgs([
        "--force",
        "--year=2026",
        "--artifact-kind=expenditures",
        "--cache-dir=/tmp/cfis",
        "--timeout-ms=5000",
      ])
    ).toEqual({
      force: true,
      year: 2026,
      artifactKind: "expenditures",
      url: undefined,
      cacheDir: "/tmp/cfis",
      timeoutMs: 5000,
    });
  });

  it("rejects malformed flags", () => {
    expect(() => parseNewMexicoCfisRawDataRefreshTriggerArgs(["--year=2026x"])).toThrow("Invalid --year value");
    expect(() => parseNewMexicoCfisRawDataRefreshTriggerArgs(["--artifact-kind=loans"])).toThrow(
      "Invalid --artifact-kind value"
    );
    expect(() => parseNewMexicoCfisRawDataRefreshTriggerArgs(["--cache-dir", "   "])).toThrow(
      "Missing --cache-dir value"
    );
  });
});
