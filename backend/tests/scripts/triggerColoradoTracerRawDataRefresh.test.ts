import { describe, expect, it } from "vitest";

import { parseColoradoTracerRawDataRefreshTriggerArgs } from "../../src/scripts/triggerColoradoTracerRawDataRefresh.js";

describe("triggerColoradoTracerRawDataRefresh script", () => {
  it("parses raw-data refresh trigger options", () => {
    expect(
      parseColoradoTracerRawDataRefreshTriggerArgs([
        "--year=2026",
        "--url=https://example.test/2026.zip",
        "--cache-dir=/cache",
        "--timeout-ms=5000",
        "--force",
      ])
    ).toEqual({
      year: 2026,
      url: "https://example.test/2026.zip",
      cacheDir: "/cache",
      timeoutMs: 5000,
      force: true,
    });
  });

  it("rejects malformed values", () => {
    expect(() => parseColoradoTracerRawDataRefreshTriggerArgs(["--year=2026x"])).toThrow(
      "Invalid --year value: 2026x"
    );
    expect(() => parseColoradoTracerRawDataRefreshTriggerArgs(["--timeout-ms=5x"])).toThrow(
      "Invalid --timeout-ms value: 5x"
    );
    expect(() => parseColoradoTracerRawDataRefreshTriggerArgs(["--url"])).toThrow("Missing --url value");
  });
});
