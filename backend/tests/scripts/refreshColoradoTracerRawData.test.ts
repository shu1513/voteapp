import { describe, expect, it } from "vitest";

import { parseRefreshColoradoTracerRawDataScriptArgs } from "../../src/scripts/refreshColoradoTracerRawData.js";

describe("refreshColoradoTracerRawData script", () => {
  it("parses raw-data refresh options", () => {
    expect(
      parseRefreshColoradoTracerRawDataScriptArgs([
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

  it("uses a bulk-download-safe default timeout", () => {
    expect(parseRefreshColoradoTracerRawDataScriptArgs(["--year=2026"])).toMatchObject({
      year: 2026,
      timeoutMs: 900_000,
    });
  });

  it("rejects malformed values", () => {
    expect(() => parseRefreshColoradoTracerRawDataScriptArgs(["--year=2026x"])).toThrow(
      "Invalid --year value: 2026x"
    );
    expect(() => parseRefreshColoradoTracerRawDataScriptArgs(["--timeout-ms=5x"])).toThrow(
      "Invalid --timeout-ms value: 5x"
    );
    expect(() => parseRefreshColoradoTracerRawDataScriptArgs(["--url=http://example.test/2026.zip"])).toThrow(
      "Only https is allowed"
    );
  });

  it("rejects an empty inline value instead of reading it as an empty string", () => {
    expect(() => parseRefreshColoradoTracerRawDataScriptArgs(["--url="])).toThrow("Missing --url value");
  });
});
