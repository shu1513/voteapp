import { describe, expect, it } from "vitest";

import { parseTexasTecRawDataRefreshTriggerArgs } from "../../src/scripts/triggerTexasTecRawDataRefresh.js";

describe("triggerTexasTecRawDataRefresh script", () => {
  it("parses manual trigger options", () => {
    expect(
      parseTexasTecRawDataRefreshTriggerArgs([
        "--force",
        "--url=https://example.test/TEC_CF_CSV.zip",
        "--cache-dir=/tmp/tx-tec",
        "--timeout-ms=5000",
      ])
    ).toEqual({
      force: true,
      url: "https://example.test/TEC_CF_CSV.zip",
      cacheDir: "/tmp/tx-tec",
      timeoutMs: 5000,
    });
  });

  it("rejects malformed flags", () => {
    expect(() => parseTexasTecRawDataRefreshTriggerArgs(["--timeout-ms=5x"])).toThrow(
      "Invalid --timeout-ms value"
    );
    expect(() => parseTexasTecRawDataRefreshTriggerArgs(["--url", "   "])).toThrow("Missing --url value");
  });

  it("rejects a value flag given more than once instead of taking the first", () => {
    expect(() => parseTexasTecRawDataRefreshTriggerArgs(["--timeout-ms=10", "--timeout-ms", "20"])).toThrow(
      "Provide --timeout-ms at most once"
    );
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() => parseTexasTecRawDataRefreshTriggerArgs(["--timeout-ms=9007199254740993"])).toThrow(
      "Invalid --timeout-ms value: 9007199254740993"
    );
  });
});
