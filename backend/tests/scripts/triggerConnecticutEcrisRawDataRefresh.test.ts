import { describe, expect, it } from "vitest";

import { parseConnecticutEcrisRawDataRefreshTriggerArgs } from "../../src/scripts/triggerConnecticutEcrisRawDataRefresh.js";

describe("triggerConnecticutEcrisRawDataRefresh script", () => {
  it("parses Connecticut eCRIS raw refresh trigger options", () => {
    expect(
      parseConnecticutEcrisRawDataRefreshTriggerArgs([
        "--force",
        "--year=2026",
        "--url=https://example.test/ct.csv",
        "--cache-dir=/cache",
        "--timeout-ms=5000",
      ])
    ).toEqual({
      force: true,
      year: 2026,
      transactionType: undefined,
      committeeType: undefined,
      period: undefined,
      format: undefined,
      url: "https://example.test/ct.csv",
      cacheDir: "/cache",
      timeoutMs: 5000,
    });
  });

  it("rejects malformed numeric and missing values", () => {
    expect(() => parseConnecticutEcrisRawDataRefreshTriggerArgs(["--year=2026x"])).toThrow(
      "Invalid --year value: 2026x"
    );
    expect(() => parseConnecticutEcrisRawDataRefreshTriggerArgs(["--timeout-ms=5x"])).toThrow(
      "Invalid --timeout-ms value: 5x"
    );
    expect(() => parseConnecticutEcrisRawDataRefreshTriggerArgs(["--url="])).toThrow("Missing --url value");
  });
});
