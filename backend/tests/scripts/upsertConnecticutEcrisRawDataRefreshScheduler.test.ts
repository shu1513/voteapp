import { describe, expect, it } from "vitest";

import { parseUpsertConnecticutEcrisRawDataRefreshSchedulerArgs } from "../../src/scripts/upsertConnecticutEcrisRawDataRefreshScheduler.js";

describe("upsertConnecticutEcrisRawDataRefreshScheduler script", () => {
  it("parses Connecticut eCRIS raw refresh scheduler options", () => {
    expect(
      parseUpsertConnecticutEcrisRawDataRefreshSchedulerArgs([
        "--force",
        "--year=2026",
        "--transaction-type=receipts",
        "--committee-type=candidate_exploratory",
        "--period=election",
        "--format=csv",
        "--url=https://example.test/ct.csv",
        "--cache-dir=/cache",
        "--timeout-ms=5000",
      ])
    ).toEqual({
      force: true,
      year: 2026,
      transactionType: "receipts",
      committeeType: "candidate_exploratory",
      period: "election",
      format: "csv",
      url: "https://example.test/ct.csv",
      cacheDir: "/cache",
      timeoutMs: 5000,
    });
  });

  it("rejects malformed numeric and missing values", () => {
    expect(() => parseUpsertConnecticutEcrisRawDataRefreshSchedulerArgs(["--year=2026x"])).toThrow(
      "Invalid --year value: 2026x"
    );
    expect(() => parseUpsertConnecticutEcrisRawDataRefreshSchedulerArgs(["--timeout-ms=5x"])).toThrow(
      "Invalid --timeout-ms value: 5x"
    );
    expect(() => parseUpsertConnecticutEcrisRawDataRefreshSchedulerArgs(["--cache-dir"])).toThrow(
      "Missing --cache-dir value"
    );
  });

  it("rejects a value flag given more than once instead of taking the first", () => {
    expect(() => parseUpsertConnecticutEcrisRawDataRefreshSchedulerArgs(["--timeout-ms=10", "--timeout-ms", "20"])).toThrow(
      "Provide --timeout-ms at most once"
    );
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() => parseUpsertConnecticutEcrisRawDataRefreshSchedulerArgs(["--timeout-ms=9007199254740993"])).toThrow(
      "Invalid --timeout-ms value: 9007199254740993"
    );
  });
});
