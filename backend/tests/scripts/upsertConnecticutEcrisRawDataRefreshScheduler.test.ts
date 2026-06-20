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
});
