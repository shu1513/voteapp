import { describe, expect, it } from "vitest";

import { parseRefreshConnecticutEcrisRawDataScriptArgs } from "../../src/scripts/refreshConnecticutEcrisRawData.js";

describe("refreshConnecticutEcrisRawData script", () => {
  it("parses default candidate receipt CSV refresh options", () => {
    expect(parseRefreshConnecticutEcrisRawDataScriptArgs(["--year=2026"])).toMatchObject({
      year: 2026,
      transactionType: "receipts",
      committeeType: "candidate_exploratory",
      period: "election",
      format: "csv",
      url: "https://seec.ct.gov/ecrisreporting/Data/eCrisDownloads/exportdatafiles/Receipts2026ElectionYearCandidateExploratoryCommittees.csv",
      force: false,
      timeoutMs: 30_000,
    });
  });

  it("parses explicit artifact refresh options", () => {
    expect(
      parseRefreshConnecticutEcrisRawDataScriptArgs([
        "--year=2024",
        "--transaction-type=disbursements",
        "--committee-type=party_pac",
        "--period=calendar",
        "--format=xlsx",
        "--url=https://example.test/ct-disbursements.xlsx",
        "--cache-dir=/cache",
        "--timeout-ms=5000",
        "--force",
      ])
    ).toEqual({
      year: 2024,
      transactionType: "disbursements",
      committeeType: "party_pac",
      period: "calendar",
      format: "xlsx",
      url: "https://example.test/ct-disbursements.xlsx",
      cacheDir: "/cache",
      timeoutMs: 5000,
      force: true,
    });
  });

  it("rejects malformed values", () => {
    expect(() => parseRefreshConnecticutEcrisRawDataScriptArgs(["--year=2026x"])).toThrow(
      "Invalid --year value: 2026x"
    );
    expect(() => parseRefreshConnecticutEcrisRawDataScriptArgs(["--year="])).toThrow("Missing --year value");
    expect(() => parseRefreshConnecticutEcrisRawDataScriptArgs(["--url", "   "])).toThrow("Missing --url value");
    expect(() => parseRefreshConnecticutEcrisRawDataScriptArgs(["--timeout-ms=5x"])).toThrow(
      "Invalid --timeout-ms value: 5x"
    );
    expect(() => parseRefreshConnecticutEcrisRawDataScriptArgs(["--url=http://example.test/ct.csv"])).toThrow(
      "Only https is allowed"
    );
    expect(() => parseRefreshConnecticutEcrisRawDataScriptArgs(["--transaction-type=unknown"])).toThrow(
      "Invalid --transaction-type value: unknown"
    );
    expect(() =>
      parseRefreshConnecticutEcrisRawDataScriptArgs([
        "--year=2026",
        "--committee-type=candidate_exploratory",
        "--period=calendar",
      ])
    ).toThrow("candidate/exploratory artifacts use election-year files");
  });
});
