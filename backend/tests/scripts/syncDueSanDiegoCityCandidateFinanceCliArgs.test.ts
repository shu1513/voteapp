import { describe, expect, it } from "vitest";
import { parseSyncDueSanDiegoCityCandidateFinanceScriptArgs } from "../../src/scripts/syncDueSanDiegoCityCandidateFinance.js";

describe("parseSyncDueSanDiegoCityCandidateFinanceScriptArgs", () => {
  it("parses the known flags", () => {
    expect(
      parseSyncDueSanDiegoCityCandidateFinanceScriptArgs([
        "--dry-run",
        "--max-candidates",
        "5",
        "--election-id",
        "0b0b0b0b-0000-4000-8000-000000000001",
      ]),
    ).toMatchObject({
      dryRun: true,
      force: false,
      bypassAnomalyCheck: false,
      maxCandidates: 5,
      electionId: "0b0b0b0b-0000-4000-8000-000000000001",
    });
  });

  it("fails loudly on an unknown flag", () => {
    expect(() =>
      parseSyncDueSanDiegoCityCandidateFinanceScriptArgs(["--dryrun"]),
    ).toThrow(/Unknown San Diego candidate finance flag/);
  });

  it("fails loudly on a repeated value flag instead of silently taking the first", () => {
    expect(() =>
      parseSyncDueSanDiegoCityCandidateFinanceScriptArgs([
        "--max-candidates",
        "5",
        "--max-candidates",
        "50",
      ]),
    ).toThrow(/--max-candidates was passed more than once/);
  });

  it("rejects a non-integer value", () => {
    expect(() =>
      parseSyncDueSanDiegoCityCandidateFinanceScriptArgs(["--stale-after-days", "x"]),
    ).toThrow(/requires a positive integer/);
  });
});
