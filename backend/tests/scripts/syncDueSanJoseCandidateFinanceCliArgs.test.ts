import { describe, expect, it } from "vitest";
import { parseSyncDueSanJoseCandidateFinanceScriptArgs } from "../../src/scripts/syncDueSanJoseCandidateFinance.js";

describe("parseSyncDueSanJoseCandidateFinanceScriptArgs", () => {
  it("parses the known flags", () => {
    expect(
      parseSyncDueSanJoseCandidateFinanceScriptArgs([
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
      parseSyncDueSanJoseCandidateFinanceScriptArgs(["--dryrun"]),
    ).toThrow(/Unknown San José candidate finance flag/);
  });

  it("fails loudly on a repeated value flag instead of silently taking the first", () => {
    expect(() =>
      parseSyncDueSanJoseCandidateFinanceScriptArgs([
        "--max-candidates",
        "5",
        "--max-candidates",
        "50",
      ]),
    ).toThrow(/--max-candidates was passed more than once/);
  });

  it("rejects a non-integer value", () => {
    expect(() =>
      parseSyncDueSanJoseCandidateFinanceScriptArgs(["--stale-after-days", "x"]),
    ).toThrow(/requires a positive integer/);
  });
});
