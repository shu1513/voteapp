import { describe, expect, it } from "vitest";

import { parseCaliforniaCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerCaliforniaCandidateFinanceSync.js";

describe("triggerCaliforniaCandidateFinanceSync script", () => {
  it("parses batch trigger flags", () => {
    expect(
      parseCaliforniaCandidateFinanceSyncTriggerArgs([
        "--force",
        "--skip-outside",
        "--max-candidates=5",
        "--lookback-days",
        "2",
        "--timeout-ms=5000",
      ])
    ).toEqual({
      dryRun: false,
      force: true,
      includeOutside: false,
      maxCandidates: 5,
      staleAfterDays: undefined,
      electionLookbackDays: 2,
      electionLookaheadDays: undefined,
      timeoutMs: 5000,
    });
  });

  it("defaults to outside spending included", () => {
    expect(parseCaliforniaCandidateFinanceSyncTriggerArgs(["--dry-run"])).toMatchObject({
      dryRun: true,
      force: false,
      includeOutside: true,
    });
  });

  it("rejects malformed numeric flags strictly", () => {
    expect(() => parseCaliforniaCandidateFinanceSyncTriggerArgs(["--max-candidates=5x"])).toThrow(
      "Invalid --max-candidates value"
    );
    expect(() => parseCaliforniaCandidateFinanceSyncTriggerArgs(["--timeout-ms=0"])).toThrow(
      "Invalid --timeout-ms value"
    );
  });
});
