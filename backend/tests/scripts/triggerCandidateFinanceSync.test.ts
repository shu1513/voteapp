import { describe, expect, it } from "vitest";

import { parseCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerCandidateFinanceSync.js";

describe("triggerCandidateFinanceSync script", () => {
  it("parses batch trigger flags", () => {
    expect(
      parseCandidateFinanceSyncTriggerArgs([
        "--force",
        "--include-outside",
        "--max-candidates=5",
        "--lookback-days",
        "2",
        "--timeout-ms=5000",
      ])
    ).toEqual({
      dryRun: false,
      force: true,
      includeOutside: true,
      candidateId: undefined,
      fecCandidateId: undefined,
      electionYear: undefined,
      maxCandidates: 5,
      staleAfterDays: undefined,
      electionLookbackDays: 2,
      electionLookaheadDays: undefined,
      perPage: undefined,
      outsideGroupLimit: undefined,
      timeoutMs: 5000,
      aiClassifyIndustries: true,
      aiClassificationMinAmount: undefined,
    });
  });

  it("parses targeted trigger flags", () => {
    expect(
      parseCandidateFinanceSyncTriggerArgs([
        "--candidate-id=candidate-1",
        "--fec-id",
        "S80000001",
        "--year=2026",
        "--dry-run",
      ])
    ).toMatchObject({
      dryRun: true,
      candidateId: "candidate-1",
      fecCandidateId: "S80000001",
      electionYear: 2026,
    });
  });

  it("can opt out of AI industry classification", () => {
    expect(parseCandidateFinanceSyncTriggerArgs(["--no-ai-classify-industries"])).toMatchObject({
      aiClassifyIndustries: false,
    });
  });

  it("rejects malformed numeric flags strictly", () => {
    expect(() => parseCandidateFinanceSyncTriggerArgs(["--year=2026x"])).toThrow("Invalid --year value");
    expect(() => parseCandidateFinanceSyncTriggerArgs(["--timeout-ms=0"])).toThrow("Invalid --timeout-ms value");
  });
});
