import { describe, expect, it } from "vitest";

import { parseTennesseeCandidateFinanceSyncTriggerArgs } from "../../src/scripts/triggerTennesseeCandidateFinanceSync.js";

describe("triggerTennesseeCandidateFinanceSync script", () => {
  it("rejects unknown flags before enqueueing a job", () => {
    expect(() => parseTennesseeCandidateFinanceSyncTriggerArgs(["--dryrun"])).toThrow("Unknown option: --dryrun");
  });

  it("parses supported scheduler options", () => {
    expect(parseTennesseeCandidateFinanceSyncTriggerArgs(["--dry-run", "--max-candidates", "2"])).toMatchObject({
      dryRun: true,
      maxCandidates: 2,
    });
  });
});
