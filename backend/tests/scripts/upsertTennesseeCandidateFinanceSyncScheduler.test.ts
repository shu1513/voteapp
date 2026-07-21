import { describe, expect, it } from "vitest";

import { parseUpsertTennesseeCandidateFinanceSyncSchedulerArgs } from "../../src/scripts/upsertTennesseeCandidateFinanceSyncScheduler.js";

describe("upsertTennesseeCandidateFinanceSyncScheduler script", () => {
  it("rejects unknown flags before upserting the recurring scheduler", () => {
    expect(() => parseUpsertTennesseeCandidateFinanceSyncSchedulerArgs(["--max-candidate=2"])).toThrow(
      "Unknown option: --max-candidate"
    );
  });

  it("parses supported scheduler options", () => {
    expect(parseUpsertTennesseeCandidateFinanceSyncSchedulerArgs(["--force", "--ai-min-amount=25000"])).toMatchObject({
      force: true,
    });
  });
});
