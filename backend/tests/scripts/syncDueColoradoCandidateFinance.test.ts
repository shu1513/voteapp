import { describe, expect, it } from "vitest";

import { toSyncDueColoradoCandidateFinanceScriptOutput } from "../../src/scripts/syncDueColoradoCandidateFinance.js";

describe("syncDueColoradoCandidateFinance script", () => {
  it("includes force in script output for audit logs", () => {
    const output = toSyncDueColoradoCandidateFinanceScriptOutput({
      startedAt: new Date("2026-01-02T03:04:05.000Z"),
      options: {
        dryRun: true,
        force: true,
        maxCandidates: 2,
      },
      result: {
        dryRun: true,
        now: "2026-01-02T03:04:05.000Z",
        staleAfterDays: 7,
        maxCandidates: 2,
        dueCandidateCount: 3,
        selectedCandidateCount: 2,
        syncedCandidateCount: 1,
        failedCandidateCount: 1,
        results: [],
      },
    });

    expect(output).toMatchObject({
      type: "colorado_candidate_finance_due_sync",
      started_at: "2026-01-02T03:04:05.000Z",
      dry_run: true,
      force: true,
    });
    expect(typeof output.ts).toBe("string");
  });
});
