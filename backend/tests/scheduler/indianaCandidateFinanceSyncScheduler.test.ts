import { afterEach, describe, expect, it, vi } from "vitest";

import {
  buildIndianaCandidateFinanceLinkedElectionSyncJobId,
  runIndianaCandidateFinanceSyncJob,
} from "../../src/scheduler/indianaCandidateFinanceSyncScheduler.js";

const ORIGINAL_INDIANA_FINANCE_VALUE = process.env.INDIANA_CAMPAIGN_FINANCE_ENABLED;
const ORIGINAL_INDIANA_FINANCE_SYNC_VALUE = process.env.INDIANA_CAMPAIGN_FINANCE_SYNC_ENABLED;

describe("indianaCandidateFinanceSyncScheduler", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    if (ORIGINAL_INDIANA_FINANCE_VALUE === undefined) {
      delete process.env.INDIANA_CAMPAIGN_FINANCE_ENABLED;
    } else {
      process.env.INDIANA_CAMPAIGN_FINANCE_ENABLED = ORIGINAL_INDIANA_FINANCE_VALUE;
    }
    if (ORIGINAL_INDIANA_FINANCE_SYNC_VALUE === undefined) {
      delete process.env.INDIANA_CAMPAIGN_FINANCE_SYNC_ENABLED;
    } else {
      process.env.INDIANA_CAMPAIGN_FINANCE_SYNC_ENABLED = ORIGINAL_INDIANA_FINANCE_SYNC_VALUE;
    }
  });

  it("builds stable daily linked-election sync job IDs", () => {
    expect(buildIndianaCandidateFinanceLinkedElectionSyncJobId(new Date("2026-06-25T23:59:59Z"))).toBe(
      "indiana-candidate-finance-linked-election-sync-2026-06-25"
    );
  });

  it("rejects invalid linked-election sync job dates", () => {
    expect(() => buildIndianaCandidateFinanceLinkedElectionSyncJobId(new Date("not-a-date"))).toThrow(
      "Invalid Indiana finance linked-election sync job date"
    );
  });

  it("returns a disabled result without opening database connections when sync is disabled", async () => {
    delete process.env.INDIANA_CAMPAIGN_FINANCE_ENABLED;
    delete process.env.INDIANA_CAMPAIGN_FINANCE_SYNC_ENABLED;
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

    const result = await runIndianaCandidateFinanceSyncJob({
      dryRun: true,
      force: true,
      maxCandidates: 5,
      staleAfterDays: 3,
      triggeredBy: "manual",
    });

    expect(result).toMatchObject({
      enabled: false,
      force: true,
      triggeredBy: "manual",
      dryRun: true,
      staleAfterDays: 3,
      maxCandidates: 5,
      dueCandidateCount: 0,
      selectedCandidateCount: 0,
      syncedCandidateCount: 0,
      failedCandidateCount: 0,
      results: [],
    });
    expect(warnSpy).not.toHaveBeenCalled();
    expect(new Date(result.now).toString()).not.toBe("Invalid Date");
  });
});
