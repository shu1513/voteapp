import { describe, expect, it, vi } from "vitest";

import {
  listDueVermontCandidateFinanceSyncRows,
  syncDueVermontCandidateFinance,
} from "../../../src/pipeline/vermontFinance/vermontCandidateFinanceBatchSync.js";
import type { VermontCandidateFinanceSyncResult } from "../../../src/pipeline/vermontFinance/vermontCandidateFinanceSync.js";

const NOW = new Date("2026-06-01T00:00:00.000Z");
const SOURCE_URL = "https://campaignfinance.vermont.gov/";
const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";

function dueRow(overrides: Record<string, unknown> = {}) {
  return {
    candidate_id: CANDIDATE_ID,
    election_id: ELECTION_ID,
    candidate_name: "Phil Scott",
    election_year: 2024,
    office_scope: "statewide",
    office_name: "Governor",
    district: null,
    filer_registration_guid: "candidate-guid",
    entity_id: 33545,
    filer_name: "SCOTT, PHIL",
    source_url: SOURCE_URL,
    last_synced_at: null,
    total_due_rows: "1",
    ...overrides,
  };
}

function successfulSync(): VermontCandidateFinanceSyncResult {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    electionYear: 2024,
    dryRun: false,
    resolution: {
      status: "matched",
      filerRegistrationGuid: "candidate-guid",
      filerName: "SCOTT, PHIL",
      candidateName: null,
      officeId: 19,
      officeName: "Governor",
      electionYear: 2024,
      electionId: 35,
      entityId: 33545,
      reportName: null,
      confidence: "exact",
      source: "vermont_public_transactions",
      sourceUrl: SOURCE_URL,
      matchedTransactionRowCount: 1,
    },
    linkWritten: true,
    summaryWritten: true,
    directBreakdownsWritten: 1,
    outsideGroupsWritten: 1,
    outsideGroupBreakdownsWritten: 2,
    totalReceipts: 100,
    directContributionTotal: 100,
    outsideSupportTotal: 1000,
    outsideOpposeTotal: 0,
    matchedContributionRowCount: 1,
    includedContributionRowCount: 1,
    skippedContributionRowCount: 0,
    matchedExpenditureRowCount: 1,
    includedExpenditureRowCount: 1,
    skippedExpenditureRowCount: 0,
    matchedOutsideContributionRowCount: 1,
    includedOutsideContributionRowCount: 1,
    skippedOutsideContributionRowCount: 0,
    fetchedContributionRowCount: 1,
    fetchedExpenditureRowCount: 1,
    fetchedOutsideContributionRowCount: 1,
  };
}

describe("vermontCandidateFinanceBatchSync", () => {
  it("lists active Vermont finance links that are due for sync", async () => {
    const db = {
      query: vi.fn(async () => ({
        rows: [
          dueRow({ total_due_rows: "2" }),
          dueRow({
            candidate_id: "33333333-3333-4333-8333-333333333333",
            election_id: "44444444-4444-4444-8444-444444444444",
            candidate_name: "Jane Doe",
            election_year: 2026,
            office_scope: "state_lower",
            office_name: "State Lower Chamber Legislator",
            district: "Washington-1",
            filer_registration_guid: "candidate-guid-2",
            entity_id: null,
            filer_name: "DOE, JANE",
            source_url: null,
            last_synced_at: "2026-01-01 00:00:00+00",
            total_due_rows: "2",
          }),
        ],
        rowCount: 2,
      })),
    };

    await expect(
      listDueVermontCandidateFinanceSyncRows(db, {
        now: NOW,
        staleAfterDays: 7,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
      })
    ).resolves.toEqual({
      totalDueRows: 2,
      rows: [
        {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "Phil Scott",
          electionYear: 2024,
          officeScope: "statewide",
          officeName: "Governor",
          district: null,
          filerRegistrationGuid: "candidate-guid",
          entityId: 33545,
          filerName: "SCOTT, PHIL",
          sourceUrl: SOURCE_URL,
          lastSyncedAt: null,
        },
        {
          candidateId: "33333333-3333-4333-8333-333333333333",
          electionId: "44444444-4444-4444-8444-444444444444",
          candidateName: "Jane Doe",
          electionYear: 2026,
          officeScope: "state_lower",
          officeName: "State Lower Chamber Legislator",
          district: "Washington-1",
          filerRegistrationGuid: "candidate-guid-2",
          entityId: null,
          filerName: "DOE, JANE",
          sourceUrl: null,
          lastSyncedAt: "2026-01-01 00:00:00+00",
        },
      ],
    });

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.vt_candidate_finance_links AS link");
    expect(sql).toContain("link.link_status = 'active'");
    expect(sql).toContain("district.state = 'VT'");
    expect(sql).toContain("election.race_type = 'office'");
    expect(sql).toContain("candidate_election.status NOT IN ('withdrawn', 'lost')");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($6::text[])");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      7,
      25,
      1,
      730,
      expect.arrayContaining([
        "statewide::Governor",
        "state_upper::State Senator",
        "state_lower::State Lower Chamber Legislator",
      ]),
    ]);
  });

  it("syncs selected due candidates with trusted Vermont filer identities and continues after failure", async () => {
    const db = {
      query: vi.fn(async () => ({
        rows: [
          dueRow({ total_due_rows: "2" }),
          dueRow({
            candidate_id: "33333333-3333-4333-8333-333333333333",
            election_id: "44444444-4444-4444-8444-444444444444",
            candidate_name: "Jane Doe",
            filer_registration_guid: "candidate-guid-2",
            filer_name: "DOE, JANE",
            source_url: null,
            total_due_rows: "2",
          }),
        ],
        rowCount: 2,
      })),
      connect: vi.fn(),
    };
    const syncVermontCandidateFinanceFn = vi
      .fn()
      .mockResolvedValueOnce(successfulSync())
      .mockRejectedValueOnce(new Error("Vermont API unavailable"));

    const result = await syncDueVermontCandidateFinance({
      db,
      now: NOW,
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 30,
      syncVermontCandidateFinanceFn: syncVermontCandidateFinanceFn as never,
    });

    expect(result).toMatchObject({
      dryRun: false,
      dueCandidateCount: 2,
      selectedCandidateCount: 2,
      syncedCandidateCount: 1,
      failedCandidateCount: 1,
      autoLinkAttemptedCount: 0,
      autoLinkLinkedCount: 0,
    });
    expect(result.results[0]).toMatchObject({ ok: true, result: successfulSync() });
    expect(result.results[1]).toMatchObject({ ok: false, error: "Vermont API unavailable" });
    expect(syncVermontCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Phil Scott",
        electionYear: 2024,
        officeScope: "statewide",
        officeName: "Governor",
        district: null,
        sourceUrl: SOURCE_URL,
        trustedCommittee: {
          filerRegistrationGuid: "candidate-guid",
          filerName: "SCOTT, PHIL",
          entityId: 33545,
          sourceUrl: SOURCE_URL,
        },
        dryRun: false,
        now: NOW,
      })
    );
  });

  it("uses one post-election grace day by default", async () => {
    const db = { query: vi.fn(async () => ({ rows: [], rowCount: 0 })), connect: vi.fn() };
    const syncVermontCandidateFinanceFn = vi.fn();

    await syncDueVermontCandidateFinance({
      db,
      now: NOW,
      syncVermontCandidateFinanceFn: syncVermontCandidateFinanceFn as never,
    });

    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      7,
      25,
      1,
      730,
      expect.any(Array),
    ]);
    expect(syncVermontCandidateFinanceFn).not.toHaveBeenCalled();
  });
});
