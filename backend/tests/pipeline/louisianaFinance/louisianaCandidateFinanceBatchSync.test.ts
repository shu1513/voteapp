import { describe, expect, it, vi } from "vitest";

import {
  listDueLouisianaCandidateFinanceSyncRows,
  syncDueLouisianaCandidateFinance,
} from "../../../src/pipeline/louisianaFinance/louisianaCandidateFinanceBatchSync.js";

const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const NOW = new Date("2026-06-01T00:00:00.000Z");

function createMockDb(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows, rowCount: rows.length }),
    connect: vi.fn(),
  };
}

function dueRow() {
  return {
    candidate_id: CANDIDATE_ID,
    election_id: ELECTION_ID,
    candidate_name: "John Bel Edwards",
    election_year: 2027,
    office_scope: "statewide",
    office_name: "Governor",
    district: null,
    filer_number: "12345",
    filer_name: "Edwards, John Bel",
    source_url: "https://example.invalid/source",
    last_synced_at: null,
    total_due_rows: "1",
  };
}

describe("louisianaCandidateFinanceBatchSync", () => {
  it("lists active Louisiana finance links that are due for sync", async () => {
    const db = createMockDb([dueRow()]);

    await expect(
      listDueLouisianaCandidateFinanceSyncRows(db, {
        now: NOW,
        staleAfterDays: 7,
        maxCandidates: 25,
        electionLookbackDays: 1,
        electionLookaheadDays: 730,
      })
    ).resolves.toEqual({
      totalDueRows: 1,
      rows: [
        {
          candidateId: CANDIDATE_ID,
          electionId: ELECTION_ID,
          candidateName: "John Bel Edwards",
          electionYear: 2027,
          officeScope: "statewide",
          officeName: "Governor",
          district: null,
          filerNumber: "12345",
          filerName: "Edwards, John Bel",
          sourceUrl: "https://example.invalid/source",
          lastSyncedAt: null,
        },
      ],
    });

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.la_candidate_finance_links AS link");
    expect(sql).toContain("link.link_status = 'active'");
    expect(sql).toContain("district.state = 'LA'");
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
        "statewide::Attorney General",
        "state_upper::State Senator",
        "state_lower::State Lower Chamber Legislator",
      ]),
    ]);
  });

  it("syncs selected due candidates with trusted linked Louisiana filers", async () => {
    const db = createMockDb([dueRow()]);
    const syncLouisianaCandidateFinanceFn = vi.fn().mockResolvedValue({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2027,
      dryRun: false,
      resolution: {
        status: "matched",
        filerNumber: "12345",
        filerName: "Edwards, John Bel",
        confidence: "exact",
        source: "la_ethics_search",
        sourceUrl: "https://example.invalid/source",
        matchedCandidateRowCount: 0,
      },
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 0,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      totalReceipts: 0,
      directContributionTotal: 0,
      outsideSupportTotal: 0,
      outsideOpposeTotal: 0,
      matchedContributionRowCount: 0,
      includedContributionRowCount: 0,
      skippedContributionRowCount: 0,
      matchedOutsideExpenditureRowCount: 0,
      includedOutsideExpenditureRowCount: 0,
      skippedOutsideExpenditureRowCount: 0,
      matchedOutsideContributionRowCount: 0,
      includedOutsideContributionRowCount: 0,
      skippedOutsideContributionRowCount: 0,
    });

    const result = await syncDueLouisianaCandidateFinance({
      db,
      now: NOW,
      maxCandidates: 25,
      staleAfterDays: 7,
      electionLookbackDays: 1,
      electionLookaheadDays: 730,
      autoLinkMissingLinks: false,
      contributionRows: [],
      expenditureRows: [],
      syncLouisianaCandidateFinanceFn: syncLouisianaCandidateFinanceFn as never,
    });

    expect(result).toMatchObject({
      dryRun: false,
      now: NOW.toISOString(),
      staleAfterDays: 7,
      maxCandidates: 25,
      dueCandidateCount: 1,
      selectedCandidateCount: 1,
      syncedCandidateCount: 1,
      failedCandidateCount: 0,
      autoLinkAttemptedCount: 0,
      autoLinkLinkedCount: 0,
    });
    expect(result.results).toHaveLength(1);
    expect(syncLouisianaCandidateFinanceFn).toHaveBeenCalledTimes(1);
    expect(syncLouisianaCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "John Bel Edwards",
        electionYear: 2027,
        officeScope: "statewide",
        officeName: "Governor",
        district: null,
        sourceUrl: "https://example.invalid/source",
        contributionRows: [],
        contributionSourceUrl: null,
        expenditureRows: [],
        expenditureSourceUrl: null,
        trustedCommittee: {
          filerNumber: "12345",
          filerName: "Edwards, John Bel",
          sourceUrl: "https://example.invalid/source",
        },
        dryRun: false,
        now: NOW,
      })
    );
  });
});
