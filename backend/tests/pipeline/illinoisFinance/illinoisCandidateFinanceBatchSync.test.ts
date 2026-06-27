import { describe, expect, it, vi } from "vitest";

import {
  listDueIllinoisCandidateFinanceSyncRows,
  syncDueIllinoisCandidateFinance,
} from "../../../src/pipeline/illinoisFinance/illinoisCandidateFinanceBatchSync.js";
import type { IllinoisCandidateFinanceSyncResult } from "../../../src/pipeline/illinoisFinance/syncIllinoisCandidateFinance.js";
import type { IllinoisSbeContributionRecord } from "../../../src/pipeline/illinoisFinance/illinoisSbeClient.js";

const NOW = new Date("2026-06-01T00:00:00.000Z");
const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";
const SOURCE_URL = "https://www.elections.il.gov/CampaignDisclosure/ContributionSearchByCommittees.aspx";

function dueRow(overrides: Record<string, unknown> = {}) {
  return {
    candidate_id: CANDIDATE_ID,
    election_id: ELECTION_ID,
    candidate_name: "Jane Doe",
    election_year: 2026,
    office_scope: "statewide",
    office_name: "Governor",
    district: null,
    committee_key: "JANE DOE",
    committee_name: "Friends of Jane Doe",
    source_url: SOURCE_URL,
    last_synced_at: null,
    total_due_rows: "1",
    ...overrides,
  };
}

function contribution(overrides: Partial<IllinoisSbeContributionRecord> = {}): IllinoisSbeContributionRecord {
  return {
    contributorName: "Pat Person",
    contributorAddress: "1 Main St",
    occupation: "Attorney",
    employer: "Law LLP",
    amount: 250,
    receivedDate: "3/1/2026",
    reportReceivedDate: null,
    contributionType: "Individual Contributions",
    recipientCommitteeName: "Friends of Jane Doe",
    description: null,
    vendorName: null,
    vendorAddress: null,
    sourceUrl: SOURCE_URL,
    ...overrides,
  };
}

function successfulSync(overrides: Partial<IllinoisCandidateFinanceSyncResult> = {}): IllinoisCandidateFinanceSyncResult {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    electionYear: 2026,
    dryRun: false,
    linkWritten: true,
    summaryWritten: true,
    directBreakdownsWritten: 2,
    outsideGroupsWritten: 0,
    outsideGroupBreakdownsWritten: 0,
    totalReceipts: 250,
    directContributionTotal: 250,
    outsideSupportTotal: 0,
    outsideOpposeTotal: 0,
    matchedContributionRowCount: 1,
    includedContributionRowCount: 1,
    skippedContributionRowCount: 0,
    matchedOutsideExpenditureRowCount: 0,
    includedOutsideExpenditureRowCount: 0,
    skippedOutsideExpenditureRowCount: 0,
    matchedOutsideContributionRowCount: 0,
    includedOutsideContributionRowCount: 0,
    skippedOutsideContributionRowCount: 0,
    ...overrides,
  };
}

describe("illinoisCandidateFinanceBatchSync", () => {
  it("lists active Illinois finance links that are due for sync", async () => {
    const db = {
      query: vi.fn(async () => ({
        rows: [
          dueRow({ total_due_rows: "2" }),
          dueRow({
            candidate_id: "33333333-3333-4333-8333-333333333333",
            election_id: "44444444-4444-4444-8444-444444444444",
            candidate_name: "John Smith",
            office_scope: "state_lower",
            office_name: "State Lower Chamber Legislator",
            district: "44",
            committee_key: "JOHN SMITH",
            committee_name: "Friends of John Smith",
            source_url: null,
            last_synced_at: "2026-01-01 00:00:00+00",
            total_due_rows: "2",
          }),
        ],
        rowCount: 2,
      })),
    };

    await expect(
      listDueIllinoisCandidateFinanceSyncRows(db, {
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
          candidateName: "Jane Doe",
          electionYear: 2026,
          officeScope: "statewide",
          officeName: "Governor",
          district: null,
          committeeKey: "JANE DOE",
          committeeName: "Friends of Jane Doe",
          sourceUrl: SOURCE_URL,
          lastSyncedAt: null,
        },
        {
          candidateId: "33333333-3333-4333-8333-333333333333",
          electionId: "44444444-4444-4444-8444-444444444444",
          candidateName: "John Smith",
          electionYear: 2026,
          officeScope: "state_lower",
          officeName: "State Lower Chamber Legislator",
          district: "44",
          committeeKey: "JOHN SMITH",
          committeeName: "Friends of John Smith",
          sourceUrl: null,
          lastSyncedAt: "2026-01-01 00:00:00+00",
        },
      ],
    });

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.il_candidate_finance_links AS link");
    expect(sql).toContain("link.link_status = 'active'");
    expect(sql).toContain("district.state = 'IL'");
    expect(sql).toContain("(office.scope || '::' || office.canonical_name) = ANY($6::text[])");
    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      7,
      25,
      1,
      730,
      expect.arrayContaining(["statewide::Governor", "state_lower::State Lower Chamber Legislator"]),
    ]);
  });

  it("uses one post-election grace day by default", async () => {
    const db = { query: vi.fn(async () => ({ rows: [], rowCount: 0 })), connect: vi.fn() };
    const syncIllinoisCandidateFinanceFn = vi.fn();

    await syncDueIllinoisCandidateFinance({
      db,
      now: NOW,
      autoLinkMissingLinks: false,
      syncIllinoisCandidateFinanceFn: syncIllinoisCandidateFinanceFn as never,
    });

    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      7,
      25,
      1,
      730,
      expect.any(Array),
    ]);
    expect(syncIllinoisCandidateFinanceFn).not.toHaveBeenCalled();
  });

  it("syncs selected due candidates and continues after a candidate failure", async () => {
    const db = {
      query: vi.fn(async () => ({
        rows: [
          dueRow({ total_due_rows: "2" }),
          dueRow({
            candidate_id: "33333333-3333-4333-8333-333333333333",
            election_id: "44444444-4444-4444-8444-444444444444",
            committee_key: "JOHN SMITH",
            committee_name: "Friends of John Smith",
            total_due_rows: "2",
          }),
        ],
        rowCount: 2,
      })),
      connect: vi.fn(),
    };
    const loadIllinoisFinanceDataFn = vi.fn(async () => ({
      directContributionRecords: [contribution()],
      outsideExpenditureRecords: [],
      outsideGroupContributionRecords: [],
      directContributionSourceUrl: SOURCE_URL,
    }));
    const syncIllinoisCandidateFinanceFn = vi
      .fn()
      .mockResolvedValueOnce(successfulSync())
      .mockRejectedValueOnce(new Error("SBE parse failed"));

    const result = await syncDueIllinoisCandidateFinance({
      db,
      now: NOW,
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 30,
      autoLinkMissingLinks: false,
      loadIllinoisFinanceDataFn,
      syncIllinoisCandidateFinanceFn: syncIllinoisCandidateFinanceFn as never,
    });

    expect(result).toMatchObject({
      dryRun: false,
      staleAfterDays: 3,
      maxCandidates: 2,
      dueCandidateCount: 2,
      selectedCandidateCount: 2,
      syncedCandidateCount: 1,
      failedCandidateCount: 1,
      autoLinkAttemptedCount: 0,
      autoLinkLinkedCount: 0,
    });
    expect(loadIllinoisFinanceDataFn).toHaveBeenCalledTimes(2);
    expect(syncIllinoisCandidateFinanceFn).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        committeeKey: "JANE DOE",
        committeeName: "Friends of Jane Doe",
        directContributionRecords: [contribution()],
        dryRun: false,
      })
    );
    expect(result.results[1]).toMatchObject({
      candidateId: "33333333-3333-4333-8333-333333333333",
      ok: false,
      error: "SBE parse failed",
    });
  });

  it("auto-links missing candidates before listing due rows when enabled", async () => {
    const db = {
      query: vi
        .fn()
        .mockResolvedValueOnce({
          rows: [
            {
              candidate_id: CANDIDATE_ID,
              election_id: ELECTION_ID,
              candidate_name: "Jane Doe",
              election_year: 2026,
              office_scope: "statewide",
              office_name: "Governor",
              district: null,
            },
          ],
          rowCount: 1,
        })
        .mockResolvedValueOnce({ rows: [{ id: "link-1" }], rowCount: 1 })
        .mockResolvedValueOnce({ rows: [], rowCount: 0 }),
      connect: vi.fn(),
    };

    const result = await syncDueIllinoisCandidateFinance({
      db,
      now: NOW,
      maxCandidates: 1,
      resolveCandidateCommittee: vi.fn(async () => ({
        status: "matched",
        committeeKey: "JANE DOE",
        committeeName: "Friends of Jane Doe",
        confidence: "exact",
        source: "illinois_sbe",
        sourceUrl: SOURCE_URL,
        matchedContributionRowCount: 1,
      })),
      loadIllinoisFinanceDataFn: vi.fn(),
      syncIllinoisCandidateFinanceFn: vi.fn() as never,
    });

    expect(result).toMatchObject({
      dueCandidateCount: 0,
      selectedCandidateCount: 0,
      autoLinkAttemptedCount: 1,
      autoLinkLinkedCount: 1,
    });
    expect(String(db.query.mock.calls[0]?.[0])).toContain("FROM public.candidate_elections AS candidate_election");
    expect(String(db.query.mock.calls[1]?.[0])).toContain("INSERT INTO public.il_candidate_finance_links");
    expect(String(db.query.mock.calls[2]?.[0])).toContain("FROM public.il_candidate_finance_links AS link");
  });
});
