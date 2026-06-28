import { describe, expect, it, vi } from "vitest";

import {
  listDueKentuckyCandidateFinanceSyncRows,
  syncDueKentuckyCandidateFinance,
} from "../../../src/pipeline/kentuckyFinance/kentuckyCandidateFinanceBatchSync.js";
import type { KentuckyCandidateFinanceSyncResult } from "../../../src/pipeline/kentuckyFinance/kentuckyCandidateFinanceSync.js";

const NOW = new Date("2026-06-01T00:00:00.000Z");
const SOURCE_URL = "https://secure.kentucky.gov/kref/publicsearch/ToCandidateSearch";
const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";

function dueRow(overrides: Record<string, unknown> = {}) {
  return {
    candidate_id: CANDIDATE_ID,
    election_id: ELECTION_ID,
    candidate_name: "Andy Beshear",
    election_year: 2023,
    election_date: "2023-11-07",
    office_scope: "statewide",
    office_name: "Governor",
    location: "Statewide",
    candidate_key: "ANDY BESHEAR|GOVERNOR|STATEWIDE|2023-11-07",
    committee_key: "BESHEAR CAMPAIGN COMMITTEE",
    committee_name: "Beshear Campaign Committee",
    source_url: SOURCE_URL,
    last_synced_at: null,
    total_due_rows: "1",
    ...overrides,
  };
}

function successfulSync(overrides: Partial<KentuckyCandidateFinanceSyncResult> = {}): KentuckyCandidateFinanceSyncResult {
  return {
    candidateId: CANDIDATE_ID,
    electionId: ELECTION_ID,
    electionYear: 2023,
    dryRun: false,
    linkWritten: true,
    summaryWritten: true,
    directBreakdownsWritten: 3,
    outsideGroupsWritten: 1,
    outsideGroupBreakdownsWritten: 2,
    totalReceipts: 1750,
    directContributionTotal: 750,
    outsideSupportTotal: 10_000,
    outsideOpposeTotal: 0,
    matchedContributionRowCount: 3,
    includedContributionRowCount: 2,
    skippedContributionRowCount: 1,
    matchedExpenditureRowCount: 1,
    includedExpenditureRowCount: 1,
    skippedExpenditureRowCount: 0,
    matchedOutsideContributionRowCount: 1,
    includedOutsideContributionRowCount: 1,
    skippedOutsideContributionRowCount: 0,
    candidateContributionRowCount: 3,
    independentExpenditureRowCount: 1,
    outsideContributionRowCount: 1,
    ...overrides,
  };
}

describe("kentuckyCandidateFinanceBatchSync", () => {
  it("lists active Kentucky finance links that are due for sync", async () => {
    const db = {
      query: vi.fn(async () => ({
        rows: [
          dueRow({ total_due_rows: "2" }),
          dueRow({
            candidate_id: "33333333-3333-4333-8333-333333333333",
            election_id: "44444444-4444-4444-8444-444444444444",
            candidate_name: "Jane Doe",
            election_year: 2026,
            election_date: "2026-11-03",
            office_scope: "state_lower",
            office_name: "State Lower Chamber Legislator",
            location: "9",
            candidate_key: "JANE DOE|HOUSE|9|2026-11-03",
            committee_key: "JANE DOE CAMPAIGN",
            committee_name: "Jane Doe Campaign",
            source_url: null,
            last_synced_at: "2026-01-01 00:00:00+00",
            total_due_rows: "2",
          }),
        ],
        rowCount: 2,
      })),
    };

    await expect(
      listDueKentuckyCandidateFinanceSyncRows(db, {
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
          candidateName: "Andy Beshear",
          electionYear: 2023,
          electionDate: "2023-11-07",
          officeScope: "statewide",
          officeName: "Governor",
          location: "Statewide",
          candidateKey: "ANDY BESHEAR|GOVERNOR|STATEWIDE|2023-11-07",
          committeeKey: "BESHEAR CAMPAIGN COMMITTEE",
          committeeName: "Beshear Campaign Committee",
          sourceUrl: SOURCE_URL,
          lastSyncedAt: null,
        },
        {
          candidateId: "33333333-3333-4333-8333-333333333333",
          electionId: "44444444-4444-4444-8444-444444444444",
          candidateName: "Jane Doe",
          electionYear: 2026,
          electionDate: "2026-11-03",
          officeScope: "state_lower",
          officeName: "State Lower Chamber Legislator",
          location: "9",
          candidateKey: "JANE DOE|HOUSE|9|2026-11-03",
          committeeKey: "JANE DOE CAMPAIGN",
          committeeName: "Jane Doe Campaign",
          sourceUrl: null,
          lastSyncedAt: "2026-01-01 00:00:00+00",
        },
      ],
    });

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.ky_candidate_finance_links AS link");
    expect(sql).toContain("link.link_status = 'active'");
    expect(sql).toContain("district.state = 'KY'");
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

  it("syncs selected due candidates with trusted KREF keys and continues after a candidate failure", async () => {
    const db = {
      query: vi.fn(async () => ({
        rows: [
          dueRow({ total_due_rows: "2" }),
          dueRow({
            candidate_id: "33333333-3333-4333-8333-333333333333",
            election_id: "44444444-4444-4444-8444-444444444444",
            candidate_name: "Jane Doe",
            candidate_key: "JANE DOE|GOVERNOR|STATEWIDE|2023-11-07",
            committee_key: "JANE DOE CAMPAIGN",
            committee_name: "Jane Doe Campaign",
            source_url: null,
            total_due_rows: "2",
          }),
        ],
        rowCount: 2,
      })),
      connect: vi.fn(),
    };
    const syncKentuckyCandidateFinanceFn = vi
      .fn()
      .mockResolvedValueOnce(successfulSync())
      .mockRejectedValueOnce(new Error("KREF unavailable"));

    const result = await syncDueKentuckyCandidateFinance({
      db,
      now: NOW,
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 30,
      autoLinkMissingLinks: false,
      syncKentuckyCandidateFinanceFn: syncKentuckyCandidateFinanceFn as never,
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
    expect(result.results[1]).toMatchObject({ ok: false, error: "KREF unavailable" });
    expect(syncKentuckyCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Andy Beshear",
        electionYear: 2023,
        electionDate: "2023-11-07",
        officeName: "Governor",
        location: "Statewide",
        district: "Statewide",
        sourceUrl: SOURCE_URL,
        trustedLink: {
          candidateKey: "ANDY BESHEAR|GOVERNOR|STATEWIDE|2023-11-07",
          committeeKey: "BESHEAR CAMPAIGN COMMITTEE",
          committeeName: "Beshear Campaign Committee",
          sourceUrl: SOURCE_URL,
        },
        dryRun: false,
        now: NOW,
      })
    );
  });

  it("uses one post-election grace day by default", async () => {
    const db = { query: vi.fn(async () => ({ rows: [], rowCount: 0 })), connect: vi.fn() };
    const syncKentuckyCandidateFinanceFn = vi.fn();

    await syncDueKentuckyCandidateFinance({
      db,
      now: NOW,
      autoLinkMissingLinks: false,
      syncKentuckyCandidateFinanceFn: syncKentuckyCandidateFinanceFn as never,
    });

    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      7,
      25,
      1,
      730,
      expect.any(Array),
    ]);
    expect(syncKentuckyCandidateFinanceFn).not.toHaveBeenCalled();
  });

  it("auto-links missing candidates only through an injected resolver", async () => {
    const db = {
      query: vi.fn(async (sql: string) => {
        const text = String(sql);
        if (text.includes("INSERT INTO public.ky_candidate_finance_links")) {
          return { rows: [{ id: "link-1" }], rowCount: 1 };
        }
        if (text.includes("FROM public.candidate_elections AS candidate_election") && !text.includes("WITH due AS")) {
          return {
            rows: [
              {
                candidate_id: CANDIDATE_ID,
                election_id: ELECTION_ID,
                candidate_name: "Andy Beshear",
                election_year: 2023,
                election_date: "2023-11-07",
                office_scope: "statewide",
                office_name: "Governor",
                location: "Statewide",
              },
            ],
            rowCount: 1,
          };
        }
        if (text.includes("FROM public.ky_candidate_finance_links AS link")) {
          return { rows: [], rowCount: 0 };
        }
        return { rows: [], rowCount: 0 };
      }),
      connect: vi.fn(),
    };
    const resolveCandidateFinanceLink = vi.fn(async () => ({
      status: "matched" as const,
      candidateKey: "ANDY BESHEAR|GOVERNOR|STATEWIDE|2023-11-07",
      committeeKey: "BESHEAR CAMPAIGN COMMITTEE",
      committeeName: "Beshear Campaign Committee",
      sourceUrl: SOURCE_URL,
    }));

    const result = await syncDueKentuckyCandidateFinance({
      db,
      now: NOW,
      maxCandidates: 5,
      autoLinkMissingLinks: true,
      resolveCandidateFinanceLink,
      syncKentuckyCandidateFinanceFn: vi.fn() as never,
    });

    expect(result).toMatchObject({
      dueCandidateCount: 0,
      selectedCandidateCount: 0,
      autoLinkAttemptedCount: 1,
      autoLinkLinkedCount: 1,
    });
    expect(resolveCandidateFinanceLink).toHaveBeenCalledWith({
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      candidateName: "Andy Beshear",
      electionYear: 2023,
      electionDate: "2023-11-07",
      officeScope: "statewide",
      officeName: "Governor",
      location: "Statewide",
    });
    expect(
      db.query.mock.calls.some((call) => String(call[0]).includes("INSERT INTO public.ky_candidate_finance_links"))
    ).toBe(true);
  });
});
