import { describe, expect, it, vi } from "vitest";

import {
  listDueWashingtonCandidateFinanceSyncRows,
  syncDueWashingtonCandidateFinance,
} from "../../../src/pipeline/washingtonFinance/washingtonCandidateFinanceBatchSync.js";
import type { WashingtonCandidateCommitteeResolution } from "../../../src/pipeline/washingtonFinance/washingtonCandidateCommitteeResolver.js";

const NOW = new Date("2026-06-01T00:00:00.000Z");
const SOURCE_URL = "https://data.wa.gov/resource/3h9x-7bvm.json";
const CANDIDATE_ID = "11111111-1111-4111-8111-111111111111";
const ELECTION_ID = "22222222-2222-4222-8222-222222222222";

function dueRow(overrides: Record<string, unknown> = {}) {
  return {
    candidate_id: CANDIDATE_ID,
    election_id: ELECTION_ID,
    candidate_name: "Bob Ferguson",
    election_year: 2024,
    office_scope: "statewide",
    office_name: "Governor",
    legislative_district: null,
    filer_id: "FERGR *115",
    committee_id: "32311",
    committee_name: "Robert W. Ferguson (Bob Ferguson)",
    candidacy_id: "689556",
    source_url: SOURCE_URL,
    last_synced_at: null,
    total_due_rows: "1",
    ...overrides,
  };
}

function matchedResolution(): Extract<WashingtonCandidateCommitteeResolution, { status: "matched" }> {
  return {
    status: "matched",
    filerId: "FERGR *115",
    committeeId: "32311",
    committeeName: "Robert W. Ferguson (Bob Ferguson)",
    candidacyId: "689556",
    confidence: "exact",
    source: "pdc_api",
    sourceUrl: SOURCE_URL,
    matchedSummaryRowCount: 1,
  };
}

describe("washingtonCandidateFinanceBatchSync", () => {
  it("lists active Washington finance links that are due for sync", async () => {
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
            legislative_district: "09",
            filer_id: "DOEJ--101",
            committee_id: "4001",
            committee_name: "Jane Doe",
            candidacy_id: null,
            source_url: null,
            last_synced_at: "2026-01-01 00:00:00+00",
            total_due_rows: "2",
          }),
        ],
        rowCount: 2,
      })),
    };

    await expect(
      listDueWashingtonCandidateFinanceSyncRows(db, {
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
          candidateName: "Bob Ferguson",
          electionYear: 2024,
          officeScope: "statewide",
          officeName: "Governor",
          legislativeDistrict: null,
          filerId: "FERGR *115",
          committeeId: "32311",
          committeeName: "Robert W. Ferguson (Bob Ferguson)",
          candidacyId: "689556",
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
          legislativeDistrict: "09",
          filerId: "DOEJ--101",
          committeeId: "4001",
          committeeName: "Jane Doe",
          candidacyId: null,
          sourceUrl: null,
          lastSyncedAt: "2026-01-01 00:00:00+00",
        },
      ],
    });

    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.wa_candidate_finance_links AS link");
    expect(sql).toContain("link.link_status = 'active'");
    expect(sql).toContain("district.state = 'WA'");
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

  it("uses one post-election grace day by default", async () => {
    const db = { query: vi.fn(async () => ({ rows: [], rowCount: 0 })), connect: vi.fn() };
    const syncWashingtonCandidateFinanceFn = vi.fn();

    await syncDueWashingtonCandidateFinance({
      db,
      now: NOW,
      autoLinkMissingLinks: false,
      syncWashingtonCandidateFinanceFn: syncWashingtonCandidateFinanceFn as never,
    });

    expect(db.query.mock.calls[0]?.[1]).toEqual([
      "2026-06-01T00:00:00.000Z",
      7,
      25,
      1,
      730,
      expect.any(Array),
    ]);
    expect(syncWashingtonCandidateFinanceFn).not.toHaveBeenCalled();
  });

  it("syncs selected due candidates with trusted linked committees and continues after a candidate failure", async () => {
    const db = {
      query: vi.fn(async () => ({
        rows: [
          dueRow({ total_due_rows: "2" }),
          dueRow({
            candidate_id: "33333333-3333-4333-8333-333333333333",
            election_id: "44444444-4444-4444-8444-444444444444",
            candidate_name: "Jane Doe",
            filer_id: "DOEJ--101",
            committee_id: "4001",
            committee_name: "Jane Doe",
            candidacy_id: null,
            source_url: null,
            total_due_rows: "2",
          }),
        ],
        rowCount: 2,
      })),
      connect: vi.fn(),
    };
    const successfulSync = {
      candidateId: CANDIDATE_ID,
      electionId: ELECTION_ID,
      electionYear: 2024,
      dryRun: false,
      linkWritten: true,
      summaryWritten: true,
      directBreakdownsWritten: 1,
      outsideGroupsWritten: 0,
      outsideGroupBreakdownsWritten: 0,
      totalReceipts: 100,
      directContributionTotal: 100,
      totalDisbursements: 50,
      outsideSupportTotal: 0,
      outsideOpposeTotal: 0,
      directOccupationRowCount: 1,
      directContributionSizeRowCount: 0,
      outsideGroupCount: 0,
      outsideFunderRowCount: 0,
      skippedOutsideGroupFunderLookupCount: 0,
      resolution: matchedResolution(),
    };
    const syncWashingtonCandidateFinanceFn = vi
      .fn()
      .mockResolvedValueOnce(successfulSync)
      .mockRejectedValueOnce(new Error("PDC unavailable"));

    const result = await syncDueWashingtonCandidateFinance({
      db,
      now: NOW,
      maxCandidates: 2,
      staleAfterDays: 3,
      electionLookbackDays: 30,
      autoLinkMissingLinks: false,
      syncWashingtonCandidateFinanceFn: syncWashingtonCandidateFinanceFn as never,
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
    expect(result.results[0]).toMatchObject({ ok: true, result: successfulSync });
    expect(result.results[1]).toMatchObject({ ok: false, error: "PDC unavailable" });
    expect(syncWashingtonCandidateFinanceFn).toHaveBeenCalledWith(
      expect.objectContaining({
        candidateId: CANDIDATE_ID,
        electionId: ELECTION_ID,
        candidateName: "Bob Ferguson",
        electionYear: 2024,
        officeScope: "statewide",
        officeName: "Governor",
        legislativeDistrict: null,
        sourceUrl: SOURCE_URL,
        trustedCommittee: {
          filerId: "FERGR *115",
          committeeId: "32311",
          committeeName: "Robert W. Ferguson (Bob Ferguson)",
          candidacyId: "689556",
          sourceUrl: SOURCE_URL,
        },
        dryRun: false,
        now: NOW,
      })
    );
  });

  it("auto-links missing eligible candidate elections before selecting due links", async () => {
    const db = {
      query: vi.fn(async (sql: string) => {
        const text = String(sql);
        if (text.includes("INSERT INTO public.wa_candidate_finance_links")) {
          return { rows: [{ id: "link-1" }], rowCount: 1 };
        }
        if (text.includes("FROM public.candidate_elections AS candidate_election") && !text.includes("WITH due AS")) {
          return {
            rows: [
              {
                candidate_id: CANDIDATE_ID,
                election_id: ELECTION_ID,
                candidate_name: "Bob Ferguson",
                election_year: 2024,
                office_scope: "statewide",
                office_name: "Governor",
                legislative_district: null,
              },
            ],
            rowCount: 1,
          };
        }
        if (text.includes("FROM public.wa_candidate_finance_links AS link")) {
          return { rows: [], rowCount: 0 };
        }
        return { rows: [], rowCount: 0 };
      }),
      connect: vi.fn(),
    };
    const resolveCandidateCommittee = vi.fn(async () => matchedResolution());

    const result = await syncDueWashingtonCandidateFinance({
      db,
      now: NOW,
      maxCandidates: 5,
      resolveCandidateCommittee,
      syncWashingtonCandidateFinanceFn: vi.fn() as never,
    });

    expect(result).toMatchObject({
      dueCandidateCount: 0,
      selectedCandidateCount: 0,
      autoLinkAttemptedCount: 1,
      autoLinkLinkedCount: 1,
    });
    expect(resolveCandidateCommittee).toHaveBeenCalledWith(
      {
        candidateName: "Bob Ferguson",
        officeScope: "statewide",
        officeName: "Governor",
        electionYear: 2024,
        legislativeDistrict: null,
      },
      undefined
    );
    expect(db.query.mock.calls.some((call) => String(call[0]).includes("INSERT INTO public.wa_candidate_finance_links"))).toBe(
      true
    );
  });
});
