import { describe, expect, it, vi } from "vitest";

import { listDueMontanaCandidateFinanceSyncRows } from "../../../src/pipeline/montanaFinance/montanaCandidateFinanceDueList.js";
import { MONTANA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "../../../src/pipeline/montanaFinance/montanaFinanceEligibleOffices.js";

describe("listDueMontanaCandidateFinanceSyncRows", () => {
  it("queries active general-election links with staleness gating and maps rows", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({
        rows: [
          {
            candidate_id: "candidate-1",
            election_id: "election-1",
            candidate_name: "David Bedey",
            election_year: 2026,
            election_date: "2026-11-03T00:00:00.000Z",
            office_scope: "state_upper",
            office_name: "State Senator",
            district: "43",
            committee_id: "21020",
            committee_name: "Bedey, David F.",
            link_source: "cers_portal",
            source_url: "https://cers-ext.mt.gov/CampaignTracker/dashboard",
            last_synced_at: null,
            total_due_rows: "2",
          },
        ],
      }),
    };

    const result = await listDueMontanaCandidateFinanceSyncRows(db, {
      now: new Date("2026-08-28T00:00:00.000Z"),
      staleAfterDays: 7,
      maxCandidates: 10,
      electionLookbackDays: 55,
      electionLookaheadDays: 730,
    });

    const [sql, params] = db.query.mock.calls[0]!;
    expect(String(sql)).toContain("mt_candidate_finance_links");
    expect(String(sql)).toContain("mt_candidate_finance_summaries");
    expect(String(sql)).toContain("election.election_stage='general'");
    expect(String(sql)).toContain("district_row.state='MT'");
    expect(params).toEqual([
      "2026-08-28T00:00:00.000Z",
      7,
      10,
      55,
      730,
      [...MONTANA_FINANCE_ELIGIBLE_OFFICE_KEYS],
    ]);

    expect(result.totalDueRows).toBe(2);
    expect(result.rows).toEqual([
      {
        candidateId: "candidate-1",
        electionId: "election-1",
        candidateName: "David Bedey",
        electionYear: 2026,
        electionDate: "2026-11-03",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "43",
        committeeId: "21020",
        committeeName: "Bedey, David F.",
        linkSource: "cers_portal",
        sourceUrl: "https://cers-ext.mt.gov/CampaignTracker/dashboard",
        lastSyncedAt: null,
      },
    ]);
  });

  it("rejects a corrupted stored CERS id instead of syncing the wrong entity", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({
        rows: [
          {
            candidate_id: "candidate-1",
            election_id: "election-1",
            candidate_name: "X",
            election_year: 2026,
            election_date: "2026-11-03",
            office_scope: "state_upper",
            office_name: "State Senator",
            district: null,
            committee_id: "0",
            committee_name: "Y",
            link_source: "manual",
            source_url: null,
            last_synced_at: null,
            total_due_rows: 1,
          },
        ],
      }),
    };

    await expect(
      listDueMontanaCandidateFinanceSyncRows(db, {
        now: new Date("2026-08-28T00:00:00.000Z"),
        staleAfterDays: 7,
        maxCandidates: 10,
        electionLookbackDays: 55,
        electionLookaheadDays: 730,
      })
    ).rejects.toThrow("Invalid Montana CERS entity id: 0");
  });

  it("returns zero totals for an empty due list", async () => {
    const db = { query: vi.fn().mockResolvedValue({ rows: [] }) };
    const result = await listDueMontanaCandidateFinanceSyncRows(db, {
      now: new Date("2026-08-28T00:00:00.000Z"),
      staleAfterDays: 7,
      maxCandidates: 10,
      electionLookbackDays: 55,
      electionLookaheadDays: 730,
    });
    expect(result).toEqual({ rows: [], totalDueRows: 0 });
  });
});
