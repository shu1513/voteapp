import { describe, expect, it, vi } from "vitest";

import { listDueGeorgiaCandidateFinanceSyncRows } from "../../../src/pipeline/georgiaFinance/georgiaCandidateFinanceDueList.js";
import { GEORGIA_FINANCE_ELIGIBLE_OFFICE_KEYS } from "../../../src/pipeline/georgiaFinance/georgiaFinanceEligibleOffices.js";

const NOW = new Date("2026-06-01T00:00:00.000Z");

const DUE_LIST_INPUT = {
  now: NOW,
  staleAfterDays: 7,
  maxCandidates: 25,
  electionLookbackDays: 30,
  electionLookaheadDays: 730,
};

describe("listDueGeorgiaCandidateFinanceSyncRows", () => {
  it("targets the ga tables with the GA state filter and eligible office keys", async () => {
    const db = { query: vi.fn().mockResolvedValue({ rows: [] }) };
    const result = await listDueGeorgiaCandidateFinanceSyncRows(db, DUE_LIST_INPUT);
    expect(result).toEqual({ rows: [], totalDueRows: 0 });
    const [sql, params] = db.query.mock.calls[0]!;
    expect(sql).toContain("FROM public.ga_candidate_finance_links AS link");
    expect(sql).toContain("LEFT JOIN public.ga_candidate_finance_summaries AS summary");
    expect(sql).toContain("district.state = 'GA'");
    expect(sql).toContain("link.committee_id");
    expect(params).toEqual([NOW.toISOString(), 7, 25, 30, 730, [...GEORGIA_FINANCE_ELIGIBLE_OFFICE_KEYS]]);
  });

  it("maps canonical rows to camelCase due rows", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({
        rows: [
          {
            candidate_id: "11111111-1111-4111-8111-111111111111",
            election_id: "22222222-2222-4222-8222-222222222222",
            candidate_name: "Christopher Carr",
            election_year: 2026,
            office_scope: "statewide",
            office_name: "Governor",
            district: null,
            committee_id: "100035",
            committee_name: "Carr for Georgia, Inc.",
            source_url: "https://ethics.ga.gov/records-search-all/",
            last_synced_at: null,
            total_due_rows: "1",
          },
        ],
      }),
    };
    const result = await listDueGeorgiaCandidateFinanceSyncRows(db, DUE_LIST_INPUT);
    expect(result.totalDueRows).toBe(1);
    expect(result.rows[0]).toEqual({
      candidateId: "11111111-1111-4111-8111-111111111111",
      electionId: "22222222-2222-4222-8222-222222222222",
      candidateName: "Christopher Carr",
      electionYear: 2026,
      officeScope: "statewide",
      officeName: "Governor",
      district: null,
      committeeId: "100035",
      committeeName: "Carr for Georgia, Inc.",
      sourceUrl: "https://ethics.ga.gov/records-search-all/",
      lastSyncedAt: null,
    });
  });
});
