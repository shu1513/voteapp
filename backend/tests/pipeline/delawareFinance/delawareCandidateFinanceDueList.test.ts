import { describe, expect, it, vi } from "vitest";

import { listDueDelawareCandidateFinanceSyncRows } from "../../../src/pipeline/delawareFinance/delawareCandidateFinanceDueList.js";
import { DELAWARE_FINANCE_ELIGIBLE_OFFICE_KEYS } from "../../../src/pipeline/delawareFinance/delawareFinanceEligibleOffices.js";

describe("listDueDelawareCandidateFinanceSyncRows", () => {
  it("queries link-gated staleness and returns ISO election dates for the window resolver", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({
        rows: [
          {
            candidate_id: "c1",
            election_id: "e1",
            candidate_name: "Jane Example",
            election_year: 2026,
            election_date: "2026-11-03T00:00:00.000Z",
            office_scope: "statewide",
            office_name: "Attorney General",
            district: null,
            committee_id: "01009999",
            committee_name: "Jane Example for Delaware",
            link_source: "cfrs_portal",
            source_url: null,
            last_synced_at: null,
            total_due_rows: "4",
          },
        ],
        rowCount: 1,
      }),
    };
    const result = await listDueDelawareCandidateFinanceSyncRows(db, {
      now: new Date("2026-08-28T00:00:00.000Z"),
      staleAfterDays: 7,
      maxCandidates: 10,
      electionLookbackDays: 39,
      electionLookaheadDays: 730,
    });
    expect(result.totalDueRows).toBe(4);
    expect(result.rows[0]).toMatchObject({
      candidateId: "c1",
      electionDate: "2026-11-03",
      cfId: "01009999",
      linkSource: "cfrs_portal",
      lastSyncedAt: null,
    });
    const sql = String(db.query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM public.de_candidate_finance_links link");
    expect(sql).toContain("summary.last_synced_at IS NULL OR summary.last_synced_at <");
    expect(sql).toContain("district_row.state='DE'");
    expect(db.query.mock.calls[0]?.[1]?.[5]).toEqual([...DELAWARE_FINANCE_ELIGIBLE_OFFICE_KEYS]);
  });
});
