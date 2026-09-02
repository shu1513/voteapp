import { describe, expect, it, vi } from "vitest";

import { listDueKansasCandidateFinanceSyncRows } from "../../../src/pipeline/kansasFinance/kansasCandidateFinanceDueList.js";
import { KANSAS_FINANCE_ELIGIBLE_OFFICE_KEYS } from "../../../src/pipeline/kansasFinance/kansasFinanceEligibleOffices.js";

describe("listDueKansasCandidateFinanceSyncRows", () => {
  it("queries active Kansas links against the ks_* tables for eligible offices", async () => {
    const db = { query: vi.fn().mockResolvedValue({ rows: [] }) };
    const result = await listDueKansasCandidateFinanceSyncRows(db, {
      now: new Date("2026-09-02T12:00:00.000Z"),
      staleAfterDays: 0,
      maxCandidates: 25,
      electionLookbackDays: 98,
      electionLookaheadDays: 730,
    });
    expect(result).toEqual({ rows: [], totalDueRows: 0 });
    const [sql, params] = db.query.mock.calls[0]!;
    expect(String(sql)).toContain("public.ks_candidate_finance_links");
    expect(String(sql)).toContain("public.ks_candidate_finance_summaries");
    expect(String(sql)).toContain("district.state = 'KS'");
    expect(params).toEqual(["2026-09-02T12:00:00.000Z", 0, 25, 98, 730, [...KANSAS_FINANCE_ELIGIBLE_OFFICE_KEYS]]);
  });
});
