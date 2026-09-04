import { describe, expect, it, vi } from "vitest";

import { listDueKansasCandidateFinanceSyncRows } from "../../../src/pipeline/kansasFinance/kansasCandidateFinanceDueList.js";
import { KANSAS_FINANCE_ELIGIBLE_OFFICE_KEYS } from "../../../src/pipeline/kansasFinance/kansasFinanceEligibleOffices.js";

describe("listDueKansasCandidateFinanceSyncRows", () => {
  it("queries active Kansas links against the ks_* tables for eligible offices and carries link_source", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({
        rows: [
          {
            candidate_id: "11111111-1111-4111-8111-111111111111",
            election_id: "22222222-2222-4222-8222-222222222222",
            candidate_name: "Margaret Holloway",
            election_year: 2026,
            office_scope: "state_lower",
            office_name: "State Lower Chamber Legislator",
            district: "85",
            committee_id: "7:85:HOLLOWAY:MARGARET",
            committee_name: "HOLLOWAY MARGARET",
            link_source: "manual",
            source_url: null,
            last_synced_at: null,
            total_due_rows: "3",
          },
        ],
      }),
    };
    const result = await listDueKansasCandidateFinanceSyncRows(db, {
      now: new Date("2026-09-02T12:00:00.000Z"),
      staleAfterDays: 0,
      maxCandidates: 25,
      electionLookbackDays: 98,
      electionLookaheadDays: 730,
    });
    expect(result).toEqual({
      totalDueRows: 3,
      rows: [
        {
          candidateId: "11111111-1111-4111-8111-111111111111",
          electionId: "22222222-2222-4222-8222-222222222222",
          candidateName: "Margaret Holloway",
          electionYear: 2026,
          officeScope: "state_lower",
          officeName: "State Lower Chamber Legislator",
          district: "85",
          committeeId: "7:85:HOLLOWAY:MARGARET",
          committeeName: "HOLLOWAY MARGARET",
          linkSource: "manual",
          sourceUrl: null,
          lastSyncedAt: null,
        },
      ],
    });
    const [sql, params] = db.query.mock.calls[0]!;
    expect(String(sql)).toContain("public.ks_candidate_finance_links");
    expect(String(sql)).toContain("public.ks_candidate_finance_summaries");
    expect(String(sql)).toContain("district.state = 'KS'");
    expect(String(sql)).toContain("link.link_source");
    expect(params).toEqual(["2026-09-02T12:00:00.000Z", 0, 25, 98, 730, [...KANSAS_FINANCE_ELIGIBLE_OFFICE_KEYS]]);
  });
});
