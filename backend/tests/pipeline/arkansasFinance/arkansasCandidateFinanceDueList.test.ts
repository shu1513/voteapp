import { describe, expect, it, vi } from "vitest";

import { listDueArkansasCandidateFinanceSyncRows } from "../../../src/pipeline/arkansasFinance/arkansasCandidateFinanceDueList.js";
import { ARKANSAS_FINANCE_ELIGIBLE_OFFICE_KEYS } from "../../../src/pipeline/arkansasFinance/arkansasFinanceEligibleOffices.js";

const NOW = new Date("2026-09-02T12:00:00Z");

describe("listDueArkansasCandidateFinanceSyncRows", () => {
  it("queries the ar_ tables for stale active links and maps the CFIS identity", async () => {
    const query = vi.fn(async () => ({
      rows: [
        {
          candidate_id: "11111111-1111-4111-8111-111111111111",
          election_id: "22222222-2222-4222-8222-222222222222",
          candidate_name: "Jane Doe",
          election_year: 2026,
          office_scope: "state_lower",
          office_name: "State Lower Chamber Legislator",
          district: "State House District 59",
          filing_entity_id: "7968",
          filer_name: "Doe, Jane A.",
          link_source: "manual",
          source_url: null,
          last_synced_at: "2026-08-01T00:00:00.000Z",
          total_due_rows: "3",
        },
      ],
    }));

    const result = await listDueArkansasCandidateFinanceSyncRows({ query }, {
      now: NOW,
      staleAfterDays: 7,
      maxCandidates: 10,
      electionLookbackDays: 38,
      electionLookaheadDays: 730,
    });

    const [sql, params] = query.mock.calls[0] as unknown as [string, unknown[]];
    expect(sql).toContain("FROM public.ar_candidate_finance_links AS link");
    expect(sql).toContain("LEFT JOIN public.ar_candidate_finance_summaries AS summary");
    expect(sql).toContain("district.state = 'AR'");
    expect(sql).toContain("link.filing_entity_id,");
    expect(sql).toContain("link.link_source,");
    expect(params).toEqual([NOW.toISOString(), 7, 10, 38, 730, [...ARKANSAS_FINANCE_ELIGIBLE_OFFICE_KEYS]]);
    expect(result).toEqual({
      totalDueRows: 3,
      rows: [
        {
          candidateId: "11111111-1111-4111-8111-111111111111",
          electionId: "22222222-2222-4222-8222-222222222222",
          candidateName: "Jane Doe",
          electionYear: 2026,
          officeScope: "state_lower",
          officeName: "State Lower Chamber Legislator",
          district: "State House District 59",
          filingEntityId: 7968,
          filerName: "Doe, Jane A.",
          linkSource: "manual",
          sourceUrl: null,
          lastSyncedAt: "2026-08-01T00:00:00.000Z",
        },
      ],
    });
  });

  it("rejects a malformed stored filing entity ID", async () => {
    const query = vi.fn(async () => ({
      rows: [{ candidate_id: "a", election_id: "b", filing_entity_id: "0", total_due_rows: "1" }],
    }));
    await expect(
      listDueArkansasCandidateFinanceSyncRows({ query }, {
        now: NOW,
        staleAfterDays: 7,
        maxCandidates: 10,
        electionLookbackDays: 38,
        electionLookaheadDays: 730,
      })
    ).rejects.toThrow(/Invalid Arkansas filing entity ID in due list: 0/);
  });
});
