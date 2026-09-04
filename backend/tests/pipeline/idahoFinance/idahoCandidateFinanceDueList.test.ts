import { describe, expect, it, vi } from "vitest";

import { listDueIdahoCandidateFinanceSyncRows } from "../../../src/pipeline/idahoFinance/idahoCandidateFinanceDueList.js";
import { IDAHO_FINANCE_ELIGIBLE_OFFICE_KEYS } from "../../../src/pipeline/idahoFinance/idahoFinanceEligibleOffices.js";
import { GUID_A } from "./idahoTestFixtures.js";

const INPUT = {
  now: new Date("2026-09-03T00:00:00.000Z"),
  staleAfterDays: 7,
  maxCandidates: 25,
  electionLookbackDays: 98,
  electionLookaheadDays: 730,
};

describe("listDueIdahoCandidateFinanceSyncRows", () => {
  it("queries active general-election links with staleness gating and maps rows", async () => {
    const db = {
      query: vi.fn().mockResolvedValue({
        rows: [
          {
            candidate_id: "candidate-1",
            election_id: "election-1",
            candidate_name: "Todd Achilles",
            election_year: 2026,
            election_date: "2026-11-03T00:00:00.000Z",
            office_scope: "state_upper",
            office_name: "State Senator",
            district: "16",
            registration_guid: GUID_A.toUpperCase(),
            filer_name: "Achilles, Todd Baker",
            link_source: "sunshine_grid",
            source_url: null,
            last_synced_at: null,
            total_due_rows: "3",
          },
        ],
      }),
    };

    const result = await listDueIdahoCandidateFinanceSyncRows(db, INPUT);

    const [sql, params] = db.query.mock.calls[0]!;
    expect(String(sql)).toContain("id_candidate_finance_links");
    expect(String(sql)).toContain("id_candidate_finance_summaries");
    expect(String(sql)).toContain("election.election_stage='general'");
    expect(String(sql)).toContain("district_row.state='ID'");
    expect(String(sql)).toContain("link.registration_guid");
    expect(params).toEqual(["2026-09-03T00:00:00.000Z", 7, 25, 98, 730, [...IDAHO_FINANCE_ELIGIBLE_OFFICE_KEYS]]);

    expect(result.totalDueRows).toBe(3);
    expect(result.rows).toEqual([
      {
        candidateId: "candidate-1",
        electionId: "election-1",
        candidateName: "Todd Achilles",
        electionYear: 2026,
        electionDate: "2026-11-03",
        officeScope: "state_upper",
        officeName: "State Senator",
        district: "16",
        registrationGuid: GUID_A,
        filerName: "Achilles, Todd Baker",
        linkSource: "sunshine_grid",
        sourceUrl: null,
        lastSyncedAt: null,
      },
    ]);
  });

  it("rejects a corrupted stored registration guid instead of syncing the wrong registration", async () => {
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
            registration_guid: "not-a-guid",
            filer_name: "Y",
            link_source: "manual",
            source_url: null,
            last_synced_at: null,
            total_due_rows: 1,
          },
        ],
      }),
    };
    await expect(listDueIdahoCandidateFinanceSyncRows(db, INPUT)).rejects.toThrow("Invalid Idaho registration guid");
  });

  it("returns zero totals for an empty due list", async () => {
    const db = { query: vi.fn().mockResolvedValue({ rows: [] }) };
    expect(await listDueIdahoCandidateFinanceSyncRows(db, INPUT)).toEqual({ rows: [], totalDueRows: 0 });
  });
});
