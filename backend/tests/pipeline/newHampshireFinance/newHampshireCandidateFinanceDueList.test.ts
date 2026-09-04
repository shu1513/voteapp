import { describe, expect, it, vi } from "vitest";

import { listDueNewHampshireCandidateFinanceSyncRows } from "../../../src/pipeline/newHampshireFinance/newHampshireCandidateFinanceDueList.js";
import { NEW_HAMPSHIRE_FINANCE_ELIGIBLE_OFFICE_KEYS } from "../../../src/pipeline/newHampshireFinance/newHampshireFinanceEligibleOffices.js";

const INPUT = {
  now: new Date("2026-09-03T00:00:00.000Z"),
  staleAfterDays: 7,
  maxCandidates: 25,
  electionLookbackDays: 30,
  electionLookaheadDays: 730,
};

function dbRow(overrides: Record<string, unknown> = {}) {
  return {
    candidate_id: "candidate-1",
    election_id: "election-1",
    candidate_name: "Sample Candidate",
    election_year: 2026,
    election_date: "2026-11-03T00:00:00.000Z",
    office_scope: "state_upper",
    office_name: "State Senate",
    district: "1",
    filing_entity_id: "50450",
    filer_name: "Sample Candidate Committee",
    link_source: "cfs_registration",
    source_url: "https://cfs.sos.nh.gov/",
    last_synced_at: null,
    total_due_rows: "3",
    ...overrides,
  };
}

describe("listDueNewHampshireCandidateFinanceSyncRows", () => {
  it("queries active general-election links with staleness gating and maps rows", async () => {
    const db = { query: vi.fn().mockResolvedValue({ rows: [dbRow()] }) };

    const result = await listDueNewHampshireCandidateFinanceSyncRows(db, INPUT);

    const [sql, params] = db.query.mock.calls[0]!;
    expect(String(sql)).toContain("nh_candidate_finance_links");
    expect(String(sql)).toContain("nh_candidate_finance_summaries");
    expect(String(sql)).toContain("election.election_stage='general'");
    expect(String(sql)).toContain("district_row.state='NH'");
    expect(String(sql)).toContain("link.filing_entity_id");
    expect(String(sql)).toContain("summary.last_synced_at NULLS FIRST");
    expect(params).toEqual(["2026-09-03T00:00:00.000Z", 7, 25, 30, 730, [...NEW_HAMPSHIRE_FINANCE_ELIGIBLE_OFFICE_KEYS]]);

    expect(result.totalDueRows).toBe(3);
    expect(result.rows).toEqual([
      {
        candidateId: "candidate-1",
        electionId: "election-1",
        candidateName: "Sample Candidate",
        electionYear: 2026,
        electionDate: "2026-11-03",
        officeScope: "state_upper",
        officeName: "State Senate",
        district: "1",
        filingEntityId: 50_450,
        filerName: "Sample Candidate Committee",
        linkSource: "cfs_registration",
        sourceUrl: "https://cfs.sos.nh.gov/",
        lastSyncedAt: null,
      },
    ]);
  });

  it("rejects a corrupted stored filing entity ID instead of syncing the wrong filer", async () => {
    const db = { query: vi.fn().mockResolvedValue({ rows: [dbRow({ filing_entity_id: "0x50", total_due_rows: 1 })] }) };
    await expect(listDueNewHampshireCandidateFinanceSyncRows(db, INPUT)).rejects.toThrow(
      "Invalid New Hampshire filing entity ID: 0x50"
    );
  });

  it("returns zero totals for an empty due list", async () => {
    const db = { query: vi.fn().mockResolvedValue({ rows: [] }) };
    expect(await listDueNewHampshireCandidateFinanceSyncRows(db, INPUT)).toEqual({ rows: [], totalDueRows: 0 });
  });
});
