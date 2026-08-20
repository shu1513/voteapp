import { describe, expect, it, vi } from "vitest";

import { listDueMissouriCandidateFinanceSyncRows } from "../../../src/pipeline/missouriFinance/missouriCandidateFinanceDueList.js";

describe("listDueMissouriCandidateFinanceSyncRows", () => {
  it("returns exact election dates needed for cycle boundaries", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [{
      candidate_id: "c1", election_id: "e1", candidate_name: "Jane Doe", election_year: 2026,
      election_date: "2026-11-03 00:00:00+00", office_scope: "state_lower", office_name: "State Lower Chamber Legislator",
      district: "1", committee_id: "C263985", committee_name: "Jane for Missouri", link_source: "mec_portal",
      source_url: "https://example.test", last_synced_at: null, total_due_rows: "3",
    }] });
    const result = await listDueMissouriCandidateFinanceSyncRows({ query } as never, {
      now: new Date("2026-08-19T00:00:00Z"), staleAfterDays: 7, maxCandidates: 10,
      electionLookbackDays: 1, electionLookaheadDays: 730,
    });
    expect(result.totalDueRows).toBe(3);
    expect(result.rows[0]).toMatchObject({ electionDate: "2026-11-03", linkSource: "mec_portal" });
    expect(query.mock.calls[0]![0]).toContain("election.election_date::text election_date");
    expect(query.mock.calls[0]![0]).toContain("election.election_stage='general'");
    expect(query.mock.calls[0]![1]).toHaveLength(6);
    expect(query.mock.calls[0]![1][5]).toEqual(expect.arrayContaining([
      "state_lower::State Lower Chamber Legislator",
      "county::County Executive",
    ]));
    expect(query.mock.calls[0]![1][5]).not.toEqual(expect.arrayContaining([
      "place::City Council Member",
      "school_unified::School Board Member",
    ]));
  });
});
