import { describe, expect, it, vi } from "vitest";

import { listCandidateRostersDue } from "../../src/scripts/listCandidateRostersDue.js";

function createMockQueryable(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows }),
  };
}

describe("listCandidateRostersDue", () => {
  it("passes the as-of date, cooldown, window, and item type as parameters and returns the rows", async () => {
    const row = {
      election_id: "e-1",
      district_name: "Denver County",
      district_type: "county",
      state: "CO",
      official_ballot_title: "County Commissioner",
      election_date: "2026-08-11",
      election_stage: "general",
      roster_status: "written",
      roster_written_at: "2026-05-01 12:00:00+00",
      linked_candidate_count: 3,
      reason: "stale",
    };
    const db = createMockQueryable([row]);

    const result = await listCandidateRostersDue(db, {
      asOfDate: "2026-07-16",
      cooldownDays: 30,
      withinDays: 90,
    });

    expect(result).toEqual([row]);
    expect(db.query).toHaveBeenCalledTimes(1);
    const [sql, params] = db.query.mock.calls[0]!;
    expect(params).toEqual(["2026-07-16", 30, 90, "candidate_roster"]);
    // The refresh loop covers only rosters a research pass already resolved;
    // pending/failed rosters belong to the initial research flow.
    expect(sql).toContain("s.status IN ('written', 'no_results')");
    expect(sql).toContain("race_type = 'office'");
    // Upcoming elections only, capped to the lookahead window — rosters for
    // far-future elections change too rarely to be worth refreshing.
    expect(sql).toContain("e.election_date >= $1::date");
    expect(sql).toContain("<= $3::int");
    // Cooldown cutoff pinned to UTC so the boundary does not drift with the
    // session TimeZone (written_at is timestamptz).
    expect(sql).toContain("AT TIME ZONE 'UTC'");
    // Zero-candidate written rosters (pre-fix data debt) are due regardless
    // of age, as are no_results rosters.
    expect(sql).toContain("linked.linked_candidate_count = 0");
    expect(sql).toContain("s.status = 'no_results'");
    // Linked-candidate count must ignore deleted candidates.
    expect(sql).toContain("c.deleted_at IS NULL");
  });

  it("orders soonest election first, oldest roster first within a date", async () => {
    const db = createMockQueryable();
    await listCandidateRostersDue(db, { asOfDate: "2026-07-16", cooldownDays: 30, withinDays: 90 });
    const [sql] = db.query.mock.calls[0]!;
    expect(sql).toContain("ORDER BY e.election_date ASC, s.written_at ASC NULLS FIRST, e.id ASC");
  });
});
