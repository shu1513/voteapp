import { describe, expect, it, vi } from "vitest";

import {
  listFollowedCandidateRecordsDue,
  listFollowedCandidatesWithoutOfficeElection,
  readCooldownDaysDefault,
} from "../../src/scripts/listFollowedCandidateRecordsDue.js";

function createMockQueryable(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows }),
  };
}

describe("listFollowedCandidateRecordsDue", () => {
  it("passes the as-of date and cooldown as parameters and returns the rows", async () => {
    const row = {
      candidate_id: "c-1",
      display_name: "Jane Doe",
      state: "CA",
      party: "Independent",
      current_office: null,
      follower_count: 2,
      last_records_searched_at: null,
      last_records_researched_through: null,
      election_id: "e-1",
      official_ballot_title: "Governor",
      election_date: "2026-11-03",
    };
    const db = createMockQueryable([row]);

    const result = await listFollowedCandidateRecordsDue(db, {
      asOfDate: "2026-07-12",
      cooldownDays: 30,
    });

    expect(result).toEqual([row]);
    expect(db.query).toHaveBeenCalledTimes(1);
    const [sql, params] = db.query.mock.calls[0]!;
    expect(params).toEqual(["2026-07-12", 30]);
    expect(sql).toContain("user_candidate_follows");
    // Any follower qualifies: the query must not gate on the follow's email
    // notification toggles (the in-app new-records surface serves
    // non-subscribers too).
    expect(sql).not.toContain("notify_updates");
    expect(sql).not.toContain("notify_elections");
    expect(sql).toContain("last_records_searched_at IS NULL");
    expect(sql).toContain("race_type = 'office'");
    expect(sql).toContain("merged_into_candidate_id IS NULL");
  });

  it("orders the due list most-overdue first (never-searched candidates lead)", async () => {
    const db = createMockQueryable();
    await listFollowedCandidateRecordsDue(db, { asOfDate: "2026-07-12", cooldownDays: 30 });
    const [sql] = db.query.mock.calls[0]!;
    expect(sql).toContain("ORDER BY fc.last_records_searched_at ASC NULLS FIRST");
  });
});

describe("listFollowedCandidatesWithoutOfficeElection", () => {
  it("lists followed candidates that lack any office election link", async () => {
    const row = {
      candidate_id: "c-2",
      display_name: "John Roe",
      state: "TX",
      follower_count: 1,
      last_records_searched_at: null,
    };
    const db = createMockQueryable([row]);

    const result = await listFollowedCandidatesWithoutOfficeElection(db);

    expect(result).toEqual([row]);
    const [sql] = db.query.mock.calls[0]!;
    expect(sql).toContain("NOT EXISTS");
    expect(sql).toContain("race_type = 'office'");
  });
});

describe("readCooldownDaysDefault", () => {
  it("defaults to 30 when the env var is unset or blank", () => {
    expect(readCooldownDaysDefault({})).toBe(30);
    expect(readCooldownDaysDefault({ CANDIDATE_RECORDS_SEARCH_COOLDOWN_DAYS: "  " })).toBe(30);
  });

  it("reads a positive integer from the env var", () => {
    expect(readCooldownDaysDefault({ CANDIDATE_RECORDS_SEARCH_COOLDOWN_DAYS: "45" })).toBe(45);
  });

  it("rejects non-positive or malformed values", () => {
    expect(() => readCooldownDaysDefault({ CANDIDATE_RECORDS_SEARCH_COOLDOWN_DAYS: "0" })).toThrow(
      /Invalid CANDIDATE_RECORDS_SEARCH_COOLDOWN_DAYS/
    );
    expect(() => readCooldownDaysDefault({ CANDIDATE_RECORDS_SEARCH_COOLDOWN_DAYS: "30d" })).toThrow(
      /Invalid CANDIDATE_RECORDS_SEARCH_COOLDOWN_DAYS/
    );
  });
});
