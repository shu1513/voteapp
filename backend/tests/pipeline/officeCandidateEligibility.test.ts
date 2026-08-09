import { describe, expect, it, vi } from "vitest";
import type { Pool } from "pg";

import {
  defaultOfficeCandidateEligibilityConfig,
  evaluateOfficeCandidateEligibilityByElectionIds,
  getOfficeCandidateEligibilityForElectionId,
  listOfficeCandidateEligibilityForUpcomingOffices,
  summarizeOfficeCandidateEligibilityReasons,
  type OfficeCandidateEligibilityRow,
} from "../../src/pipeline/candidates/officeCandidateEligibility.js";

describe("officeCandidateEligibility", () => {
  it("uses expected default config values", () => {
    const config = defaultOfficeCandidateEligibilityConfig();
    expect(config.defaultBufferDays).toBe(7);
    expect(config.shortStageGapDays).toBe(60);
    expect(config.shortStageBufferDays).toBe(3);
    expect(config.asOfDate).toMatch(/^\d{4}-\d{2}-\d{2}$/);
  });

  it("dedupes election ids before query", async () => {
    const query = vi.fn(async () => ({ rows: [] }));
    const pool = { query } as unknown as Pool;
    const config = defaultOfficeCandidateEligibilityConfig();

    await evaluateOfficeCandidateEligibilityByElectionIds(
      pool,
      ["id-1", "id-2", "id-1"],
      config
    );

    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0]?.[1]?.[0]).toEqual(["id-1", "id-2"]);
  });

  it("treats no_results rosters as complete so the rollover producer never auto-retries them", async () => {
    const query = vi.fn(async () => ({ rows: [] }));
    const pool = { query } as unknown as Pool;
    const config = defaultOfficeCandidateEligibilityConfig();

    await evaluateOfficeCandidateEligibilityByElectionIds(pool, ["id-1"], config);

    const [sql] = query.mock.calls[0]!;
    // A roster pass that found nobody ('no_results') must block automated
    // re-research the same way 'written' does — refresh is manual, via
    // manual:candidate-roster:due + manual:candidate-roster:inject.
    expect(sql).toContain("s.status IN ('written', 'no_results')");
    expect(sql).not.toContain("s.status = 'written'");
  });

  it("rules out office-less shells in both the by-ids and daily-rollover selectors", async () => {
    const query = vi.fn(async () => ({ rows: [] }));
    const pool = { query } as unknown as Pool;
    const config = defaultOfficeCandidateEligibilityConfig();

    // The elections writer holds office-less shells back from its own handoff;
    // the daily rollover would otherwise pick the same shell up the next day
    // and enqueue a roster the records stage cannot finish.
    await evaluateOfficeCandidateEligibilityByElectionIds(pool, ["id-1"], config);
    const [byIdsSql] = query.mock.calls[0]!;
    expect(byIdsSql).toContain("b.office_id IS NULL THEN 'not_office_or_missing'");

    await listOfficeCandidateEligibilityForUpcomingOffices(pool, config);
    const [upcomingSql] = query.mock.calls[1]!;
    expect(upcomingSql).toContain("WHEN b.office_id IS NULL THEN 'not_office_or_missing'");
  });

  it("returns not_office_or_missing fallback when selector has no row", async () => {
    const query = vi.fn(async () => ({ rows: [] }));
    const pool = { query } as unknown as Pool;
    const config = defaultOfficeCandidateEligibilityConfig();

    const row = await getOfficeCandidateEligibilityForElectionId(pool, "missing-id", config);
    expect(row.reason).toBe("not_office_or_missing");
    expect(row.election_id).toBe("missing-id");
  });

  it("summarizes eligibility reasons", () => {
    const rows: OfficeCandidateEligibilityRow[] = [
      {
        election_id: "1",
        reason: "eligible",
        prior_election_date: null,
        stage_gap_days: null,
        buffer_days: 7,
        eligible_after_date: null,
      },
      {
        election_id: "2",
        reason: "not_nearest_in_track",
        prior_election_date: "2026-06-02",
        stage_gap_days: 154,
        buffer_days: 7,
        eligible_after_date: "2026-06-09",
      },
      {
        election_id: "3",
        reason: "buffer_not_elapsed",
        prior_election_date: "2026-09-15",
        stage_gap_days: 49,
        buffer_days: 3,
        eligible_after_date: "2026-09-18",
      },
      {
        election_id: "4",
        reason: "too_far_in_future",
        prior_election_date: null,
        stage_gap_days: null,
        buffer_days: 7,
        eligible_after_date: null,
      },
    ];

    const summary = summarizeOfficeCandidateEligibilityReasons(rows);
    expect(summary.eligible).toBe(1);
    expect(summary.not_nearest_in_track).toBe(1);
    expect(summary.buffer_not_elapsed).toBe(1);
    expect(summary.too_far_in_future).toBe(1);
  });
});
