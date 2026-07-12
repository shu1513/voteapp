import { describe, expect, it, vi } from "vitest";

import {
  DEFAULT_NOTIFICATION_EVENT_PRUNE_BATCH_SIZE,
  DEFAULT_NOTIFICATION_EVENT_RETENTION_DAYS,
  parsePruneNotificationEventsArgs,
  pruneNotificationEvents,
} from "../../src/scripts/pruneNotificationEvents.js";

describe("parsePruneNotificationEventsArgs", () => {
  it("defaults to a 90-day dry run with the default batch size", () => {
    expect(parsePruneNotificationEventsArgs([])).toEqual({
      olderThanDays: DEFAULT_NOTIFICATION_EVENT_RETENTION_DAYS,
      live: false,
      batchSize: DEFAULT_NOTIFICATION_EVENT_PRUNE_BATCH_SIZE,
    });
  });

  it("parses --live, --older-than-days, and --batch-size in both flag forms", () => {
    expect(parsePruneNotificationEventsArgs(["--live", "--older-than-days", "30", "--batch-size", "500"])).toEqual({
      olderThanDays: 30,
      live: true,
      batchSize: 500,
    });
    expect(parsePruneNotificationEventsArgs(["--older-than-days=180", "--batch-size=2000"])).toEqual({
      olderThanDays: 180,
      live: false,
      batchSize: 2000,
    });
  });

  it("rejects non-positive or malformed day values", () => {
    expect(() => parsePruneNotificationEventsArgs(["--older-than-days", "0"])).toThrow(
      "--older-than-days must be a positive integer"
    );
    expect(() => parsePruneNotificationEventsArgs(["--older-than-days", "abc"])).toThrow(
      "--older-than-days must be a positive integer"
    );
  });

  it("rejects a trailing flag with no value instead of silently defaulting", () => {
    expect(() => parsePruneNotificationEventsArgs(["--older-than-days"])).toThrow(
      "--older-than-days requires a value"
    );
    expect(() => parsePruneNotificationEventsArgs(["--live", "--batch-size"])).toThrow(
      "--batch-size requires a value"
    );
  });
});

describe("pruneNotificationEvents", () => {
  it("counts all five tables without deleting on dry run", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [{ matched: "12" }], rowCount: 1 });

    await expect(
      pruneNotificationEvents({ query } as never, { olderThanDays: 90, live: false, batchSize: 1000 })
    ).resolves.toEqual({ matchedCount: 60, deletedCount: 0 });

    expect(query).toHaveBeenCalledTimes(5);
    const sqls = query.mock.calls.map((call) => String(call[0]));
    expect(sqls[0]).toContain("SELECT count(*)");
    expect(sqls[0]).toContain("user_candidate_follow_notification_events");
    // Candidate-follow events prune by age alone; district alerts must also
    // be delivered/resolved so a late-verifying user keeps a pending alert
    // for a still-future election.
    expect(sqls[0]).not.toContain("notified_at IS NOT NULL");
    expect(sqls[1]).toContain("user_district_notification_events");
    expect(sqls[1]).toContain("notified_at IS NOT NULL");
    // Reminder sends age by the election date itself, not row creation time.
    expect(sqls[2]).toContain("user_election_reminder_sends");
    expect(sqls[2]).toContain("election_date <");
    expect(sqls[2]).not.toContain("created_at");
    // Only long-revoked device tokens are prunable; active rows stay.
    expect(sqls[3]).toContain("user_push_tokens");
    expect(sqls[3]).toContain("revoked_at IS NOT NULL");
    expect(sqls[4]).toContain("user_push_notification_receipts");
    expect(sqls.join(" ")).not.toContain("DELETE");
    expect(query.mock.calls[0]?.[1]).toEqual([90]);
  });

  it("deletes in batches until a short batch on live runs, then moves to the next table", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [], rowCount: 2 })
      .mockResolvedValueOnce({ rows: [], rowCount: 2 })
      .mockResolvedValueOnce({ rows: [], rowCount: 1 })
      .mockResolvedValueOnce({ rows: [], rowCount: 1 }) // district table: single short batch
      .mockResolvedValueOnce({ rows: [], rowCount: 1 }) // reminder sends: single short batch
      .mockResolvedValueOnce({ rows: [], rowCount: 1 }) // push tokens: single short batch
      .mockResolvedValueOnce({ rows: [], rowCount: 1 }); // push receipts: single short batch

    await expect(
      pruneNotificationEvents({ query } as never, { olderThanDays: 30, live: true, batchSize: 2 })
    ).resolves.toEqual({ matchedCount: 9, deletedCount: 9 });

    expect(query).toHaveBeenCalledTimes(7);
    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("DELETE FROM public.user_candidate_follow_notification_events");
    expect(sql).toContain("created_at < now() - make_interval(days => $1::int)");
    expect(sql).toContain("id IN");
    expect(sql).toContain("LIMIT $2::int");
    expect(query.mock.calls[0]?.[1]).toEqual([30, 2]);
    const districtSql = String(query.mock.calls[3]?.[0]);
    expect(districtSql).toContain("DELETE FROM public.user_district_notification_events");
    expect(districtSql).toContain("notified_at IS NOT NULL");
    // No id column on the composite-key reminder table: batches key on ctid.
    const reminderSql = String(query.mock.calls[4]?.[0]);
    expect(reminderSql).toContain("DELETE FROM public.user_election_reminder_sends");
    expect(reminderSql).toContain("ctid IN");
    expect(reminderSql).toContain("election_date <");
    const pushTokensSql = String(query.mock.calls[5]?.[0]);
    expect(pushTokensSql).toContain("DELETE FROM public.user_push_tokens");
    expect(pushTokensSql).toContain("revoked_at IS NOT NULL");
    expect(pushTokensSql).toContain("id IN");
    // No id column on the receipt table: batches key on ctid.
    const receiptsSql = String(query.mock.calls[6]?.[0]);
    expect(receiptsSql).toContain("DELETE FROM public.user_push_notification_receipts");
    expect(receiptsSql).toContain("ctid IN");
  });

  it("stops after one batch per table when fewer rows than the batch size match", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 0 });

    await expect(
      pruneNotificationEvents({ query } as never, { olderThanDays: 30, live: true, batchSize: 1000 })
    ).resolves.toEqual({ matchedCount: 0, deletedCount: 0 });
    expect(query).toHaveBeenCalledTimes(5);
  });
});
