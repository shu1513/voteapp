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
  it("counts without deleting on dry run", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [{ matched: "12" }], rowCount: 1 });

    await expect(
      pruneNotificationEvents({ query } as never, { olderThanDays: 90, live: false, batchSize: 1000 })
    ).resolves.toEqual({ matchedCount: 12, deletedCount: 0 });

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("SELECT count(*)");
    expect(sql).not.toContain("DELETE");
    expect(query.mock.calls[0]?.[1]).toEqual([90]);
  });

  it("deletes in batches until a short batch on live runs", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [], rowCount: 2 })
      .mockResolvedValueOnce({ rows: [], rowCount: 2 })
      .mockResolvedValueOnce({ rows: [], rowCount: 1 });

    await expect(
      pruneNotificationEvents({ query } as never, { olderThanDays: 30, live: true, batchSize: 2 })
    ).resolves.toEqual({ matchedCount: 5, deletedCount: 5 });

    expect(query).toHaveBeenCalledTimes(3);
    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("DELETE FROM public.user_candidate_follow_notification_events");
    expect(sql).toContain("created_at < now() - make_interval(days => $1::int)");
    expect(sql).toContain("LIMIT $2::int");
    expect(query.mock.calls[0]?.[1]).toEqual([30, 2]);
  });

  it("stops after one batch when fewer rows than the batch size match", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 0 });

    await expect(
      pruneNotificationEvents({ query } as never, { olderThanDays: 30, live: true, batchSize: 1000 })
    ).resolves.toEqual({ matchedCount: 0, deletedCount: 0 });
    expect(query).toHaveBeenCalledTimes(1);
  });
});
