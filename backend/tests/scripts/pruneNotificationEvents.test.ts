import { describe, expect, it, vi } from "vitest";

import {
  DEFAULT_NOTIFICATION_EVENT_RETENTION_DAYS,
  parsePruneNotificationEventsArgs,
  pruneNotificationEvents,
} from "../../src/scripts/pruneNotificationEvents.js";

describe("parsePruneNotificationEventsArgs", () => {
  it("defaults to a 90-day dry run", () => {
    expect(parsePruneNotificationEventsArgs([])).toEqual({
      olderThanDays: DEFAULT_NOTIFICATION_EVENT_RETENTION_DAYS,
      live: false,
    });
  });

  it("parses --live and --older-than-days in both flag forms", () => {
    expect(parsePruneNotificationEventsArgs(["--live", "--older-than-days", "30"])).toEqual({
      olderThanDays: 30,
      live: true,
    });
    expect(parsePruneNotificationEventsArgs(["--older-than-days=180"])).toEqual({
      olderThanDays: 180,
      live: false,
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
});

describe("pruneNotificationEvents", () => {
  it("counts without deleting on dry run", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [{ matched: "12" }], rowCount: 1 });

    await expect(
      pruneNotificationEvents({ query } as never, { olderThanDays: 90, live: false })
    ).resolves.toEqual({ matchedCount: 12, deletedCount: 0 });

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("SELECT count(*)");
    expect(sql).not.toContain("DELETE");
    expect(query.mock.calls[0]?.[1]).toEqual([90]);
  });

  it("deletes only rows older than the window on live runs", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 7 });

    await expect(
      pruneNotificationEvents({ query } as never, { olderThanDays: 30, live: true })
    ).resolves.toEqual({ matchedCount: 7, deletedCount: 7 });

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("DELETE FROM public.user_candidate_follow_notification_events");
    expect(sql).toContain("created_at < now() - make_interval(days => $1::int)");
    expect(query.mock.calls[0]?.[1]).toEqual([30]);
  });
});
