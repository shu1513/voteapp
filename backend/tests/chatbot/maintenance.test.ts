import { describe, expect, it, vi } from "vitest";
import type { Pool } from "pg";

import { maybeRunQuestionRetention, type MaintenanceRedis } from "../../src/chatbot/maintenance.js";

function fakeDb(): { pool: Pool; queries: string[] } {
  const queries: string[] = [];
  const pool = {
    query: async (text: string) => {
      queries.push(text);
      return { rows: [{ stats_rows_written: 2, purged_question_texts: 3 }], rowCount: 1 };
    },
  } as unknown as Pool;
  return { pool, queries };
}

/** Scripted Redis: SET NX succeeds while the guard key is absent, records
 * deletes. Mirrors the real key lifecycle so the retry test is honest. */
function fakeRedis(): { redis: MaintenanceRedis; keys: Set<string>; setCalls: unknown[][]; delCalls: string[] } {
  const keys = new Set<string>();
  const setCalls: unknown[][] = [];
  const delCalls: string[] = [];
  const redis: MaintenanceRedis = {
    set: async (key, value, options) => {
      setCalls.push([key, value, options]);
      if (keys.has(key)) {
        return null;
      }
      keys.add(key);
      return "OK";
    },
    del: async (key) => {
      delCalls.push(key);
      keys.delete(key);
      return 1;
    },
  };
  return { redis, keys, setCalls, delCalls };
}

const NOW = new Date(Date.UTC(2026, 7, 17, 12, 0));

describe("maybeRunQuestionRetention", () => {
  it("runs retention when it wins the day's SET NX election", async () => {
    const { pool, queries } = fakeDb();
    const { redis, setCalls } = fakeRedis();
    await expect(maybeRunQuestionRetention(pool, redis, NOW)).resolves.toBe("ran");
    expect(setCalls).toEqual([["chatbot:purge:2026-08-17", "1", { NX: true, EX: 26 * 3600 }]]);
    expect(queries).toEqual([
      `SELECT stats_rows_written, purged_question_texts FROM chatbot.roll_up_and_purge_questions()`,
    ]);
  });

  it("does not touch the database when another process already ran today", async () => {
    const { pool, queries } = fakeDb();
    const { redis } = fakeRedis();
    await maybeRunQuestionRetention(pool, redis, NOW);
    queries.length = 0;
    await expect(maybeRunQuestionRetention(pool, redis, NOW)).resolves.toBe("skipped_already_ran");
    expect(queries).toEqual([]);
  });

  it("skips without Redis (manual report run still covers retention)", async () => {
    const { pool, queries } = fakeDb();
    await expect(maybeRunQuestionRetention(pool, null, NOW)).resolves.toBe("skipped_no_redis");
    expect(queries).toEqual([]);
  });

  it("never throws: a Redis failure is a warning, not a failed ask", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      const { pool, queries } = fakeDb();
      const redis: MaintenanceRedis = {
        set: async () => {
          throw new Error("redis down");
        },
        del: async () => 0,
      };
      await expect(maybeRunQuestionRetention(pool, redis, NOW)).resolves.toBe("failed");
      expect(queries).toEqual([]);
      expect(warn).toHaveBeenCalledOnce();
    } finally {
      warn.mockRestore();
    }
  });

  it("releases the day's guard on a DB failure so a later tick can retry", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      const { redis, delCalls } = fakeRedis();
      let failuresLeft = 1;
      const queries: string[] = [];
      const pool = {
        query: async (text: string) => {
          if (failuresLeft > 0) {
            failuresLeft -= 1;
            throw new Error("db down");
          }
          queries.push(text);
          return { rows: [{ stats_rows_written: 0, purged_question_texts: 1 }], rowCount: 1 };
        },
      } as unknown as Pool;
      // First attempt wins the election, fails on the DB, and gives the day
      // back; the next hourly tick must be able to run retention for real.
      await expect(maybeRunQuestionRetention(pool, redis, NOW)).resolves.toBe("failed");
      expect(delCalls).toEqual(["chatbot:purge:2026-08-17"]);
      await expect(maybeRunQuestionRetention(pool, redis, NOW)).resolves.toBe("ran");
      expect(queries).toHaveLength(1);
    } finally {
      warn.mockRestore();
    }
  });
});
