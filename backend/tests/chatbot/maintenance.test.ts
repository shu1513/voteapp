import { describe, expect, it, vi } from "vitest";
import type { Pool } from "pg";

import { maybePurgeQuestionText, type MaintenanceRedis } from "../../src/chatbot/maintenance.js";

function fakeDb(): { pool: Pool; queries: string[] } {
  const queries: string[] = [];
  const pool = {
    query: async (text: string) => {
      queries.push(text);
      return { rows: [{ purged: 3 }], rowCount: 1 };
    },
  } as unknown as Pool;
  return { pool, queries };
}

const NOW = new Date(Date.UTC(2026, 7, 17, 12, 0));

describe("maybePurgeQuestionText", () => {
  it("purges when it wins the day's SET NX election", async () => {
    const { pool, queries } = fakeDb();
    const setCalls: unknown[][] = [];
    const redis: MaintenanceRedis = {
      set: async (...args) => {
        setCalls.push(args);
        return "OK";
      },
    };
    await expect(maybePurgeQuestionText(pool, redis, NOW)).resolves.toBe("purged");
    expect(setCalls).toEqual([["chatbot:purge:2026-08-17", "1", { NX: true, EX: 26 * 3600 }]]);
    expect(queries).toEqual([`SELECT chatbot.purge_question_text() AS purged`]);
  });

  it("does not touch the database when another process already ran today", async () => {
    const { pool, queries } = fakeDb();
    const redis: MaintenanceRedis = { set: async () => null };
    await expect(maybePurgeQuestionText(pool, redis, NOW)).resolves.toBe("skipped_already_ran");
    expect(queries).toEqual([]);
  });

  it("skips without Redis (manual report purge still covers retention)", async () => {
    const { pool, queries } = fakeDb();
    await expect(maybePurgeQuestionText(pool, null, NOW)).resolves.toBe("skipped_no_redis");
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
      };
      await expect(maybePurgeQuestionText(pool, redis, NOW)).resolves.toBe("failed");
      expect(queries).toEqual([]);
      expect(warn).toHaveBeenCalledOnce();
    } finally {
      warn.mockRestore();
    }
  });

  it("never throws: a DB failure after winning the election is a warning", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      const pool = {
        query: async () => {
          throw new Error("db down");
        },
      } as unknown as Pool;
      const redis: MaintenanceRedis = { set: async () => "OK" };
      await expect(maybePurgeQuestionText(pool, redis, NOW)).resolves.toBe("failed");
      expect(warn).toHaveBeenCalledOnce();
    } finally {
      warn.mockRestore();
    }
  });
});
