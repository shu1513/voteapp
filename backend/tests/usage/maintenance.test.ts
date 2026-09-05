import { describe, expect, it, vi } from "vitest";
import type { Pool } from "pg";

import type { MaintenanceRedis } from "../../src/chatbot/maintenance.js";
import { maybeRunUsageRetention } from "../../src/usage/maintenance.js";

function fakeDb(): { pool: Pool; queries: string[] } {
  const queries: string[] = [];
  const pool = {
    query: async (text: string) => {
      queries.push(text);
      return { rows: [{ purge_events: 7 }], rowCount: 1 };
    },
  } as unknown as Pool;
  return { pool, queries };
}

function fakeRedis(): { redis: MaintenanceRedis; setCalls: unknown[][]; delCalls: string[] } {
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
  return { redis, setCalls, delCalls };
}

const NOW = new Date(Date.UTC(2026, 8, 4, 12, 0));

describe("maybeRunUsageRetention", () => {
  it("purges once per UTC day under its own guard key", async () => {
    const log = vi.spyOn(console, "log").mockImplementation(() => undefined);
    try {
      const { pool, queries } = fakeDb();
      const { redis, setCalls } = fakeRedis();
      await expect(maybeRunUsageRetention(pool, redis, NOW)).resolves.toBe("ran");
      expect(setCalls).toEqual([["usage:purge:2026-09-04", "1", { NX: true, EX: 26 * 3600 }]]);
      expect(queries).toEqual([`SELECT usage.purge_events()`]);
      await expect(maybeRunUsageRetention(pool, redis, NOW)).resolves.toBe("skipped_already_ran");
      expect(queries).toHaveLength(1);
    } finally {
      log.mockRestore();
    }
  });

  it("skips without Redis and never throws on a DB failure", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      const { pool, queries } = fakeDb();
      await expect(maybeRunUsageRetention(pool, null, NOW)).resolves.toBe("skipped_no_redis");
      expect(queries).toEqual([]);

      const { redis, delCalls } = fakeRedis();
      const failing = {
        query: async () => {
          throw new Error("db down");
        },
      } as unknown as Pool;
      await expect(maybeRunUsageRetention(failing, redis, NOW)).resolves.toBe("failed");
      expect(delCalls).toEqual(["usage:purge:2026-09-04"]);
      expect(warn).toHaveBeenCalledOnce();
    } finally {
      warn.mockRestore();
    }
  });
});
