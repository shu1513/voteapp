import { describe, expect, it, vi } from "vitest";
import type { Pool } from "pg";

import {
  ANSWER_CACHE_TTL_SECONDS,
  answerCacheKey,
  consumeUserDailyAllowance,
  getCachedAnswer,
  hashUserId,
  reconcileDailyBudget,
  reserveDailyBudget,
  setCachedAnswer,
  utcDay,
  type LimitsRedis,
} from "../../src/chatbot/limits.js";

// ── Fakes ────────────────────────────────────────────────────────────────

function fakeRedis(): LimitsRedis & { store: Map<string, string>; counters: Map<string, number>; ttls: Map<string, number> } {
  const store = new Map<string, string>();
  const counters = new Map<string, number>();
  const ttls = new Map<string, number>();
  return {
    store,
    counters,
    ttls,
    async incr(key) {
      const next = (counters.get(key) ?? 0) + 1;
      counters.set(key, next);
      return next;
    },
    async expire(key, seconds) {
      ttls.set(key, seconds);
      return 1;
    },
    async get(key) {
      return store.get(key) ?? null;
    },
    async set(key, value, options) {
      store.set(key, value);
      if (options?.EX) {
        ttls.set(key, options.EX);
      }
      return "OK";
    },
  };
}

type FakeQuery = { text: string; values: unknown[] };

/** Pool fake for the reserve transaction: records queries, scripts the
 * UPDATE's rowCount. */
function fakePool(updateRowCount: number): { pool: Pool; queries: FakeQuery[]; released: () => boolean } {
  const queries: FakeQuery[] = [];
  let released = false;
  const client = {
    query: vi.fn(async (text: string, values?: unknown[]) => {
      queries.push({ text, values: values ?? [] });
      if (text.includes("UPDATE chatbot.daily_budget")) {
        return { rowCount: updateRowCount, rows: updateRowCount ? [{ tokens_reserved: "123" }] : [] };
      }
      return { rowCount: 0, rows: [] };
    }),
    release: () => {
      released = true;
    },
  };
  const pool = {
    connect: async () => client,
    query: client.query,
  } as unknown as Pool;
  return { pool, queries, released: () => released };
}

// ── Tests ────────────────────────────────────────────────────────────────

describe("hashUserId", () => {
  it("is deterministic per (user, secret) and never contains the raw id", () => {
    const id = "3d1f8a52-0000-4000-8000-000000000001";
    const a = hashUserId(id, "secret-a");
    expect(a).toBe(hashUserId(id, "secret-a"));
    expect(a).not.toBe(hashUserId(id, "secret-b"));
    expect(a).toMatch(/^[0-9a-f]{64}$/);
    expect(a).not.toContain(id);
  });
});

describe("consumeUserDailyAllowance", () => {
  it("allows up to the limit and refuses beyond it", async () => {
    const redis = fakeRedis();
    const now = new Date("2026-08-12T15:00:00Z");
    expect(await consumeUserDailyAllowance(redis, "hash", 2, now)).toBe(true);
    expect(await consumeUserDailyAllowance(redis, "hash", 2, now)).toBe(true);
    expect(await consumeUserDailyAllowance(redis, "hash", 2, now)).toBe(false);
  });

  it("keys per UTC day and per user hash, and sets a TTL once", async () => {
    const redis = fakeRedis();
    const now = new Date("2026-08-12T15:00:00Z");
    await consumeUserDailyAllowance(redis, "user-a", 5, now);
    await consumeUserDailyAllowance(redis, "user-a", 5, now);
    await consumeUserDailyAllowance(redis, "user-b", 5, now);
    const keys = [...redis.counters.keys()];
    expect(keys).toEqual(["chatbot:usercap:2026-08-12:user-a", "chatbot:usercap:2026-08-12:user-b"]);
    expect(redis.ttls.get("chatbot:usercap:2026-08-12:user-a")).toBeGreaterThan(0);
  });

  it("fails CLOSED (false) when Redis errors", async () => {
    const redis: LimitsRedis = {
      incr: async () => {
        throw new Error("redis down");
      },
      expire: async () => 1,
      get: async () => null,
      set: async () => "OK",
    };
    expect(await consumeUserDailyAllowance(redis, "hash", 5)).toBe(false);
  });
});

describe("reserveDailyBudget / reconcileDailyBudget", () => {
  it("runs the two-statement transaction and reserves when it fits", async () => {
    const { pool, queries, released } = fakePool(1);
    const now = new Date("2026-08-12T15:00:00Z");
    const reservation = await reserveDailyBudget(pool, 5_000, 1_000_000, now);
    expect(reservation).toEqual({ day: "2026-08-12", estimatedTokens: 5_000 });
    const texts = queries.map((q) => q.text.trim().split(/\s+/).slice(0, 2).join(" "));
    expect(texts).toEqual(["BEGIN", "INSERT INTO", "UPDATE chatbot.daily_budget", "COMMIT"]);
    // The INSERT guarantees the fresh-day row exists before the guarded UPDATE.
    expect(queries[1]?.text).toContain("ON CONFLICT (day) DO NOTHING");
    expect(queries[2]?.text).toContain("tokens_reserved + $2 <= $3");
    expect(released()).toBe(true);
  });

  it("returns null when the budget is exhausted (UPDATE matches no row)", async () => {
    const { pool, released } = fakePool(0);
    expect(await reserveDailyBudget(pool, 5_000, 1_000)).toBeNull();
    expect(released()).toBe(true);
  });

  it("fails CLOSED (null) on a database error", async () => {
    const pool = {
      connect: async () => ({
        query: vi.fn(async (text: string) => {
          if (text === "BEGIN") {
            return { rowCount: 0, rows: [] };
          }
          throw new Error("db down");
        }),
        release: () => undefined,
      }),
    } as unknown as Pool;
    expect(await reserveDailyBudget(pool, 5_000, 1_000_000)).toBeNull();
  });

  it("reconciles the estimate down to actual usage, floored at zero", async () => {
    const queries: FakeQuery[] = [];
    const pool = {
      query: vi.fn(async (text: string, values?: unknown[]) => {
        queries.push({ text, values: values ?? [] });
        return { rowCount: 1, rows: [] };
      }),
    } as unknown as Pool;
    await reconcileDailyBudget(pool, { day: "2026-08-12", estimatedTokens: 5_000 }, 1_200);
    expect(queries[0]?.text).toContain("GREATEST(0, tokens_reserved + ($3 - $2))");
    expect(queries[0]?.values).toEqual(["2026-08-12", 5_000, 1_200]);
  });
});

describe("answer cache", () => {
  const parts = {
    questionNorm: "who is jon ossoff",
    scopeKey: "GA|",
    generationId: "9b3a0d8e-0000-4000-8000-000000000001",
    model: "gpt-5.6-luna",
    promptVersion: "p1",
  };

  it("changes the key when any part changes", () => {
    const base = answerCacheKey(parts);
    expect(answerCacheKey({ ...parts, questionNorm: "who is mike collins" })).not.toBe(base);
    expect(answerCacheKey({ ...parts, scopeKey: "OH|" })).not.toBe(base);
    expect(answerCacheKey({ ...parts, generationId: "9b3a0d8e-0000-4000-8000-000000000002" })).not.toBe(base);
    expect(answerCacheKey({ ...parts, model: "other-model" })).not.toBe(base);
    expect(answerCacheKey({ ...parts, promptVersion: "p2" })).not.toBe(base);
    expect(answerCacheKey(parts)).toBe(base);
  });

  it("round-trips JSON with a 24h TTL and misses safely on garbage", async () => {
    const redis = fakeRedis();
    const key = answerCacheKey(parts);
    await setCachedAnswer(redis, key, { answer: "cached" });
    expect(await getCachedAnswer(redis, key)).toEqual({ answer: "cached" });
    expect(redis.ttls.get(key)).toBe(ANSWER_CACHE_TTL_SECONDS);
    redis.store.set(key, "{truncated");
    expect(await getCachedAnswer(redis, key)).toBeNull();
  });
});

describe("utcDay", () => {
  it("uses the UTC calendar date", () => {
    expect(utcDay(new Date("2026-08-12T23:59:59Z"))).toBe("2026-08-12");
    expect(utcDay(new Date("2026-08-13T00:00:01Z"))).toBe("2026-08-13");
  });
});
