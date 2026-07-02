import { describe, expect, it, vi } from "vitest";

import {
  AUTH_SESSION_KEY_PREFIX,
  buildAuthSessionKey,
  createAuthSession,
  destroyAuthSession,
  resolveAuthSessionUserId,
  rotateAuthSession,
} from "../../src/auth/authSessionStore.js";

const userId = "11111111-1111-4111-8111-111111111111";
const sessionIdA = "session-id-a";
const sessionIdB = "session-id-b";
const ttlSeconds = 3600;

function createRedisMock() {
  const store = new Map<string, string>();
  return {
    store,
    redis: {
      get: vi.fn(async (key: string) => store.get(key) ?? null),
      setEx: vi.fn(async (key: string, seconds: number, value: string) => {
        expect(seconds).toBe(ttlSeconds);
        store.set(key, value);
        return "OK";
      }),
      del: vi.fn(async (key: string) => (store.delete(key) ? 1 : 0)),
    },
  };
}

describe("authSessionStore", () => {
  it("creates hashed Redis-backed sessions", async () => {
    const { redis, store } = createRedisMock();

    const result = await createAuthSession(redis, {
      userId,
      ttlSeconds,
      generateSessionId: () => sessionIdA,
    });

    expect(result).toEqual({
      sessionId: sessionIdA,
      userId,
      ttlSeconds,
    });
    expect(redis.setEx).toHaveBeenCalledTimes(1);
    expect(redis.setEx).toHaveBeenCalledWith(buildAuthSessionKey(sessionIdA), ttlSeconds, userId);
    expect(store.get(buildAuthSessionKey(sessionIdA))).toBe(userId);
    expect(buildAuthSessionKey(sessionIdA)).toContain(AUTH_SESSION_KEY_PREFIX);
  });

  it("resolves and destroys sessions by raw session id", async () => {
    const { redis, store } = createRedisMock();
    store.set(buildAuthSessionKey(sessionIdA), userId);

    await expect(resolveAuthSessionUserId(redis, sessionIdA)).resolves.toBe(userId);
    await expect(destroyAuthSession(redis, sessionIdA)).resolves.toBe(true);
    await expect(resolveAuthSessionUserId(redis, sessionIdA)).resolves.toBeNull();
  });

  it("rotates an existing session into a new session id", async () => {
    const { redis, store } = createRedisMock();
    store.set(buildAuthSessionKey(sessionIdA), userId);

    const result = await rotateAuthSession(redis, {
      sessionId: sessionIdA,
      ttlSeconds,
      generateSessionId: () => sessionIdB,
    });

    expect(result).toEqual({
      sessionId: sessionIdB,
      userId,
      ttlSeconds,
    });
    expect(store.has(buildAuthSessionKey(sessionIdA))).toBe(false);
    expect(store.get(buildAuthSessionKey(sessionIdB))).toBe(userId);
  });

  it("returns null when rotating a missing session", async () => {
    const { redis } = createRedisMock();

    await expect(
      rotateAuthSession(redis, {
        sessionId: sessionIdA,
        ttlSeconds,
        generateSessionId: () => sessionIdB,
      })
    ).resolves.toBeNull();
    expect(redis.setEx).not.toHaveBeenCalled();
  });

  it("rejects invalid inputs early", async () => {
    const { redis } = createRedisMock();

    await expect(
      createAuthSession(redis, {
        userId: "not-a-uuid",
        ttlSeconds,
        generateSessionId: () => sessionIdA,
      })
    ).rejects.toThrow("User ID must be a valid UUID");
    await expect(resolveAuthSessionUserId(redis, "   ")).rejects.toThrow("Session ID must be a non-empty string");
    await expect(destroyAuthSession(redis, "   ")).rejects.toThrow("Session ID must be a non-empty string");
  });
});
