import { isUuid } from "../utils/uuid.js";
import { generateSessionId, hashSessionId } from "./authPrimitives.js";

export type AuthSessionRedisClient = {
  get(key: string): Promise<string | null>;
  setEx(key: string, seconds: number, value: string): Promise<unknown>;
  del(key: string): Promise<number>;
  sAdd?(key: string, member: string): Promise<number>;
  sRem?(key: string, member: string): Promise<number>;
  sMembers?(key: string): Promise<string[]>;
};

export const AUTH_SESSION_KEY_PREFIX = "auth:session:";
export const AUTH_SESSION_USER_SET_KEY_PREFIX = "auth:user-sessions:";

export type CreateAuthSessionInput = {
  userId: string;
  ttlSeconds: number;
  generateSessionId?: () => string;
};

export type CreateAuthSessionResult = {
  sessionId: string;
  userId: string;
  ttlSeconds: number;
};

export type RotateAuthSessionInput = {
  sessionId: string;
  ttlSeconds: number;
  generateSessionId?: () => string;
};

export type DestroyAuthSessionsByUserIdInput = {
  userId: string;
};

function normalizeUserId(userId: string): string {
  const normalized = userId.trim();
  if (!isUuid(normalized)) {
    throw new TypeError("User ID must be a valid UUID");
  }
  return normalized;
}

function normalizeSessionId(sessionId: string): string {
  const normalized = sessionId.trim();
  if (normalized.length === 0) {
    throw new TypeError("Session ID must be a non-empty string");
  }
  return normalized;
}

function normalizeTtlSeconds(ttlSeconds: number): number {
  if (!Number.isInteger(ttlSeconds) || ttlSeconds <= 0) {
    throw new TypeError("TTL must be a positive integer");
  }
  return ttlSeconds;
}

function buildUserSessionsKey(userId: string): string {
  return `${AUTH_SESSION_USER_SET_KEY_PREFIX}${normalizeUserId(userId)}`;
}

async function recordSessionMembership(
  redis: Pick<AuthSessionRedisClient, "sAdd" | "del">,
  sessionId: string,
  userId: string
): Promise<void> {
  if (!redis.sAdd) {
    return;
  }

  try {
    await redis.sAdd(buildUserSessionsKey(userId), hashSessionId(sessionId));
  } catch (error) {
    await redis.del(buildAuthSessionKey(sessionId));
    throw error;
  }
}

export function buildAuthSessionKey(sessionId: string): string {
  return `${AUTH_SESSION_KEY_PREFIX}${hashSessionId(normalizeSessionId(sessionId))}`;
}

export async function createAuthSession(
  redis: AuthSessionRedisClient,
  input: CreateAuthSessionInput
): Promise<CreateAuthSessionResult> {
  const userId = normalizeUserId(input.userId);
  const ttlSeconds = normalizeTtlSeconds(input.ttlSeconds);
  const sessionId = normalizeSessionId((input.generateSessionId ?? generateSessionId)());
  await redis.setEx(buildAuthSessionKey(sessionId), ttlSeconds, userId);
  await recordSessionMembership(redis, sessionId, userId);
  return { sessionId, userId, ttlSeconds };
}

export async function resolveAuthSessionUserId(
  redis: Pick<AuthSessionRedisClient, "get">,
  sessionId: string
): Promise<string | null> {
  const value = await redis.get(buildAuthSessionKey(sessionId));
  const trimmed = value?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : null;
}

export async function destroyAuthSession(
  redis: Pick<AuthSessionRedisClient, "del" | "get" | "sRem">,
  sessionId: string
): Promise<boolean> {
  const normalizedSessionId = normalizeSessionId(sessionId);
  const sessionKey = buildAuthSessionKey(normalizedSessionId);
  const existingUserId = await redis.get(sessionKey);
  const deleted = await redis.del(sessionKey);
  if (deleted > 0 && existingUserId?.trim() && redis.sRem) {
    try {
      await redis.sRem(buildUserSessionsKey(existingUserId), hashSessionId(normalizedSessionId));
    } catch {
      // Best-effort reverse index cleanup; the session itself is already removed.
    }
  }
  return deleted > 0;
}

export async function destroyAuthSessionsByUserId(
  redis: Pick<AuthSessionRedisClient, "del" | "sMembers" | "sRem">,
  input: DestroyAuthSessionsByUserIdInput
): Promise<number> {
  const userId = normalizeUserId(input.userId);
  if (!redis.sMembers) {
    throw new TypeError("Redis client does not support session invalidation by user");
  }

  const sessionHashes = await redis.sMembers(buildUserSessionsKey(userId));
  let destroyedCount = 0;
  for (const sessionHash of sessionHashes) {
    const normalizedSessionHash = sessionHash.trim();
    if (normalizedSessionHash.length === 0) {
      continue;
    }
    destroyedCount += (await redis.del(`${AUTH_SESSION_KEY_PREFIX}${normalizedSessionHash}`)) > 0 ? 1 : 0;
  }
  await redis.del(buildUserSessionsKey(userId));
  return destroyedCount;
}

export async function rotateAuthSession(
  redis: AuthSessionRedisClient,
  input: RotateAuthSessionInput
): Promise<CreateAuthSessionResult | null> {
  const existingUserId = await resolveAuthSessionUserId(redis, input.sessionId);
  if (!existingUserId) {
    return null;
  }

  await destroyAuthSession(redis, input.sessionId);
  return createAuthSession(redis, {
    userId: existingUserId,
    ttlSeconds: input.ttlSeconds,
    generateSessionId: input.generateSessionId,
  });
}
