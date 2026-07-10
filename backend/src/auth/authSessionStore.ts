import { isUuid } from "../utils/uuid.js";
import { generateSessionId, hashSessionId } from "./authPrimitives.js";

export type AuthSessionRedisClient = {
  get(key: string): Promise<string | null>;
  setEx(key: string, seconds: number, value: string): Promise<unknown>;
  del(key: string): Promise<number>;
  // Set support is required: without the per-user session index, password
  // reset cannot destroy all of a user's sessions.
  sAdd(key: string, member: string): Promise<number>;
  sRem(key: string, member: string): Promise<number>;
  sMembers(key: string): Promise<string[]>;
  expire(key: string, seconds: number): Promise<unknown>;
};

export const AUTH_SESSION_KEY_PREFIX = "auth:session:";
export const AUTH_SESSION_USER_SET_KEY_PREFIX = "auth:user-sessions:";

export type CreateAuthSessionInput = {
  userId: string;
  ttlSeconds: number;
  /** users.session_epoch at creation time; per-request auth compares it against the DB. */
  sessionEpoch: number;
  generateSessionId?: () => string;
};

export type CreateAuthSessionResult = {
  sessionId: string;
  userId: string;
  sessionEpoch: number;
  ttlSeconds: number;
};

export type AuthSessionRecord = {
  userId: string;
  sessionEpoch: number;
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

function normalizeSessionEpoch(sessionEpoch: number): number {
  if (!Number.isInteger(sessionEpoch) || sessionEpoch <= 0) {
    throw new TypeError("Session epoch must be a positive integer");
  }
  return sessionEpoch;
}

// Session values are "<userId>:<epoch>". UUIDs carry no colon, so the split
// is unambiguous. Values that do not parse (including any pre-epoch plain
// userId values) are treated as no session: the holder just logs in again.
function parseAuthSessionValue(value: string | null | undefined): AuthSessionRecord | null {
  const trimmed = value?.trim();
  if (!trimmed) {
    return null;
  }
  const separatorIndex = trimmed.lastIndexOf(":");
  if (separatorIndex <= 0) {
    return null;
  }
  const userId = trimmed.slice(0, separatorIndex);
  const epochText = trimmed.slice(separatorIndex + 1);
  if (!isUuid(userId) || !/^\d+$/.test(epochText)) {
    return null;
  }
  const sessionEpoch = Number.parseInt(epochText, 10);
  if (!Number.isInteger(sessionEpoch) || sessionEpoch <= 0) {
    return null;
  }
  return { userId, sessionEpoch };
}

function buildUserSessionsKey(userId: string): string {
  return `${AUTH_SESSION_USER_SET_KEY_PREFIX}${normalizeUserId(userId)}`;
}

async function recordSessionMembership(
  redis: Pick<AuthSessionRedisClient, "sAdd" | "del" | "expire">,
  sessionId: string,
  userId: string,
  ttlSeconds: number
): Promise<void> {
  try {
    const userSessionsKey = buildUserSessionsKey(userId);
    await redis.sAdd(userSessionsKey, hashSessionId(sessionId));
    // Keep the index from outliving its sessions: refresh the set TTL to the
    // newest session's TTL, so a set left holding only naturally-expired
    // session hashes expires too instead of accumulating ghosts forever.
    await redis.expire(userSessionsKey, ttlSeconds);
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
  const sessionEpoch = normalizeSessionEpoch(input.sessionEpoch);
  const sessionId = normalizeSessionId((input.generateSessionId ?? generateSessionId)());
  await redis.setEx(buildAuthSessionKey(sessionId), ttlSeconds, `${userId}:${sessionEpoch}`);
  await recordSessionMembership(redis, sessionId, userId, ttlSeconds);
  return { sessionId, userId, sessionEpoch, ttlSeconds };
}

export async function resolveAuthSession(
  redis: Pick<AuthSessionRedisClient, "get">,
  sessionId: string
): Promise<AuthSessionRecord | null> {
  const value = await redis.get(buildAuthSessionKey(sessionId));
  return parseAuthSessionValue(value);
}

export async function resolveAuthSessionUserId(
  redis: Pick<AuthSessionRedisClient, "get">,
  sessionId: string
): Promise<string | null> {
  const record = await resolveAuthSession(redis, sessionId);
  return record?.userId ?? null;
}

export async function destroyAuthSession(
  redis: Pick<AuthSessionRedisClient, "del" | "get" | "sRem">,
  sessionId: string
): Promise<boolean> {
  const normalizedSessionId = normalizeSessionId(sessionId);
  const sessionKey = buildAuthSessionKey(normalizedSessionId);
  const existingRecord = parseAuthSessionValue(await redis.get(sessionKey));
  const deleted = await redis.del(sessionKey);
  if (deleted > 0 && existingRecord) {
    try {
      await redis.sRem(buildUserSessionsKey(existingRecord.userId), hashSessionId(normalizedSessionId));
    } catch {
      // Best-effort reverse index cleanup; the session itself is already removed.
    }
  }
  return deleted > 0;
}

/**
 * Best-effort sweep of a user's sessions via the reverse index. Deliberately
 * not atomic with concurrent logins: a new-epoch session created mid-sweep
 * can be swept too (that user just logs in again) or miss the index deletion
 * and stay unindexed (harmless — the index only feeds this sweep, and every
 * caller first makes the revocation durable in the database: an epoch bump,
 * or the soft delete that fails the per-request user lookup).
 * Per-request epoch validation is the correctness mechanism; this is cleanup.
 */
export async function destroyAuthSessionsByUserId(
  redis: Pick<AuthSessionRedisClient, "del" | "sMembers" | "sRem">,
  input: DestroyAuthSessionsByUserIdInput
): Promise<number> {
  const userId = normalizeUserId(input.userId);
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
  const existing = await resolveAuthSession(redis, input.sessionId);
  if (!existing) {
    return null;
  }

  await destroyAuthSession(redis, input.sessionId);
  return createAuthSession(redis, {
    userId: existing.userId,
    ttlSeconds: input.ttlSeconds,
    sessionEpoch: existing.sessionEpoch,
    generateSessionId: input.generateSessionId,
  });
}
