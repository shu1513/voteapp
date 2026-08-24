import type { Pool, PoolClient } from "pg";

import { isUuid } from "../../utils/uuid.js";

type Queryable = Pick<Pool | PoolClient, "query">;

// Device push tokens for the mobile app (user_push_tokens, 170). A token is
// device-scoped, not session-scoped: registration upserts on expo_push_token
// so the same device logging into another account reassigns the row, and
// revocation is a soft revoked_at stamp so senders and pruning share one
// definition of "active".

export type UserPushTokenPlatform = "ios" | "android";

export type RegisterUserPushTokenInput = {
  expoPushToken: string;
  nativeToken: string | null;
  platform: UserPushTokenPlatform;
};

export type UserPushTokensErrorCode = "invalid_user_id" | "user_not_found";

export class UserPushTokensError extends Error {
  constructor(
    readonly code: UserPushTokensErrorCode,
    message: string
  ) {
    super(message);
    this.name = "UserPushTokensError";
  }
}

function normalizeUserId(userId: string): string {
  const normalized = userId.trim();
  if (!isUuid(normalized)) {
    throw new UserPushTokensError("invalid_user_id", "userId must be a UUID");
  }
  return normalized;
}

/**
 * Registers (or refreshes) a device token for the user. Upserts on
 * expo_push_token: re-registration bumps last_seen_at and clears any prior
 * revocation, and a token previously owned by another account moves to this
 * one. Foreign-key violations surface as user_not_found (deleted account
 * racing its own registration).
 */
export async function registerUserPushToken(
  db: Queryable,
  userId: string,
  input: RegisterUserPushTokenInput
): Promise<void> {
  const normalizedUserId = normalizeUserId(userId);

  try {
    await db.query(
      `
        INSERT INTO public.user_push_tokens (user_id, expo_push_token, native_token, platform)
        VALUES ($1::uuid, $2, $3, $4)
        ON CONFLICT (expo_push_token) DO UPDATE
        SET
          user_id = EXCLUDED.user_id,
          native_token = EXCLUDED.native_token,
          platform = EXCLUDED.platform,
          last_seen_at = now(),
          revoked_at = NULL
      `,
      [normalizedUserId, input.expoPushToken, input.nativeToken, input.platform]
    );
  } catch (error) {
    if (isForeignKeyViolation(error)) {
      throw new UserPushTokensError("user_not_found", "User not found");
    }
    throw error;
  }
}

/**
 * Soft-revokes the user's registration of a token (explicit unregister or
 * logout on the device). Scoped to the user so a stale client cannot revoke
 * a token that has since moved to another account. Idempotent: revoking an
 * unknown or already-revoked token is a no-op.
 */
export async function revokeUserPushToken(db: Queryable, userId: string, expoPushToken: string): Promise<void> {
  const normalizedUserId = normalizeUserId(userId);

  await db.query(
    `
      UPDATE public.user_push_tokens
      SET revoked_at = now()
      WHERE user_id = $1::uuid
        AND expo_push_token = $2
        AND revoked_at IS NULL
    `,
    [normalizedUserId, expoPushToken]
  );
}

/**
 * Soft-revokes every token the user has registered: logout-all means no
 * device of theirs should keep receiving personalized pushes. Re-login
 * re-registers (the upsert clears revoked_at).
 */
export async function revokeAllUserPushTokens(db: Queryable, userId: string): Promise<void> {
  const normalizedUserId = normalizeUserId(userId);

  await db.query(
    `
      UPDATE public.user_push_tokens
      SET revoked_at = now()
      WHERE user_id = $1::uuid
        AND revoked_at IS NULL
    `,
    [normalizedUserId]
  );
}

/**
 * Soft-revokes a token regardless of owner: the delivery path calls this when
 * Expo reports DeviceNotRegistered (ticket or receipt), which is a statement
 * about the device, not the account.
 */
export async function revokeUserPushTokenByToken(db: Queryable, expoPushToken: string): Promise<void> {
  await db.query(
    `
      UPDATE public.user_push_tokens
      SET revoked_at = now()
      WHERE expo_push_token = $1
        AND revoked_at IS NULL
    `,
    [expoPushToken]
  );
}

/** Active (unrevoked) Expo push tokens for the user, for the send loops. */
export async function listActiveUserPushTokens(db: Queryable, userId: string): Promise<string[]> {
  const normalizedUserId = normalizeUserId(userId);

  const result = await db.query<{ expo_push_token: string }>(
    `
      SELECT expo_push_token
      FROM public.user_push_tokens
      WHERE user_id = $1::uuid
        AND revoked_at IS NULL
      ORDER BY created_at, expo_push_token
    `,
    [normalizedUserId]
  );
  return result.rows.map((row) => row.expo_push_token);
}

function isForeignKeyViolation(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    (error as { code?: unknown }).code === "23503"
  );
}
