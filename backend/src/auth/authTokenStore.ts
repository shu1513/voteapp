import type { Pool, PoolClient } from "pg";

import { hashAuthToken, type AuthTokenPurpose, AUTH_TOKEN_PURPOSES } from "./authPrimitives.js";
import { isUuid } from "../utils/uuid.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type UserAuthTokenRow = {
  id: string;
  user_id: string;
  token_hash: string;
  purpose: AuthTokenPurpose;
  expires_at: string | Date;
  consumed_at: string | Date | null;
  created_at: string | Date;
};

export type IssueUserAuthTokenInput = {
  userId: string;
  tokenHash: string;
  purpose: AuthTokenPurpose;
  expiresAt: Date;
};

export type ConsumeUserAuthTokenInput = {
  token: string;
  purpose: AuthTokenPurpose;
  now?: Date;
};

function normalizeUserId(userId: string): string {
  const normalized = userId.trim();
  if (!isUuid(normalized)) {
    throw new TypeError("User ID must be a valid UUID");
  }
  return normalized;
}

function normalizeTokenHash(tokenHash: string): string {
  const normalized = tokenHash.trim();
  if (normalized.length === 0) {
    throw new TypeError("Token hash must be a non-empty string");
  }
  if (!/^[0-9a-f]{64}$/i.test(normalized)) {
    throw new TypeError("Token hash must be a SHA-256 hex digest");
  }
  return normalized.toLowerCase();
}

function normalizePurpose(purpose: AuthTokenPurpose): AuthTokenPurpose {
  if (!AUTH_TOKEN_PURPOSES.includes(purpose)) {
    throw new TypeError(`Unsupported auth token purpose: ${purpose}`);
  }
  return purpose;
}

function normalizeExpiresAt(expiresAt: Date): Date {
  if (!(expiresAt instanceof Date) || Number.isNaN(expiresAt.getTime())) {
    throw new TypeError("expiresAt must be a valid Date");
  }
  return expiresAt;
}

function normalizeNow(now: Date): Date {
  if (!(now instanceof Date) || Number.isNaN(now.getTime())) {
    throw new TypeError("now must be a valid Date");
  }
  return now;
}

function rowToAuthToken(row: UserAuthTokenRow) {
  return {
    id: row.id,
    userId: row.user_id,
    tokenHash: row.token_hash,
    purpose: row.purpose,
    expiresAt: row.expires_at instanceof Date ? row.expires_at : new Date(row.expires_at),
    consumedAt: row.consumed_at === null ? null : row.consumed_at instanceof Date ? row.consumed_at : new Date(row.consumed_at),
    createdAt: row.created_at instanceof Date ? row.created_at : new Date(row.created_at),
  };
}

export async function issueUserAuthToken(
  db: Queryable,
  input: IssueUserAuthTokenInput
): Promise<ReturnType<typeof rowToAuthToken>> {
  const userId = normalizeUserId(input.userId);
  const tokenHash = normalizeTokenHash(input.tokenHash);
  const purpose = normalizePurpose(input.purpose);
  const expiresAt = normalizeExpiresAt(input.expiresAt);

  // Only the newest link should work: void any outstanding unconsumed tokens
  // of the same purpose so a re-request (resend, repeat forgot-password)
  // leaves exactly one valid token per user/purpose.
  await db.query(
    `
      UPDATE public.user_auth_tokens
      SET consumed_at = now()
      WHERE user_id = $1::uuid
        AND purpose = $2
        AND consumed_at IS NULL
    `,
    [userId, purpose]
  );

  const result = await db.query<UserAuthTokenRow>(
    `
      INSERT INTO public.user_auth_tokens (user_id, token_hash, purpose, expires_at)
      VALUES ($1::uuid, $2, $3, $4::timestamptz)
      RETURNING
        id::text AS id,
        user_id::text AS user_id,
        token_hash,
        purpose,
        expires_at,
        consumed_at,
        created_at
    `,
    [userId, tokenHash, purpose, expiresAt]
  );

  const row = result.rows[0];
  if (!row) {
    throw new Error("Failed to issue auth token");
  }
  return rowToAuthToken(row);
}

export async function consumeUserAuthToken(
  db: Queryable,
  input: ConsumeUserAuthTokenInput
): Promise<ReturnType<typeof rowToAuthToken> | null> {
  const tokenHash = normalizeTokenHash(hashAuthToken(input.token));
  const purpose = normalizePurpose(input.purpose);
  const now = normalizeNow(input.now ?? new Date());

  const result = await db.query<UserAuthTokenRow>(
    `
      UPDATE public.user_auth_tokens
      SET consumed_at = $3::timestamptz
      WHERE token_hash = $1
        AND purpose = $2
        AND consumed_at IS NULL
        AND expires_at > $3::timestamptz
      RETURNING
        id::text AS id,
        user_id::text AS user_id,
        token_hash,
        purpose,
        expires_at,
        consumed_at,
        created_at
    `,
    [tokenHash, purpose, now]
  );

  const row = result.rows[0];
  return row ? rowToAuthToken(row) : null;
}
