import type { Pool, PoolClient } from "pg";

import { hashAuthToken, type AuthTokenPurpose, AUTH_TOKEN_PURPOSES } from "./authPrimitives.js";
import { isUuid } from "../utils/uuid.js";

type Queryable = Pick<Pool | PoolClient, "query">;

export type UserAuthTokenRow = {
  id: string;
  user_id: string;
  token_hash: string;
  purpose: AuthTokenPurpose;
  new_email: string | null;
  expires_at: string | Date;
  consumed_at: string | Date | null;
  created_at: string | Date;
};

export type IssueUserAuthTokenInput = {
  userId: string;
  tokenHash: string;
  purpose: AuthTokenPurpose;
  /** Target address for email_change tokens; forbidden for other purposes. */
  newEmail?: string;
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

function normalizeNewEmail(purpose: AuthTokenPurpose, newEmail: string | undefined): string | null {
  if (purpose === "email_change") {
    const normalized = newEmail?.trim();
    if (!normalized) {
      throw new TypeError("newEmail is required for email_change tokens");
    }
    return normalized;
  }
  if (newEmail !== undefined) {
    throw new TypeError(`newEmail is not allowed for ${purpose} tokens`);
  }
  return null;
}

function rowToAuthToken(row: UserAuthTokenRow) {
  return {
    id: row.id,
    userId: row.user_id,
    tokenHash: row.token_hash,
    purpose: row.purpose,
    newEmail: row.new_email,
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
  const newEmail = normalizeNewEmail(purpose, input.newEmail);
  const expiresAt = normalizeExpiresAt(input.expiresAt);

  // Only the newest link should work: void any outstanding unconsumed tokens
  // of the same purpose so a re-request (resend, repeat forgot-password)
  // leaves exactly one valid token per user/purpose.
  //
  // Concurrency invariant: every caller issues inside a transaction that
  // holds a FOR UPDATE lock on the user row (register/resend/forgot flows in
  // authService), which serializes the void+insert per user. Callers that do
  // not hold that lock can race and leave multiple live tokens.
  //
  // clock_timestamp(), not now(): now() is the transaction start, which
  // predates the lock wait. A token committed by another transaction during
  // that wait has created_at after our start, and chk_user_auth_tokens_consumed_at
  // (consumed_at >= created_at) would reject the void.
  await db.query(
    `
      UPDATE public.user_auth_tokens
      SET consumed_at = clock_timestamp()
      WHERE user_id = $1::uuid
        AND purpose = $2
        AND consumed_at IS NULL
    `,
    [userId, purpose]
  );

  const result = await db.query<UserAuthTokenRow>(
    `
      INSERT INTO public.user_auth_tokens (user_id, token_hash, purpose, new_email, expires_at)
      VALUES ($1::uuid, $2, $3, $4::citext, $5::timestamptz)
      RETURNING
        id::text AS id,
        user_id::text AS user_id,
        token_hash,
        purpose,
        new_email::text AS new_email,
        expires_at,
        consumed_at,
        created_at
    `,
    [userId, tokenHash, purpose, newEmail, expiresAt]
  );

  const row = result.rows[0];
  if (!row) {
    throw new Error("Failed to issue auth token");
  }
  return rowToAuthToken(row);
}

/**
 * Resolves a still-valid token to its owner WITHOUT consuming it. Consumers
 * use this to find which user row to lock before the atomic consume below;
 * the consume re-checks validity after the lock wait, so a token voided by a
 * concurrent ownership change (re-registration, Google takeover, password
 * change) is rejected even though it looked valid here.
 */
export async function peekUserAuthToken(
  db: Queryable,
  input: ConsumeUserAuthTokenInput
): Promise<{ userId: string } | null> {
  const tokenHash = normalizeTokenHash(hashAuthToken(input.token));
  const purpose = normalizePurpose(input.purpose);
  const now = normalizeNow(input.now ?? new Date());

  const result = await db.query<{ user_id: string }>(
    `
      SELECT user_id::text AS user_id
      FROM public.user_auth_tokens
      WHERE token_hash = $1
        AND purpose = $2
        AND consumed_at IS NULL
        AND expires_at > $3::timestamptz
    `,
    [tokenHash, purpose, now]
  );

  const row = result.rows[0];
  return row ? { userId: row.user_id } : null;
}

/**
 * Voids every outstanding (unconsumed) token of the given purposes for a
 * user. Called inside the transaction of an ownership or credential change —
 * re-registration of an unverified address, Google takeover, password
 * reset/change, email verification, email change — after the user row is
 * locked FOR UPDATE, so links issued under the previous owner or credentials
 * stop working the moment that change commits. Same lock order as issuance
 * and the consumers (user row first, then token rows), so the two cannot
 * deadlock.
 */
export async function voidUserAuthTokens(
  db: Queryable,
  input: { userId: string; purposes: readonly AuthTokenPurpose[] }
): Promise<number> {
  const userId = normalizeUserId(input.userId);
  if (input.purposes.length === 0) {
    throw new TypeError("purposes must not be empty");
  }
  const purposes = input.purposes.map(normalizePurpose);

  // clock_timestamp(), not now(): see issueUserAuthToken.
  const result = await db.query(
    `
      UPDATE public.user_auth_tokens
      SET consumed_at = clock_timestamp()
      WHERE user_id = $1::uuid
        AND purpose = ANY($2::text[])
        AND consumed_at IS NULL
    `,
    [userId, purposes]
  );
  return result.rowCount ?? 0;
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
        new_email::text AS new_email,
        expires_at,
        consumed_at,
        created_at
    `,
    [tokenHash, purpose, now]
  );

  const row = result.rows[0];
  return row ? rowToAuthToken(row) : null;
}
