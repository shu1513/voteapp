import type { Pool, PoolClient } from "pg";

import { isUuid } from "../../utils/uuid.js";

type Queryable = Pick<Pool | PoolClient, "query">;

// Session-holder identity for GET /api/me. Deliberately minimal: only what
// the frontend needs to render header state and the unverified-email and
// terms-reacceptance interstitials. Not gated on email verification — an
// unverified user must be able to learn that they are unverified.
export type UserIdentity = {
  email: string;
  first_name: string;
  email_verified: boolean;
  accepted_terms_version: string | null;
  /** False on Google-created accounts that never set a password: Settings
   * swaps its password-gated forms for an "add a password" hint. */
  has_password: boolean;
};

export type UserIdentityErrorCode = "invalid_user_id" | "user_not_found";

export class UserIdentityError extends Error {
  constructor(
    readonly code: UserIdentityErrorCode,
    message: string
  ) {
    super(message);
    this.name = "UserIdentityError";
  }
}

function normalizeUserId(userId: string): string {
  const normalized = userId.trim();
  if (!isUuid(normalized)) {
    throw new UserIdentityError("invalid_user_id", "userId must be a UUID");
  }
  return normalized;
}

// Mirror registration's derived-name cap (deriveFirstName slices to 80).
export const MAX_FIRST_NAME_LENGTH = 80;

export async function setUserFirstName(db: Queryable, userId: string, firstName: string): Promise<UserIdentity> {
  const normalizedUserId = normalizeUserId(userId);
  const normalizedFirstName = typeof firstName === "string" ? firstName.trim() : "";
  if (normalizedFirstName.length === 0) {
    throw new TypeError("first_name must be a non-empty string");
  }
  if (normalizedFirstName.length > MAX_FIRST_NAME_LENGTH) {
    throw new TypeError(`first_name must be at most ${MAX_FIRST_NAME_LENGTH} characters`);
  }

  const result = await db.query<UserIdentity>(
    `
      UPDATE public.users
      SET first_name = $2,
          updated_at = now()
      WHERE id = $1::uuid
        AND deleted_at IS NULL
      RETURNING email, first_name, email_verified, accepted_terms_version,
        (password_hash IS NOT NULL) AS has_password
    `,
    [normalizedUserId, normalizedFirstName]
  );

  const row = result.rows[0];
  if (!row) {
    throw new UserIdentityError("user_not_found", "User not found");
  }
  return {
    email: row.email,
    first_name: row.first_name,
    email_verified: row.email_verified,
    accepted_terms_version: row.accepted_terms_version,
    has_password: row.has_password,
  };
}

/** Records the session holder's acceptance of the given terms version.
 * The caller (apiServer) is responsible for only passing the current
 * version; this just stamps what was accepted and when.
 *
 * The users columns are overwritten — they answer "which version is this
 * account on now" — while user_terms_acceptances gains a row, so the version
 * being replaced here stays provable afterwards.
 *
 * Both happen in ONE statement on purpose. A data-modifying CTE is atomic
 * without any transaction handling, so this still works when handed a plain
 * Pool, and there is no arrangement of failures that can overwrite the users
 * row while losing the history of what it used to say. */
export async function acceptUserTerms(db: Queryable, userId: string, termsVersion: string): Promise<UserIdentity> {
  const normalizedUserId = normalizeUserId(userId);
  const normalizedVersion = typeof termsVersion === "string" ? termsVersion.trim() : "";
  if (normalizedVersion.length === 0) {
    throw new TypeError("termsVersion must be a non-empty string");
  }

  const result = await db.query<UserIdentity>(
    `
      WITH accepted AS (
        UPDATE public.users
        SET accepted_terms_version = $2,
            accepted_terms_at = now(),
            updated_at = now()
        WHERE id = $1::uuid
          AND deleted_at IS NULL
        RETURNING id, email, first_name, email_verified, accepted_terms_version, accepted_terms_at,
          (password_hash IS NOT NULL) AS has_password
      ), logged AS (
        INSERT INTO public.user_terms_acceptances (user_id, terms_version, context, accepted_at)
        SELECT id, accepted_terms_version, 'renewal', accepted_terms_at
        FROM accepted
      )
      SELECT email, first_name, email_verified, accepted_terms_version, has_password
      FROM accepted
    `,
    [normalizedUserId, normalizedVersion]
  );

  const row = result.rows[0];
  if (!row) {
    throw new UserIdentityError("user_not_found", "User not found");
  }
  return {
    email: row.email,
    first_name: row.first_name,
    email_verified: row.email_verified,
    accepted_terms_version: row.accepted_terms_version,
    has_password: row.has_password,
  };
}

export async function getUserIdentity(db: Queryable, userId: string): Promise<UserIdentity> {
  const normalizedUserId = normalizeUserId(userId);

  const result = await db.query<UserIdentity>(
    `
      SELECT email, first_name, email_verified, accepted_terms_version,
        (password_hash IS NOT NULL) AS has_password
      FROM public.users
      WHERE id = $1::uuid
        AND deleted_at IS NULL
    `,
    [normalizedUserId]
  );

  const row = result.rows[0];
  if (!row) {
    throw new UserIdentityError("user_not_found", "User not found");
  }
  return {
    email: row.email,
    first_name: row.first_name,
    email_verified: row.email_verified,
    accepted_terms_version: row.accepted_terms_version,
    has_password: row.has_password,
  };
}
