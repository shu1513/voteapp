import type { Pool, PoolClient } from "pg";

import { isUuid } from "../../utils/uuid.js";

type Queryable = Pick<Pool | PoolClient, "query">;

// Session-holder identity for GET /api/me. Deliberately minimal: only what
// the frontend needs to render header state and the unverified-email
// interstitial. Not gated on email verification — an unverified user must be
// able to learn that they are unverified.
export type UserIdentity = {
  email: string;
  first_name: string;
  email_verified: boolean;
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

export async function getUserIdentity(db: Queryable, userId: string): Promise<UserIdentity> {
  const normalizedUserId = normalizeUserId(userId);

  const result = await db.query<UserIdentity>(
    `
      SELECT email, first_name, email_verified
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
  };
}
