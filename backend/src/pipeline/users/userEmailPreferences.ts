import type { Pool, PoolClient } from "pg";

import { isUuid } from "../../utils/uuid.js";

type Queryable = Pick<Pool | PoolClient, "query">;

// The three email opt-ins stored directly on public.users (001_init).
// email_digest gates the candidate-follow digest sender; the other two are
// reserved for their features but exposed here so the UI can manage all
// email settings in one place.
export type UserEmailPreferences = {
  email_digest: boolean;
  email_election_reminders: boolean;
  email_new_election_alerts: boolean;
};

export type UserEmailPreferencesErrorCode = "invalid_user_id" | "user_not_found";

export class UserEmailPreferencesError extends Error {
  constructor(
    readonly code: UserEmailPreferencesErrorCode,
    message: string
  ) {
    super(message);
    this.name = "UserEmailPreferencesError";
  }
}

function normalizeUserId(userId: string): string {
  const normalized = userId.trim();
  if (!isUuid(normalized)) {
    throw new UserEmailPreferencesError("invalid_user_id", "userId must be a UUID");
  }
  return normalized;
}

export async function getUserEmailPreferences(db: Queryable, userId: string): Promise<UserEmailPreferences> {
  const normalizedUserId = normalizeUserId(userId);

  const result = await db.query<UserEmailPreferences>(
    `
      SELECT email_digest, email_election_reminders, email_new_election_alerts
      FROM public.users
      WHERE id = $1::uuid
        AND deleted_at IS NULL
    `,
    [normalizedUserId]
  );

  const row = result.rows[0];
  if (!row) {
    throw new UserEmailPreferencesError("user_not_found", "User not found");
  }
  return {
    email_digest: row.email_digest,
    email_election_reminders: row.email_election_reminders,
    email_new_election_alerts: row.email_new_election_alerts,
  };
}

export async function setUserEmailPreferences(
  db: Queryable,
  userId: string,
  preferences: UserEmailPreferences
): Promise<UserEmailPreferences> {
  const normalizedUserId = normalizeUserId(userId);

  const result = await db.query<UserEmailPreferences>(
    `
      UPDATE public.users
      SET
        email_digest = $2,
        email_election_reminders = $3,
        email_new_election_alerts = $4,
        updated_at = now()
      WHERE id = $1::uuid
        AND deleted_at IS NULL
      RETURNING email_digest, email_election_reminders, email_new_election_alerts
    `,
    [
      normalizedUserId,
      preferences.email_digest,
      preferences.email_election_reminders,
      preferences.email_new_election_alerts,
    ]
  );

  const row = result.rows[0];
  if (!row) {
    throw new UserEmailPreferencesError("user_not_found", "User not found");
  }
  return {
    email_digest: row.email_digest,
    email_election_reminders: row.email_election_reminders,
    email_new_election_alerts: row.email_new_election_alerts,
  };
}

/**
 * One-click unsubscribe target: turns the digest off without touching the
 * other opt-ins. Idempotent; unknown/deleted users report user_not_found.
 */
export async function disableUserEmailDigest(db: Queryable, userId: string): Promise<void> {
  const normalizedUserId = normalizeUserId(userId);

  const result = await db.query(
    `
      UPDATE public.users
      SET email_digest = false, updated_at = now()
      WHERE id = $1::uuid
        AND deleted_at IS NULL
    `,
    [normalizedUserId]
  );

  if ((result.rowCount ?? 0) === 0) {
    throw new UserEmailPreferencesError("user_not_found", "User not found");
  }
}

/**
 * One-click unsubscribe target for new-election alerts: flips only
 * email_new_election_alerts. Idempotent; unknown/deleted users report
 * user_not_found.
 */
export async function disableUserEmailNewElectionAlerts(db: Queryable, userId: string): Promise<void> {
  const normalizedUserId = normalizeUserId(userId);

  const result = await db.query(
    `
      UPDATE public.users
      SET email_new_election_alerts = false, updated_at = now()
      WHERE id = $1::uuid
        AND deleted_at IS NULL
    `,
    [normalizedUserId]
  );

  if ((result.rowCount ?? 0) === 0) {
    throw new UserEmailPreferencesError("user_not_found", "User not found");
  }
}
