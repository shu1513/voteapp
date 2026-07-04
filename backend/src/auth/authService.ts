import type { Pool, PoolClient } from "pg";

import { createAuthSession, destroyAuthSession, destroyAuthSessionsByUserId, type AuthSessionRedisClient } from "./authSessionStore.js";
import { generateAuthToken, hashPassword, validatePasswordPolicy, verifyPassword } from "./authPrimitives.js";
import { issueUserAuthToken, consumeUserAuthToken } from "./authTokenStore.js";
import type { AuthMailer } from "./authMailer.js";
import { isUuid } from "../utils/uuid.js";
import { CURRENT_TERMS_VERSION } from "../constants/legal.js";

type Queryable = Pick<Pool | PoolClient, "query">;
type TransactionalDb = Pick<Pool, "connect" | "query">;
type TransactionClient = Pick<PoolClient, "query" | "release">;

export const DEFAULT_AUTH_SESSION_TTL_SECONDS = 60 * 60 * 24 * 30;
export const DEFAULT_AUTH_EMAIL_VERIFICATION_TTL_SECONDS = 60 * 60 * 24 * 3;
export const DEFAULT_AUTH_PASSWORD_RESET_TTL_SECONDS = 60 * 60 * 24;
export const DEFAULT_AUTH_EMAIL_CHANGE_TTL_SECONDS = 60 * 60 * 24;

export type AuthServiceOptions = {
  db: TransactionalDb;
  redis: AuthSessionRedisClient;
  mailer: AuthMailer;
  publicBaseUrl: string;
  sessionTtlSeconds?: number;
  emailVerificationTtlSeconds?: number;
  passwordResetTtlSeconds?: number;
  emailChangeTtlSeconds?: number;
};

export type AuthRegisterInput = {
  email: string;
  password: string;
  firstName?: string;
  /** Terms/disclaimer version the user accepted at signup (clickwrap record). */
  acceptedTermsVersion: string;
};

export type AuthLoginInput = {
  email: string;
  password: string;
  currentSessionId?: string | null;
};

export type AuthLogoutInput = {
  currentSessionId?: string | null;
};

export type AuthForgotPasswordInput = {
  email: string;
};

export type AuthResetPasswordInput = {
  token: string;
  password: string;
};

export type AuthLoginResult = {
  sessionId: string;
};

export type AuthChangePasswordInput = {
  userId: string;
  currentPassword: string;
  newPassword: string;
};

export type AuthDeleteAccountInput = {
  userId: string;
  password: string;
};

export type AuthRequestEmailChangeInput = {
  userId: string;
  newEmail: string;
  password: string;
};

export type AuthLogoutAllInput = {
  userId: string;
};

export type AuthService = {
  register(input: AuthRegisterInput): Promise<void>;
  verifyEmail(input: { token: string }): Promise<void>;
  login(input: AuthLoginInput): Promise<AuthLoginResult>;
  logout(input: AuthLogoutInput): Promise<void>;
  logoutAll(input: AuthLogoutAllInput): Promise<void>;
  forgotPassword(input: AuthForgotPasswordInput): Promise<void>;
  resendVerification(input: AuthForgotPasswordInput): Promise<void>;
  resetPassword(input: AuthResetPasswordInput): Promise<void>;
  /** Logged-in password change; rotates every session, returns the fresh one. */
  changePassword(input: AuthChangePasswordInput): Promise<AuthLoginResult>;
  requestEmailChange(input: AuthRequestEmailChangeInput): Promise<void>;
  verifyEmailChange(input: { token: string }): Promise<void>;
  deleteAccount(input: AuthDeleteAccountInput): Promise<void>;
};

type AuthUserRow = {
  id: string;
  email: string;
  first_name: string;
  password_hash: string;
  email_verified: boolean;
};

function normalizeEmail(email: string): string {
  if (typeof email !== "string") {
    throw new TypeError("Email must be a string");
  }
  const normalized = email.trim();
  if (normalized.length === 0) {
    throw new TypeError("Email must be a non-empty string");
  }
  if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(normalized)) {
    throw new TypeError("Email must be a valid email address");
  }
  return normalized;
}

function normalizeOptionalFirstName(firstName: string | undefined): string | null {
  if (firstName === undefined) {
    return null;
  }
  if (typeof firstName !== "string") {
    throw new TypeError("firstName must be a string");
  }
  const normalized = firstName.trim();
  return normalized.length > 0 ? normalized : null;
}

function deriveFirstName(email: string, providedFirstName: string | null): string {
  if (providedFirstName) {
    return providedFirstName;
  }
  const localPart = email.split("@", 1)[0]?.trim() ?? "";
  const collapsed = localPart.replace(/[._+-]+/g, " ").replace(/\s+/g, " ").trim();
  return collapsed.length > 0 ? collapsed.slice(0, 80) : "User";
}

function normalizePublicBaseUrl(publicBaseUrl: string): URL {
  if (typeof publicBaseUrl !== "string") {
    throw new TypeError("publicBaseUrl must be a string");
  }
  const normalized = publicBaseUrl.trim();
  if (normalized.length === 0) {
    throw new TypeError("publicBaseUrl must be a non-empty string");
  }
  const parsed = new URL(normalized);
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new TypeError("publicBaseUrl must use http or https");
  }
  return parsed;
}

function buildEmailLink(baseUrl: URL, path: string, token: string): string {
  const url = new URL(path, baseUrl);
  url.searchParams.set("token", token);
  return url.toString();
}

async function rollbackQuietly(client: TransactionClient): Promise<void> {
  try {
    await client.query("ROLLBACK");
  } catch {
    // Preserve the original failure.
  }
}

async function findActiveUserByEmail(db: Queryable, email: string): Promise<AuthUserRow | null> {
  const result = await db.query<AuthUserRow>(
    `
      SELECT
        id::text AS id,
        email::text AS email,
        first_name,
        password_hash,
        email_verified
      FROM public.users
      WHERE email = $1::citext
        AND deleted_at IS NULL
    `,
    [email]
  );
  return result.rows[0] ?? null;
}

async function findActiveUserByEmailForUpdate(db: Queryable, email: string): Promise<AuthUserRow | null> {
  const result = await db.query<AuthUserRow>(
    `
      SELECT
        id::text AS id,
        email::text AS email,
        first_name,
        password_hash,
        email_verified
      FROM public.users
      WHERE email = $1::citext
        AND deleted_at IS NULL
      FOR UPDATE
    `,
    [email]
  );
  return result.rows[0] ?? null;
}

function toEmailVerificationLink(baseUrl: URL, token: string): string {
  return buildEmailLink(baseUrl, "/verify-email", token);
}

function toEmailChangeLink(baseUrl: URL, token: string): string {
  return buildEmailLink(baseUrl, "/verify-email-change", token);
}

function normalizeUserId(userId: string): string {
  const normalized = typeof userId === "string" ? userId.trim() : "";
  if (!isUuid(normalized)) {
    throw new TypeError("userId must be a UUID");
  }
  return normalized;
}

async function findActiveUserByIdForUpdate(db: Queryable, userId: string): Promise<AuthUserRow | null> {
  const result = await db.query<AuthUserRow>(
    `
      SELECT
        id::text AS id,
        email::text AS email,
        first_name,
        password_hash,
        email_verified
      FROM public.users
      WHERE id = $1::uuid
        AND deleted_at IS NULL
      FOR UPDATE
    `,
    [userId]
  );
  return result.rows[0] ?? null;
}

function toPasswordResetLink(baseUrl: URL, token: string): string {
  return buildEmailLink(baseUrl, "/reset-password", token);
}

const DUMMY_PASSWORD_HASH_PROMISE = hashPassword("auth-login-dummy-password");

function normalizeSessionId(value: string | null | undefined): string | null {
  const trimmed = value?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : null;
}

async function updateLastLoggedIn(client: Queryable, userId: string): Promise<void> {
  await client.query(
    `
      UPDATE public.users
      SET last_logged_in = now()
      WHERE id = $1::uuid
        AND deleted_at IS NULL
    `,
    [userId]
  );
}

async function createOrRefreshAuthUser(
  client: Queryable,
  input: {
    email: string;
    firstName: string;
    passwordHash: string;
    acceptedTermsVersion: string;
  }
): Promise<AuthUserRow> {
  const existing = await findActiveUserByEmailForUpdate(client, input.email);
  if (existing && existing.email_verified) {
    return existing;
  }

  if (existing) {
    const updated = await client.query<AuthUserRow>(
      `
        UPDATE public.users
        SET
          first_name = $2,
          password_hash = $3,
          accepted_terms_version = $4,
          accepted_terms_at = now(),
          updated_at = now()
        WHERE id = $1::uuid
          AND deleted_at IS NULL
        RETURNING
          id::text AS id,
          email::text AS email,
          first_name,
          password_hash,
          email_verified
      `,
      [existing.id, input.firstName, input.passwordHash, input.acceptedTermsVersion]
    );
    const row = updated.rows[0];
    if (!row) {
      throw new Error("Failed to update auth user");
    }
    return row;
  }

  const inserted = await client.query<AuthUserRow>(
    `
      INSERT INTO public.users (
        first_name,
        email,
        password_hash,
        email_verified,
        accepted_terms_version,
        accepted_terms_at
      )
      VALUES ($1, $2::citext, $3, false, $4, now())
      RETURNING
        id::text AS id,
        email::text AS email,
        first_name,
        password_hash,
        email_verified
    `,
    [input.firstName, input.email, input.passwordHash, input.acceptedTermsVersion]
  );
  const row = inserted.rows[0];
  if (!row) {
    throw new Error("Failed to create auth user");
  }
  return row;
}

async function issueEmailVerificationToken(
  client: Queryable,
  input: {
    userId: string;
    ttlSeconds: number;
  }
): Promise<{ rawToken: string; tokenHash: string }> {
  const token = generateAuthToken();
  await issueUserAuthToken(client, {
    userId: input.userId,
    tokenHash: token.tokenHash,
    purpose: "email_verify",
    expiresAt: new Date(Date.now() + input.ttlSeconds * 1000),
  });
  return token;
}

export function createAuthService(options: AuthServiceOptions): AuthService {
  const publicBaseUrl = normalizePublicBaseUrl(options.publicBaseUrl);
  const sessionTtlSeconds = options.sessionTtlSeconds ?? DEFAULT_AUTH_SESSION_TTL_SECONDS;
  const emailVerificationTtlSeconds = options.emailVerificationTtlSeconds ?? DEFAULT_AUTH_EMAIL_VERIFICATION_TTL_SECONDS;
  const passwordResetTtlSeconds = options.passwordResetTtlSeconds ?? DEFAULT_AUTH_PASSWORD_RESET_TTL_SECONDS;
  const emailChangeTtlSeconds = options.emailChangeTtlSeconds ?? DEFAULT_AUTH_EMAIL_CHANGE_TTL_SECONDS;

  return {
    async register(input) {
      const email = normalizeEmail(input.email);
      const firstName = deriveFirstName(email, normalizeOptionalFirstName(input.firstName));
      validatePasswordPolicy(input.password);
      const acceptedTermsVersion =
        typeof input.acceptedTermsVersion === "string" ? input.acceptedTermsVersion.trim() : "";
      // Enforced here as well as at the API layer: no caller (script, admin
      // tooling, future route) may persist acceptance of anything but the
      // current terms version — the stored value is the evidentiary record.
      if (acceptedTermsVersion !== CURRENT_TERMS_VERSION) {
        throw new TypeError(
          `acceptedTermsVersion must be the current terms version (${CURRENT_TERMS_VERSION})`
        );
      }
      const passwordHash = await hashPassword(input.password);
      const client = await options.db.connect();

      try {
        await client.query("BEGIN");
        const user = await createOrRefreshAuthUser(client, {
          email,
          firstName,
          passwordHash,
          acceptedTermsVersion,
        });

        if (user.email_verified) {
          await client.query("COMMIT");
          return;
        }

        const token = await issueEmailVerificationToken(client, {
          userId: user.id,
          ttlSeconds: emailVerificationTtlSeconds,
        });

        await client.query("COMMIT");
        await options.mailer.sendVerificationEmail({
          email,
          linkUrl: toEmailVerificationLink(publicBaseUrl, token.rawToken),
        });
      } catch (error) {
        await rollbackQuietly(client);
        throw error;
      } finally {
        client.release();
      }
    },

    async resendVerification(input) {
      const email = normalizeEmail(input.email);
      const client = await options.db.connect();

      try {
        await client.query("BEGIN");
        const user = await findActiveUserByEmailForUpdate(client, email);
        if (!user || user.email_verified) {
          await client.query("COMMIT");
          return;
        }

        const token = await issueEmailVerificationToken(client, {
          userId: user.id,
          ttlSeconds: emailVerificationTtlSeconds,
        });

        await client.query("COMMIT");
        await options.mailer.sendVerificationEmail({
          email,
          linkUrl: toEmailVerificationLink(publicBaseUrl, token.rawToken),
        });
      } catch (error) {
        await rollbackQuietly(client);
        throw error;
      } finally {
        client.release();
      }
    },

    async verifyEmail(input) {
      if (typeof input.token !== "string" || input.token.trim().length === 0) {
        throw new TypeError("token must be a non-empty string");
      }

      const client = await options.db.connect();
      try {
        await client.query("BEGIN");
        const consumed = await consumeUserAuthToken(client, {
          token: input.token,
          purpose: "email_verify",
          now: new Date(),
        });
        if (!consumed) {
          throw new TypeError("Verification token is invalid or expired");
        }

        const updated = await client.query(
          `
            UPDATE public.users
            SET email_verified = true,
                updated_at = now()
            WHERE id = $1::uuid
              AND deleted_at IS NULL
          `,
          [consumed.userId]
        );
        if (updated.rowCount !== 1) {
          throw new Error("Failed to verify user email");
        }

        await client.query("COMMIT");
      } catch (error) {
        await rollbackQuietly(client);
        throw error;
      } finally {
        client.release();
      }
    },

    async login(input) {
      const email = normalizeEmail(input.email);
      const user = await findActiveUserByEmail(options.db, email);
      const passwordHash = user?.password_hash ?? (await DUMMY_PASSWORD_HASH_PROMISE);
      const passwordMatches = await verifyPassword(passwordHash, input.password);
      if (!user || !passwordMatches) {
        throw new TypeError("Invalid email or password");
      }

      const client = await options.db.connect();
      try {
        await client.query("BEGIN");
        await updateLastLoggedIn(client, user.id);
        await client.query("COMMIT");
      } catch (error) {
        await rollbackQuietly(client);
        throw error;
      } finally {
        client.release();
      }

      const currentSessionId = normalizeSessionId(input.currentSessionId);
      if (currentSessionId) {
        try {
          await destroyAuthSession(options.redis, currentSessionId);
        } catch {
          // Best-effort fixation cleanup. The fresh session below is the source of truth.
        }
      }

      const session = await createAuthSession(options.redis, {
        userId: user.id,
        ttlSeconds: sessionTtlSeconds,
      });
      return { sessionId: session.sessionId };
    },

    async logout(input) {
      const currentSessionId = normalizeSessionId(input.currentSessionId);
      if (!currentSessionId) {
        return;
      }
      await destroyAuthSession(options.redis, currentSessionId);
    },

    async forgotPassword(input) {
      const email = normalizeEmail(input.email);
      const client = await options.db.connect();
      let token: ReturnType<typeof generateAuthToken> | null = null;
      try {
        await client.query("BEGIN");
        // Lock the user row like the verification flow does: token issuance
        // relies on this lock to serialize concurrent requests so the
        // void-then-insert in issueUserAuthToken leaves exactly one live token.
        const user = await findActiveUserByEmailForUpdate(client, email);
        if (!user) {
          await client.query("COMMIT");
          return;
        }

        token = generateAuthToken();
        await issueUserAuthToken(client, {
          userId: user.id,
          tokenHash: token.tokenHash,
          purpose: "password_reset",
          expiresAt: new Date(Date.now() + passwordResetTtlSeconds * 1000),
        });
        await client.query("COMMIT");
      } catch (error) {
        await rollbackQuietly(client);
        throw error;
      } finally {
        client.release();
      }

      await options.mailer.sendPasswordResetEmail({
        email,
        linkUrl: toPasswordResetLink(publicBaseUrl, token.rawToken),
      });
    },

    async resetPassword(input) {
      if (typeof input.token !== "string" || input.token.trim().length === 0) {
        throw new TypeError("token must be a non-empty string");
      }
      validatePasswordPolicy(input.password);
      const client = await options.db.connect();
      let userIdToInvalidate: string | null = null;

      try {
        await client.query("BEGIN");
        const consumed = await consumeUserAuthToken(client, {
          token: input.token,
          purpose: "password_reset",
          now: new Date(),
        });
        if (!consumed) {
          throw new TypeError("Password reset token is invalid or expired");
        }

        // Hash only after the token is proven valid: bogus-token requests
        // must not be able to burn Argon2 work. Hashing inside the
        // transaction means a hashing failure rolls back the consumption,
        // so the token is not wasted.
        const passwordHash = await hashPassword(input.password);

        const updated = await client.query(
          `
            UPDATE public.users
            SET password_hash = $2,
                updated_at = now()
            WHERE id = $1::uuid
              AND deleted_at IS NULL
          `,
          [consumed.userId, passwordHash]
        );
        if (updated.rowCount !== 1) {
          throw new Error("Failed to update user password");
        }

        userIdToInvalidate = consumed.userId;
        await client.query("COMMIT");
      } catch (error) {
        await rollbackQuietly(client);
        throw error;
      } finally {
        client.release();
      }

      // Invalidate sessions only after the password change is committed: a
      // rollback must not log the user out of everything, and a concurrent
      // old-password login can no longer create a session behind a
      // pre-commit invalidation.
      if (userIdToInvalidate) {
        await destroyAuthSessionsByUserId(options.redis, { userId: userIdToInvalidate });
      }
    },

    async changePassword(input) {
      const userId = normalizeUserId(input.userId);
      if (typeof input.currentPassword !== "string" || input.currentPassword.length === 0) {
        throw new TypeError("currentPassword must be a non-empty string");
      }
      validatePasswordPolicy(input.newPassword);

      const client = await options.db.connect();
      try {
        await client.query("BEGIN");
        const user = await findActiveUserByIdForUpdate(client, userId);
        if (!user || !(await verifyPassword(user.password_hash, input.currentPassword))) {
          // Same message for missing user and wrong password, like login.
          throw new TypeError("Current password is incorrect");
        }

        const passwordHash = await hashPassword(input.newPassword);
        await client.query(
          `
            UPDATE public.users
            SET password_hash = $2,
                updated_at = now()
            WHERE id = $1::uuid
              AND deleted_at IS NULL
          `,
          [userId, passwordHash]
        );
        await client.query("COMMIT");
      } catch (error) {
        await rollbackQuietly(client);
        throw error;
      } finally {
        client.release();
      }

      // Rotate every session (including the caller's) after commit, then hand
      // back a fresh one so the caller stays logged in: any session an
      // attacker may hold dies with the old password.
      await destroyAuthSessionsByUserId(options.redis, { userId });
      const session = await createAuthSession(options.redis, {
        userId,
        ttlSeconds: sessionTtlSeconds,
      });
      return { sessionId: session.sessionId };
    },

    async requestEmailChange(input) {
      const userId = normalizeUserId(input.userId);
      const newEmail = normalizeEmail(input.newEmail);
      if (typeof input.password !== "string" || input.password.length === 0) {
        throw new TypeError("password must be a non-empty string");
      }

      const client = await options.db.connect();
      let token: ReturnType<typeof generateAuthToken> | null = null;
      try {
        await client.query("BEGIN");
        // FOR UPDATE lock serializes token issuance per user (see
        // issueUserAuthToken's void-then-insert invariant).
        const user = await findActiveUserByIdForUpdate(client, userId);
        if (!user || !(await verifyPassword(user.password_hash, input.password))) {
          throw new TypeError("Password is incorrect");
        }
        if (user.email.toLowerCase() === newEmail.toLowerCase()) {
          throw new TypeError("New email must be different from the current email");
        }

        // Void outstanding change links up front, not only inside
        // issueUserAuthToken: the newest request must kill older links even
        // when it ends on the silent taken-address path below. Otherwise a
        // user recovering from a typoed address (whose link sits in a
        // stranger's inbox) would believe the retry disarmed it when the
        // retry hit a taken address.
        await client.query(
          `
            UPDATE public.user_auth_tokens
            SET consumed_at = now()
            WHERE user_id = $1::uuid
              AND purpose = 'email_change'
              AND consumed_at IS NULL
          `,
          [userId]
        );

        const taken = await client.query(
          `
            SELECT 1
            FROM public.users
            WHERE email = $1::citext
              AND deleted_at IS NULL
          `,
          [newEmail]
        );
        if ((taken.rowCount ?? 0) > 0) {
          // Do not reveal that the address belongs to another account: report
          // success and send nothing, mirroring forgot-password's
          // no-enumeration behavior.
          await client.query("COMMIT");
          return;
        }

        token = generateAuthToken();
        await issueUserAuthToken(client, {
          userId,
          tokenHash: token.tokenHash,
          purpose: "email_change",
          newEmail,
          expiresAt: new Date(Date.now() + emailChangeTtlSeconds * 1000),
        });
        await client.query("COMMIT");
      } catch (error) {
        await rollbackQuietly(client);
        throw error;
      } finally {
        client.release();
      }

      // Mail the NEW address: consuming the link proves control of it.
      await options.mailer.sendEmailChangeEmail({
        email: newEmail,
        linkUrl: toEmailChangeLink(publicBaseUrl, token.rawToken),
      });
    },

    async verifyEmailChange(input) {
      if (typeof input.token !== "string" || input.token.trim().length === 0) {
        throw new TypeError("token must be a non-empty string");
      }

      const client = await options.db.connect();
      try {
        await client.query("BEGIN");
        const consumed = await consumeUserAuthToken(client, {
          token: input.token,
          purpose: "email_change",
          now: new Date(),
        });
        if (!consumed || !consumed.newEmail) {
          throw new TypeError("Email change token is invalid or expired");
        }

        // The link landed in the new inbox, so the new address is verified.
        const updated = await client.query(
          `
            UPDATE public.users
            SET email = $2::citext,
                email_verified = true,
                updated_at = now()
            WHERE id = $1::uuid
              AND deleted_at IS NULL
          `,
          [consumed.userId, consumed.newEmail]
        );
        if (updated.rowCount !== 1) {
          throw new Error("Failed to update user email");
        }

        await client.query("COMMIT");
      } catch (error) {
        await rollbackQuietly(client);
        // Another account claimed the address between request and confirm.
        if ((error as { code?: string }).code === "23505") {
          throw new TypeError("Email change token is invalid or expired");
        }
        throw error;
      } finally {
        client.release();
      }
    },

    async deleteAccount(input) {
      const userId = normalizeUserId(input.userId);
      if (typeof input.password !== "string" || input.password.length === 0) {
        throw new TypeError("password must be a non-empty string");
      }

      const client = await options.db.connect();
      try {
        await client.query("BEGIN");
        const user = await findActiveUserByIdForUpdate(client, userId);
        if (!user || !(await verifyPassword(user.password_hash, input.password))) {
          throw new TypeError("Password is incorrect");
        }

        // Soft delete. The partial unique index on users(email) only covers
        // deleted_at IS NULL rows, so the address frees up for re-signup.
        // Every reader and sender in the app filters deleted_at IS NULL.
        await client.query(
          `
            UPDATE public.users
            SET deleted_at = now(),
                updated_at = now()
            WHERE id = $1::uuid
              AND deleted_at IS NULL
          `,
          [userId]
        );

        // Void outstanding tokens so a pre-delete reset/verify link cannot
        // act on the corpse.
        await client.query(
          `
            UPDATE public.user_auth_tokens
            SET consumed_at = now()
            WHERE user_id = $1::uuid
              AND consumed_at IS NULL
          `,
          [userId]
        );
        await client.query("COMMIT");
      } catch (error) {
        await rollbackQuietly(client);
        throw error;
      } finally {
        client.release();
      }

      await destroyAuthSessionsByUserId(options.redis, { userId });
    },

    async logoutAll(input) {
      const userId = normalizeUserId(input.userId);
      await destroyAuthSessionsByUserId(options.redis, { userId });
    },
  };
}
