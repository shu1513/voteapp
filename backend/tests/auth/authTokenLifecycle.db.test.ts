import { Pool } from "pg";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";

import { createAuthService } from "../../src/auth/authService.js";
import { CURRENT_TERMS_VERSION } from "../../src/constants/legal.js";

/**
 * Live round-trip for the auth-token lifecycle: a link issued under one
 * owner or credential must stop working the moment an ownership or
 * credential change commits, and token consumers must take the user-row
 * lock BEFORE consuming so they cannot deadlock against those changes.
 *
 * The sequences below are exactly the ones a unit test with ordered mocks
 * cannot prove (the mocks return whatever the test says): whether the
 * voiding UPDATE actually hits the rows a later consume would otherwise
 * accept, and whether a consumer blocked on FOR UPDATE observes the void
 * after the lock wait instead of the stale pre-lock view.
 *
 * Needs a live Postgres (DATABASE_URL) with migrations applied. CI runs it in
 * the migrate job, which provides one; the unit-test job skips it.
 */

const databaseUrl = process.env.DATABASE_URL;

const VICTIM_EMAIL = "token-lifecycle-victim@example.test";
const ATTACKER_EMAIL = "token-lifecycle-attacker@example.test";
const GOOGLE_EMAIL = "token-lifecycle-google@gmail.com";
const TEST_EMAILS = [VICTIM_EMAIL, ATTACKER_EMAIL, GOOGLE_EMAIL];
const PASSWORD_A = "attacker-password-123";
const PASSWORD_B = "victim-password-456";

function createRedisFake() {
  const values = new Map<string, string>();
  const sets = new Map<string, Set<string>>();
  return {
    get: async (key: string) => values.get(key) ?? null,
    setEx: async (key: string, _seconds: number, value: string) => {
      values.set(key, value);
      return "OK";
    },
    del: async (key: string) => (values.delete(key) ? 1 : 0),
    sAdd: async (key: string, member: string) => {
      const set = sets.get(key) ?? new Set<string>();
      set.add(member);
      sets.set(key, set);
      return 1;
    },
    sRem: async (key: string, member: string) => (sets.get(key)?.delete(member) ? 1 : 0),
    sMembers: async (key: string) => [...(sets.get(key) ?? [])],
    expire: async () => 1,
  };
}

function tokenFromLink(linkUrl: string): string {
  const token = new URL(linkUrl).searchParams.get("token");
  if (!token) throw new Error(`No token in link: ${linkUrl}`);
  return token;
}

describe.skipIf(!databaseUrl)("auth-token lifecycle (requires DATABASE_URL)", () => {
  let pool: Pool;
  const mailer = {
    sendVerificationEmail: vi.fn().mockResolvedValue(undefined),
    sendPasswordResetEmail: vi.fn().mockResolvedValue(undefined),
    sendEmailChangeEmail: vi.fn().mockResolvedValue(undefined),
  };
  const service = () =>
    createAuthService({
      db: pool,
      redis: createRedisFake(),
      mailer,
      publicBaseUrl: "https://example.test",
      verifyGoogleIdToken: async () => ({
        sub: "token-lifecycle-google-sub",
        email: GOOGLE_EMAIL,
        email_verified: true,
        given_name: "Goo",
      }),
    });

  async function cleanup(): Promise<void> {
    // Tokens cascade from the users row.
    await pool.query("DELETE FROM public.users WHERE email = ANY($1::citext[])", [TEST_EMAILS]);
  }

  async function userIdByEmail(email: string): Promise<string> {
    const result = await pool.query<{ id: string }>(
      "SELECT id::text AS id FROM public.users WHERE email = $1::citext AND deleted_at IS NULL",
      [email]
    );
    const id = result.rows[0]?.id;
    if (!id) throw new Error(`No user ${email}`);
    return id;
  }

  /** The last link mailed by the given mailer method. */
  function lastLink(send: typeof mailer.sendVerificationEmail): string {
    const call = send.mock.calls.at(-1)?.[0] as { linkUrl: string } | undefined;
    if (!call) throw new Error("No mail sent");
    return tokenFromLink(call.linkUrl);
  }

  beforeAll(async () => {
    pool = new Pool({ connectionString: databaseUrl, max: 4 });
    await cleanup();
  });

  afterEach(async () => {
    mailer.sendVerificationEmail.mockClear();
    mailer.sendPasswordResetEmail.mockClear();
    mailer.sendEmailChangeEmail.mockClear();
    await cleanup();
  });

  afterAll(async () => {
    await pool.end();
  });

  it("re-registration + verification kill a pre-registrant's email-change link", async () => {
    const auth = service();
    // Attacker pre-registers the victim's address and requests an email
    // change to their own inbox (allowed while unverified: typo-fix flow).
    await auth.register({ email: VICTIM_EMAIL, password: PASSWORD_A, acceptedTermsVersion: CURRENT_TERMS_VERSION });
    const userId = await userIdByEmail(VICTIM_EMAIL);
    await auth.requestEmailChange({ userId, newEmail: ATTACKER_EMAIL, password: PASSWORD_A });
    const attackerChangeToken = lastLink(mailer.sendEmailChangeEmail);

    // Victim registers (replaces password, bumps epoch, voids the link) and
    // verifies with THEIR link.
    await auth.register({ email: VICTIM_EMAIL, password: PASSWORD_B, acceptedTermsVersion: CURRENT_TERMS_VERSION });
    await auth.verifyEmail({ token: lastLink(mailer.sendVerificationEmail) });

    await expect(auth.verifyEmailChange({ token: attackerChangeToken })).rejects.toThrow(
      "Email change token is invalid or expired"
    );
    const row = await pool.query<{ email: string; email_verified: boolean }>(
      "SELECT email::text AS email, email_verified FROM public.users WHERE id = $1::uuid",
      [userId]
    );
    expect(row.rows[0]).toEqual({ email: VICTIM_EMAIL, email_verified: true });
  });

  it("a legitimate unverified typo fix still works", async () => {
    const auth = service();
    await auth.register({ email: ATTACKER_EMAIL, password: PASSWORD_A, acceptedTermsVersion: CURRENT_TERMS_VERSION });
    const userId = await userIdByEmail(ATTACKER_EMAIL);
    await auth.requestEmailChange({ userId, newEmail: VICTIM_EMAIL, password: PASSWORD_A });

    await auth.verifyEmailChange({ token: lastLink(mailer.sendEmailChangeEmail) });

    const row = await pool.query<{ email: string; email_verified: boolean }>(
      "SELECT email::text AS email, email_verified FROM public.users WHERE id = $1::uuid",
      [userId]
    );
    expect(row.rows[0]).toEqual({ email: VICTIM_EMAIL, email_verified: true });
  });

  it("changePassword and resetPassword void the other-purpose links; verified links stay consumable once", async () => {
    const auth = service();
    await auth.register({ email: VICTIM_EMAIL, password: PASSWORD_A, acceptedTermsVersion: CURRENT_TERMS_VERSION });
    await auth.verifyEmail({ token: lastLink(mailer.sendVerificationEmail) });
    const userId = await userIdByEmail(VICTIM_EMAIL);

    // Outstanding reset + email-change links, then a password change.
    await auth.forgotPassword({ email: VICTIM_EMAIL });
    const staleReset = lastLink(mailer.sendPasswordResetEmail);
    await auth.requestEmailChange({ userId, newEmail: ATTACKER_EMAIL, password: PASSWORD_A });
    const staleChange = lastLink(mailer.sendEmailChangeEmail);
    await auth.changePassword({ userId, currentPassword: PASSWORD_A, newPassword: PASSWORD_B });

    await expect(auth.resetPassword({ token: staleReset, password: "another-password-789" })).rejects.toThrow(
      "Password reset token is invalid or expired"
    );
    await expect(auth.verifyEmailChange({ token: staleChange })).rejects.toThrow(
      "Email change token is invalid or expired"
    );

    // A fresh reset works exactly once and voids a newer email-change link.
    await auth.requestEmailChange({ userId, newEmail: ATTACKER_EMAIL, password: PASSWORD_B });
    const changeAfterReset = lastLink(mailer.sendEmailChangeEmail);
    await auth.forgotPassword({ email: VICTIM_EMAIL });
    const freshReset = lastLink(mailer.sendPasswordResetEmail);
    await auth.resetPassword({ token: freshReset, password: "another-password-789" });
    await expect(auth.resetPassword({ token: freshReset, password: "yet-another-password-0" })).rejects.toThrow(
      "Password reset token is invalid or expired"
    );
    await expect(auth.verifyEmailChange({ token: changeAfterReset })).rejects.toThrow(
      "Email change token is invalid or expired"
    );
    await expect(auth.login({ email: VICTIM_EMAIL, password: "another-password-789" })).resolves.toMatchObject({
      sessionId: expect.any(String),
    });
  });

  it("a Google signup takeover voids every link the pre-registrant requested", async () => {
    const auth = service();
    await auth.register({ email: GOOGLE_EMAIL, password: PASSWORD_A, acceptedTermsVersion: CURRENT_TERMS_VERSION });
    const userId = await userIdByEmail(GOOGLE_EMAIL);
    await auth.requestEmailChange({ userId, newEmail: ATTACKER_EMAIL, password: PASSWORD_A });
    const staleChange = lastLink(mailer.sendEmailChangeEmail);
    await auth.forgotPassword({ email: GOOGLE_EMAIL });
    const staleReset = lastLink(mailer.sendPasswordResetEmail);

    await auth.loginWithGoogle!({ idToken: "stub", intent: "signup", acceptedTermsVersion: CURRENT_TERMS_VERSION });

    await expect(auth.verifyEmailChange({ token: staleChange })).rejects.toThrow(
      "Email change token is invalid or expired"
    );
    await expect(auth.resetPassword({ token: staleReset, password: "another-password-789" })).rejects.toThrow(
      "Password reset token is invalid or expired"
    );
    const outstanding = await pool.query<{ n: string }>(
      "SELECT count(*)::text AS n FROM public.user_auth_tokens WHERE user_id = $1::uuid AND consumed_at IS NULL",
      [userId]
    );
    expect(outstanding.rows[0]?.n).toBe("0");
  });

  it("a consumer blocked behind an ownership change sees the void after the lock wait (no deadlock)", async () => {
    const auth = service();
    await auth.register({ email: VICTIM_EMAIL, password: PASSWORD_A, acceptedTermsVersion: CURRENT_TERMS_VERSION });
    const userId = await userIdByEmail(VICTIM_EMAIL);
    await auth.requestEmailChange({ userId, newEmail: ATTACKER_EMAIL, password: PASSWORD_A });
    const changeToken = lastLink(mailer.sendEmailChangeEmail);

    // Simulate the re-registration transaction mid-flight: user row locked,
    // void not yet committed.
    const owner = await pool.connect();
    try {
      await owner.query("BEGIN");
      await owner.query("SELECT 1 FROM public.users WHERE id = $1::uuid FOR UPDATE", [userId]);

      // The consumer peeks (token still valid), then blocks on the user lock.
      let settled = false;
      const consume = auth.verifyEmailChange({ token: changeToken }).finally(() => {
        settled = true;
      });
      await new Promise((resolve) => setTimeout(resolve, 300));
      expect(settled).toBe(false);

      await owner.query(
        "UPDATE public.user_auth_tokens SET consumed_at = now() WHERE user_id = $1::uuid AND consumed_at IS NULL",
        [userId]
      );
      await owner.query("COMMIT");

      await expect(consume).rejects.toThrow("Email change token is invalid or expired");
    } finally {
      await owner.query("ROLLBACK").catch(() => undefined);
      owner.release();
    }
    const row = await pool.query<{ email: string }>(
      "SELECT email::text AS email FROM public.users WHERE id = $1::uuid",
      [userId]
    );
    expect(row.rows[0]?.email).toBe(VICTIM_EMAIL);
  });
});
