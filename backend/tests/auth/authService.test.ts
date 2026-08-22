import { beforeAll, describe, expect, it, vi } from "vitest";

import { createAuthService } from "../../src/auth/authService.js";
import { CURRENT_TERMS_VERSION, GRACE_TERMS_VERSIONS } from "../../src/constants/legal.js";
import { hashPassword } from "../../src/auth/authPrimitives.js";

const USER_ID = "11111111-1111-4111-8111-111111111111";
const CURRENT_PASSWORD = "correct-password-123";
let currentPasswordHash: string;

beforeAll(async () => {
  currentPasswordHash = await hashPassword(CURRENT_PASSWORD);
});

function createDbClientMock() {
  return {
    query: vi.fn(),
    release: vi.fn(),
  };
}

function createDbMock(client: ReturnType<typeof createDbClientMock>) {
  return {
    connect: vi.fn().mockResolvedValue(client),
    query: vi.fn(),
  };
}

function createRedisMock() {
  return {
    get: vi.fn().mockResolvedValue(null),
    setEx: vi.fn().mockResolvedValue("OK"),
    del: vi.fn().mockResolvedValue(1),
    sAdd: vi.fn().mockResolvedValue(1),
    sRem: vi.fn().mockResolvedValue(1),
    sMembers: vi.fn().mockResolvedValue(["deadbeef"]),
    expire: vi.fn().mockResolvedValue(1),
  };
}

function createMailerMock() {
  return {
    sendVerificationEmail: vi.fn().mockResolvedValue(undefined),
    sendPasswordResetEmail: vi.fn().mockResolvedValue(undefined),
    sendEmailChangeEmail: vi.fn().mockResolvedValue(undefined),
  };
}

function userRow(overrides: Record<string, unknown> = {}) {
  return {
    id: USER_ID,
    email: "user@example.com",
    first_name: "User",
    password_hash: currentPasswordHash,
    email_verified: true,
    session_epoch: 1,
    ...overrides,
  };
}

describe("createAuthService resendVerification", () => {
  it("resends verification emails for unverified users", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({
        rows: [
          {
            id: "11111111-1111-4111-8111-111111111111",
            email: "user@example.com",
            first_name: "User",
            password_hash: "$argon2id$v=19$m=19456,t=3,p=1$dummy$dummy",
            email_verified: false,
          },
        ],
      })
      .mockResolvedValueOnce({ rows: [] }) // void outstanding same-purpose tokens
      .mockResolvedValueOnce({ rows: [{ id: "token-id" }] })
      .mockResolvedValueOnce({ rows: [] }); // COMMIT

    const mailer = createMailerMock();

    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: {} as never,
      mailer,
      publicBaseUrl: "https://example.com",
    });

    await service.resendVerification({
      email: "user@example.com",
    });

    expect(mailer.sendVerificationEmail).toHaveBeenCalledWith({
      email: "user@example.com",
      linkUrl: expect.stringContaining("https://example.com/verify-email?token="),
    });
    expect(client.query).toHaveBeenCalledWith("BEGIN");
    expect(client.release).toHaveBeenCalled();
  });

  it("does nothing for verified or missing users", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [] }) // user lookup
      .mockResolvedValueOnce({ rows: [] }); // COMMIT

    const mailer = createMailerMock();

    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: {} as never,
      mailer,
      publicBaseUrl: "https://example.com",
    });

    await service.resendVerification({
      email: "missing@example.com",
    });

    expect(mailer.sendVerificationEmail).not.toHaveBeenCalled();
    expect(client.query).toHaveBeenCalledWith("BEGIN");
    expect(client.release).toHaveBeenCalled();
  });
});

describe("createAuthService register terms acceptance", () => {
  it("stores the accepted terms version on the inserted user row", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [] }) // user lookup: none
      .mockResolvedValueOnce({ rows: [userRow({ email_verified: false })] }) // INSERT user
      .mockResolvedValueOnce({ rows: [] }) // INSERT terms acceptance
      .mockResolvedValueOnce({ rows: [] }) // void outstanding tokens
      .mockResolvedValueOnce({ rows: [{ id: "token-id" }] }) // INSERT token
      .mockResolvedValueOnce({ rows: [] }); // COMMIT

    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: {} as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
    });

    await service.register({
      email: "new@example.com",
      password: "correct horse battery staple",
      acceptedTermsVersion: CURRENT_TERMS_VERSION,
    });

    const insertCall = client.query.mock.calls.find((call) => String(call[0]).includes("INSERT INTO public.users"));
    expect(String(insertCall?.[0])).toContain("accepted_terms_version");
    expect(String(insertCall?.[0])).toContain("accepted_terms_at");
    expect(insertCall?.[1]?.[3]).toBe(CURRENT_TERMS_VERSION);

    // The history row goes down in the same transaction as the users row, so
    // the account can never claim a version with nothing behind it.
    const ledgerCall = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.user_terms_acceptances")
    );
    expect(ledgerCall?.[1]).toEqual([USER_ID, CURRENT_TERMS_VERSION, "registration"]);
  });

  it("stamps acceptance on the unverified-refresh update path too", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [userRow({ email_verified: false })] }) // existing unverified user
      .mockResolvedValueOnce({ rows: [userRow({ email_verified: false })] }) // UPDATE refresh
      .mockResolvedValueOnce({ rows: [] }) // INSERT terms acceptance
      .mockResolvedValueOnce({ rows: [] }) // void outstanding tokens
      .mockResolvedValueOnce({ rows: [{ id: "token-id" }] }) // INSERT token
      .mockResolvedValueOnce({ rows: [] }); // COMMIT

    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: {} as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
    });

    await service.register({
      email: "user@example.com",
      password: "correct horse battery staple",
      acceptedTermsVersion: CURRENT_TERMS_VERSION,
    });

    const updateCall = client.query.mock.calls.find((call) => String(call[0]).includes("UPDATE public.users"));
    expect(String(updateCall?.[0])).toContain("accepted_terms_version = $4");
    expect(String(updateCall?.[0])).toContain("accepted_terms_at = now()");
    expect(updateCall?.[1]?.[3]).toBe(CURRENT_TERMS_VERSION);

    const ledgerCall = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.user_terms_acceptances")
    );
    expect(ledgerCall?.[1]).toEqual([USER_ID, CURRENT_TERMS_VERSION, "registration"]);
  });

  it("records nothing when the address already belongs to a verified account", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [userRow({ email_verified: true })] }) // existing VERIFIED user
      .mockResolvedValueOnce({ rows: [] }); // COMMIT

    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: {} as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
    });

    await service.register({
      email: "user@example.com",
      password: "correct horse battery staple",
      acceptedTermsVersion: CURRENT_TERMS_VERSION,
    });

    // Re-registering a verified address is a silent no-op that must not reveal
    // the address is taken. Whoever submitted the form has not been shown to
    // control the account, so recording an acceptance against it would file a
    // stranger's assent in the account holder's history.
    const ledgerCall = client.query.mock.calls.find((call) =>
      String(call[0]).includes("INSERT INTO public.user_terms_acceptances")
    );
    expect(ledgerCall).toBeUndefined();
  });

  it("rejects blank or stale terms versions before touching the database", async () => {
    const client = createDbClientMock();
    const db = createDbMock(client);

    const service = createAuthService({
      db: db as never,
      redis: {} as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
    });

    await expect(
      service.register({
        email: "new@example.com",
        password: "correct horse battery staple",
        acceptedTermsVersion: "   ",
      })
    ).rejects.toThrow("accepted terms version");
    // Defense-in-depth: even a direct caller bypassing the API layer cannot
    // persist acceptance of a superseded version.
    await expect(
      service.register({
        email: "new@example.com",
        password: "correct horse battery staple",
        acceptedTermsVersion: "0.9",
      })
    ).rejects.toThrow("accepted terms version");
    expect(db.connect).not.toHaveBeenCalled();
  });

  it("accepts and records a grace terms version during a bump rollout", async () => {
    const graceVersion = GRACE_TERMS_VERSIONS[0];
    if (graceVersion === undefined) {
      // Grace list empty between rollouts — nothing to verify.
      return;
    }
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [] }) // user lookup: none
      .mockResolvedValueOnce({ rows: [userRow({ email_verified: false })] }) // INSERT user
      .mockResolvedValueOnce({ rows: [] }) // INSERT terms acceptance
      .mockResolvedValueOnce({ rows: [] }) // void outstanding tokens
      .mockResolvedValueOnce({ rows: [{ id: "token-id" }] }) // INSERT token
      .mockResolvedValueOnce({ rows: [] }); // COMMIT
    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: {} as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
    });

    await service.register({
      email: "new@example.com",
      password: "correct horse battery staple",
      acceptedTermsVersion: graceVersion,
    });

    // The stored value is the evidentiary record of what the stale bundle
    // actually showed — the grace version, never silently upgraded.
    const insertCall = client.query.mock.calls.find((call) => String(call[0]).includes("INSERT INTO public.users"));
    expect(insertCall?.[1]?.[3]).toBe(graceVersion);
  });
});

describe("createAuthService changePassword", () => {
  it("updates the hash, rotates every session, and returns a fresh one", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [userRow()] }) // user FOR UPDATE
      .mockResolvedValueOnce({ rows: [{ session_epoch: 2 }], rowCount: 1 }) // UPDATE password_hash + epoch bump
      .mockResolvedValueOnce({ rows: [] }); // COMMIT
    const redis = createRedisMock();

    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: redis as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
    });

    const result = await service.changePassword({
      userId: USER_ID,
      currentPassword: CURRENT_PASSWORD,
      newPassword: "brand-new-password-456",
    });

    expect(result.sessionId).toEqual(expect.any(String));
    const updateCall = client.query.mock.calls.find((call) => String(call[0]).includes("SET password_hash"));
    expect(updateCall?.[1]?.[0]).toBe(USER_ID);
    // All old sessions die (user-set sweep), fresh session created after.
    expect(redis.sMembers).toHaveBeenCalled();
    expect(redis.setEx).toHaveBeenCalled();
    expect(client.query).toHaveBeenCalledWith("COMMIT");
  });

  it("rejects a wrong current password without updating", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [userRow()] }); // user FOR UPDATE
    const redis = createRedisMock();

    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: redis as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
    });

    await expect(
      service.changePassword({
        userId: USER_ID,
        currentPassword: "wrong-password-000",
        newPassword: "brand-new-password-456",
      })
    ).rejects.toThrow("Current password is incorrect");

    expect(client.query.mock.calls.some((call) => String(call[0]).includes("SET password_hash"))).toBe(false);
    expect(redis.sMembers).not.toHaveBeenCalled();
    expect(client.query).toHaveBeenCalledWith("ROLLBACK");
  });

  it("rejects a policy-violating new password before touching the database", async () => {
    const client = createDbClientMock();
    const db = createDbMock(client);

    const service = createAuthService({
      db: db as never,
      redis: createRedisMock() as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
    });

    await expect(
      service.changePassword({
        userId: USER_ID,
        currentPassword: CURRENT_PASSWORD,
        newPassword: "short",
      })
    ).rejects.toThrow();
    expect(db.connect).not.toHaveBeenCalled();
  });
});

describe("createAuthService requestEmailChange", () => {
  it("issues an email_change token and mails the NEW address", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [userRow()] }) // user FOR UPDATE
      .mockResolvedValueOnce({ rows: [] }) // up-front void of outstanding change tokens
      .mockResolvedValueOnce({ rows: [], rowCount: 0 }) // taken check: free
      .mockResolvedValueOnce({ rows: [] }) // issueUserAuthToken's own void
      .mockResolvedValueOnce({
        rows: [
          {
            id: "token-id",
            user_id: USER_ID,
            token_hash: "a".repeat(64),
            purpose: "email_change",
            new_email: "new@example.com",
            expires_at: new Date(),
            consumed_at: null,
            created_at: new Date(),
          },
        ],
      }) // INSERT token
      .mockResolvedValueOnce({ rows: [] }); // COMMIT
    const mailer = createMailerMock();

    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: createRedisMock() as never,
      mailer,
      publicBaseUrl: "https://example.com",
    });

    await service.requestEmailChange({
      userId: USER_ID,
      newEmail: "new@example.com",
      password: CURRENT_PASSWORD,
    });

    expect(mailer.sendEmailChangeEmail).toHaveBeenCalledWith({
      email: "new@example.com",
      linkUrl: expect.stringContaining("https://example.com/verify-email-change?token="),
    });
    const insertCall = client.query.mock.calls.find((call) => String(call[0]).includes("INSERT INTO public.user_auth_tokens"));
    expect(insertCall?.[1]).toEqual([USER_ID, expect.any(String), "email_change", "new@example.com", expect.any(Date)]);
  });

  it("reports success but sends nothing when the address is taken (no enumeration)", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [userRow()] }) // user FOR UPDATE
      .mockResolvedValueOnce({ rows: [] }) // up-front void of outstanding change tokens
      .mockResolvedValueOnce({ rows: [{ "?column?": 1 }], rowCount: 1 }) // taken
      .mockResolvedValueOnce({ rows: [] }); // COMMIT
    const mailer = createMailerMock();

    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: createRedisMock() as never,
      mailer,
      publicBaseUrl: "https://example.com",
    });

    await expect(
      service.requestEmailChange({
        userId: USER_ID,
        newEmail: "taken@example.com",
        password: CURRENT_PASSWORD,
      })
    ).resolves.toBeUndefined();

    expect(mailer.sendEmailChangeEmail).not.toHaveBeenCalled();
    expect(client.query.mock.calls.some((call) => String(call[0]).includes("INSERT INTO public.user_auth_tokens"))).toBe(false);
    // The taken path must still disarm older change links: a typo-recovery
    // retry that lands on a taken address may not leave the typo link live.
    const voidCall = client.query.mock.calls.find((call) =>
      String(call[0]).includes("SET consumed_at = now()")
    );
    expect(voidCall?.[1]).toEqual([USER_ID]);
    expect(client.query).toHaveBeenCalledWith("COMMIT");
  });

  it("rejects a wrong password and a same-as-current email", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [userRow()] });
    const mailer = createMailerMock();

    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: createRedisMock() as never,
      mailer,
      publicBaseUrl: "https://example.com",
    });

    await expect(
      service.requestEmailChange({
        userId: USER_ID,
        newEmail: "new@example.com",
        password: "wrong-password-000",
      })
    ).rejects.toThrow("Password is incorrect");

    client.query.mockReset();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [userRow()] });
    await expect(
      service.requestEmailChange({
        userId: USER_ID,
        newEmail: "USER@example.com",
        password: CURRENT_PASSWORD,
      })
    ).rejects.toThrow("New email must be different from the current email");
    expect(mailer.sendEmailChangeEmail).not.toHaveBeenCalled();
  });
});

describe("createAuthService verifyEmailChange", () => {
  it("swaps the email and marks it verified", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({
        rows: [
          {
            id: "token-id",
            user_id: USER_ID,
            token_hash: "a".repeat(64),
            purpose: "email_change",
            new_email: "new@example.com",
            expires_at: new Date(),
            consumed_at: new Date(),
            created_at: new Date(),
          },
        ],
      }) // consume token
      .mockResolvedValueOnce({ rows: [], rowCount: 1 }) // UPDATE users
      .mockResolvedValueOnce({ rows: [] }); // COMMIT

    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: createRedisMock() as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
    });

    await service.verifyEmailChange({ token: "raw-token" });

    const updateCall = client.query.mock.calls.find((call) => String(call[0]).includes("SET email ="));
    expect(String(updateCall?.[0])).toContain("email_verified = true");
    expect(updateCall?.[1]).toEqual([USER_ID, "new@example.com"]);
    expect(client.query).toHaveBeenCalledWith("COMMIT");
  });

  // Stripe keeps prefilling Checkout and sending receipts to the customer
  // object's stored email, so a verified change pushes the new address there
  // — best-effort, after the commit.
  it("syncs the membership customer email after a successful change", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({
        rows: [
          {
            id: "token-id",
            user_id: USER_ID,
            token_hash: "a".repeat(64),
            purpose: "email_change",
            new_email: "new@example.com",
            expires_at: new Date(),
            consumed_at: new Date(),
            created_at: new Date(),
          },
        ],
      }) // consume token
      .mockResolvedValueOnce({ rows: [], rowCount: 1 }) // UPDATE users
      .mockResolvedValueOnce({ rows: [] }); // COMMIT
    const syncMembershipCustomerEmail = vi.fn().mockResolvedValue(undefined);

    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: createRedisMock() as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
      syncMembershipCustomerEmail,
    });

    await service.verifyEmailChange({ token: "raw-token" });
    expect(syncMembershipCustomerEmail).toHaveBeenCalledWith(USER_ID);
  });

  it("never fails the committed email change over a Stripe sync failure, and skips the sync on failure", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const client = createDbClientMock();
      client.query
        .mockResolvedValueOnce({ rows: [] }) // BEGIN
        .mockResolvedValueOnce({
          rows: [
            {
              id: "token-id",
              user_id: USER_ID,
              token_hash: "a".repeat(64),
              purpose: "email_change",
              new_email: "new@example.com",
              expires_at: new Date(),
              consumed_at: new Date(),
              created_at: new Date(),
            },
          ],
        }) // consume token
        .mockResolvedValueOnce({ rows: [], rowCount: 1 }) // UPDATE users
        .mockResolvedValueOnce({ rows: [] }); // COMMIT
      const syncMembershipCustomerEmail = vi.fn().mockRejectedValue(new Error("stripe down"));

      const service = createAuthService({
        db: createDbMock(client) as never,
        redis: createRedisMock() as never,
        mailer: createMailerMock(),
        publicBaseUrl: "https://example.com",
        syncMembershipCustomerEmail,
      });

      // The change itself committed; the sync failure only warns.
      await expect(service.verifyEmailChange({ token: "raw-token" })).resolves.toBeUndefined();

      // A failed (invalid-token) change never calls the sync.
      const failClient = createDbClientMock();
      failClient.query
        .mockResolvedValueOnce({ rows: [] }) // BEGIN
        .mockResolvedValueOnce({ rows: [] }); // consume: nothing
      const failSync = vi.fn();
      const failService = createAuthService({
        db: createDbMock(failClient) as never,
        redis: createRedisMock() as never,
        mailer: createMailerMock(),
        publicBaseUrl: "https://example.com",
        syncMembershipCustomerEmail: failSync,
      });
      await expect(failService.verifyEmailChange({ token: "bogus" })).rejects.toThrow(
        "Email change token is invalid or expired"
      );
      expect(failSync).not.toHaveBeenCalled();
    } finally {
      warnSpy.mockRestore();
    }
  });

  it("rejects invalid tokens and hides a lost unique race behind the same message", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [] }); // consume: nothing

    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: createRedisMock() as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
    });

    await expect(service.verifyEmailChange({ token: "bogus" })).rejects.toThrow(
      "Email change token is invalid or expired"
    );

    client.query.mockReset();
    const uniqueViolation = Object.assign(new Error("duplicate key"), { code: "23505" });
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({
        rows: [
          {
            id: "token-id",
            user_id: USER_ID,
            token_hash: "a".repeat(64),
            purpose: "email_change",
            new_email: "new@example.com",
            expires_at: new Date(),
            consumed_at: new Date(),
            created_at: new Date(),
          },
        ],
      })
      .mockRejectedValueOnce(uniqueViolation); // UPDATE users hits uq_users_email_active

    await expect(service.verifyEmailChange({ token: "raw-token" })).rejects.toThrow(
      "Email change token is invalid or expired"
    );
  });
});

describe("createAuthService deleteAccount", () => {
  it("hard-deletes the user row, scrubs surviving tables, and destroys all sessions", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [userRow()] }) // user FOR UPDATE
      .mockResolvedValueOnce({ rows: [] }) // clear content_report emails
      .mockResolvedValueOnce({ rows: [] }) // delete pending push receipts
      .mockResolvedValueOnce({ rows: [], rowCount: 1 }) // hard delete
      .mockResolvedValueOnce({ rows: [] }); // COMMIT
    const redis = createRedisMock();

    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: redis as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
    });

    await service.deleteAccount({ userId: USER_ID, password: CURRENT_PASSWORD });

    // Permanent removal, not a soft delete: associated rows (districts,
    // follows, preferences, tokens, push tokens, notification history) go
    // with the row via ON DELETE CASCADE.
    const deleteCall = client.query.mock.calls.find((call) => String(call[0]).includes("DELETE FROM public.users"));
    expect(deleteCall?.[1]).toEqual([USER_ID]);
    expect(client.query.mock.calls.some((call) => String(call[0]).includes("SET deleted_at"))).toBe(false);

    // The two tables that outlive the user row are scrubbed before it goes:
    // content reports keep the message but lose the pre-filled account
    // email, and pending push receipts lose the device token.
    const callOrder = client.query.mock.calls.map((call) => String(call[0]));
    const clearEmailIndex = callOrder.findIndex((sql) => sql.includes("SET reporter_email = NULL"));
    const receiptsIndex = callOrder.findIndex((sql) =>
      sql.includes("DELETE FROM public.user_push_notification_receipts")
    );
    const userDeleteIndex = callOrder.findIndex((sql) => sql.includes("DELETE FROM public.users"));
    expect(clearEmailIndex).toBeGreaterThan(-1);
    expect(receiptsIndex).toBeGreaterThan(-1);
    expect(clearEmailIndex).toBeLessThan(userDeleteIndex);
    expect(receiptsIndex).toBeLessThan(userDeleteIndex);
    expect(client.query.mock.calls[clearEmailIndex]?.[1]).toEqual([USER_ID]);
    expect(client.query.mock.calls[receiptsIndex]?.[1]).toEqual([USER_ID]);

    expect(client.query).toHaveBeenCalledWith("COMMIT");
    expect(redis.sMembers).toHaveBeenCalled();
  });

  it("rejects a wrong password without deleting", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [userRow()] });
    const redis = createRedisMock();

    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: redis as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
    });

    await expect(service.deleteAccount({ userId: USER_ID, password: "wrong-password-000" })).rejects.toThrow(
      "Password is incorrect"
    );
    expect(client.query.mock.calls.some((call) => String(call[0]).includes("DELETE FROM public.users"))).toBe(false);
    expect(redis.sMembers).not.toHaveBeenCalled();
  });

  // Membership cancellation is a deletion PRECONDITION
  // (docs/plans/membership-contributions.md): password-checked first so a
  // wrong password can't cancel a paid membership, run outside the delete
  // transaction, and a failure fails the whole delete with nothing removed.
  it("cancels the membership after a password check and before the delete transaction", async () => {
    const client = createDbClientMock();
    client.query
      // Password pre-check transaction.
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [userRow()] }) // user FOR UPDATE
      .mockResolvedValueOnce({ rows: [] }) // ROLLBACK (pure check)
      // Delete transaction.
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [userRow()] }) // user FOR UPDATE
      .mockResolvedValueOnce({ rows: [] }) // clear content_report emails
      .mockResolvedValueOnce({ rows: [] }) // delete pending push receipts
      .mockResolvedValueOnce({ rows: [], rowCount: 1 }) // hard delete
      .mockResolvedValueOnce({ rows: [] }); // COMMIT
    const redis = createRedisMock();
    const cancelMembershipForAccountDeletion = vi.fn(async () => {
      // The Stripe call must happen before any row is touched.
      expect(client.query.mock.calls.some((call) => String(call[0]).includes("DELETE FROM public.users"))).toBe(false);
    });

    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: redis as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
      cancelMembershipForAccountDeletion,
    });

    await service.deleteAccount({ userId: USER_ID, password: CURRENT_PASSWORD });

    expect(cancelMembershipForAccountDeletion).toHaveBeenCalledWith(USER_ID);
    expect(client.query.mock.calls.some((call) => String(call[0]).includes("DELETE FROM public.users"))).toBe(true);
    expect(client.query).toHaveBeenCalledWith("COMMIT");
  });

  it("does not cancel the membership when the password is wrong", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN (pre-check)
      .mockResolvedValueOnce({ rows: [userRow()] }); // user FOR UPDATE
    const cancelMembershipForAccountDeletion = vi.fn();

    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: createRedisMock() as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
      cancelMembershipForAccountDeletion,
    });

    await expect(service.deleteAccount({ userId: USER_ID, password: "wrong-password-000" })).rejects.toThrow(
      "Password is incorrect"
    );
    expect(cancelMembershipForAccountDeletion).not.toHaveBeenCalled();
    expect(client.query.mock.calls.some((call) => String(call[0]).includes("DELETE FROM public.users"))).toBe(false);
  });

  it("fails the delete and removes nothing when the membership cancel fails", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN (pre-check)
      .mockResolvedValueOnce({ rows: [userRow()] }) // user FOR UPDATE
      .mockResolvedValueOnce({ rows: [] }); // ROLLBACK
    const cancelMembershipForAccountDeletion = vi.fn().mockRejectedValue(new Error("stripe unreachable"));

    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: createRedisMock() as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
      cancelMembershipForAccountDeletion,
    });

    await expect(service.deleteAccount({ userId: USER_ID, password: CURRENT_PASSWORD })).rejects.toThrow(
      "stripe unreachable"
    );
    expect(client.query.mock.calls.some((call) => String(call[0]).includes("DELETE FROM public.users"))).toBe(false);
  });
});

describe("createAuthService logout", () => {
  it("destroys the current session", async () => {
    const redis = createRedisMock();

    const service = createAuthService({
      db: createDbMock(createDbClientMock()) as never,
      redis: redis as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
    });

    await service.logout({ currentSessionId: "session-1" });
    expect(redis.del).toHaveBeenCalled();
  });

  it("still resolves when Redis is down, so the API can clear the cookie", async () => {
    const redis = createRedisMock();
    redis.get.mockRejectedValue(new Error("redis down"));
    redis.del.mockRejectedValue(new Error("redis down"));

    const service = createAuthService({
      db: createDbMock(createDbClientMock()) as never,
      redis: redis as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
    });

    // A propagated Redis error would make the API skip the Set-Cookie clear;
    // the browser would then keep a cookie for a session the failed delete
    // left in Redis, and the logout would undo itself once Redis recovers.
    await expect(service.logout({ currentSessionId: "session-1" })).resolves.toBeUndefined();
  });
});

describe("createAuthService logoutAll", () => {
  it("destroys every session for the user", async () => {
    const redis = createRedisMock();

    const service = createAuthService({
      db: createDbMock(createDbClientMock()) as never,
      redis: redis as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
    });

    await service.logoutAll({ userId: USER_ID });
    expect(redis.sMembers).toHaveBeenCalled();
    expect(redis.del).toHaveBeenCalled();
  });

  it("rejects a non-UUID userId", async () => {
    const service = createAuthService({
      db: createDbMock(createDbClientMock()) as never,
      redis: createRedisMock() as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
    });

    await expect(service.logoutAll({ userId: "bob" })).rejects.toThrow("userId must be a UUID");
  });

  it("bumps the session epoch and still succeeds when the Redis sweep fails", async () => {
    const redis = createRedisMock();
    redis.sMembers.mockRejectedValue(new Error("redis down"));
    const db = createDbMock(createDbClientMock());

    const service = createAuthService({
      db: db as never,
      redis: redis as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
    });

    // The epoch bump lands on the pool before Redis is touched, so the DB
    // revocation is durable and the best-effort sweep failure must not
    // surface an error for a logout-all that already succeeded.
    await expect(service.logoutAll({ userId: USER_ID })).resolves.toBeUndefined();
    const bumpCall = db.query.mock.calls.find((call) => String(call[0]).includes("session_epoch = session_epoch + 1"));
    expect(bumpCall?.[1]).toEqual([USER_ID]);
  });
});

describe("createAuthService session epoch revocation", () => {
  function tokenRow() {
    return {
      id: "token-1",
      user_id: USER_ID,
      token_hash: "hash",
      purpose: "password_reset",
      new_email: null,
      expires_at: new Date(Date.now() + 60_000),
      consumed_at: null,
      created_at: new Date(),
    };
  }

  it("resetPassword bumps session_epoch in the same UPDATE as the password hash", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [tokenRow()] }) // consume reset token
      .mockResolvedValueOnce({ rows: [], rowCount: 1 }) // UPDATE users
      .mockResolvedValueOnce({ rows: [] }); // COMMIT
    const redis = createRedisMock();

    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: redis as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
    });

    await service.resetPassword({ token: "raw-token", password: "brand-new-password-456" });

    const updateCall = client.query.mock.calls.find((call) => String(call[0]).includes("SET password_hash"));
    expect(String(updateCall?.[0])).toContain("session_epoch = session_epoch + 1");
    expect(redis.sMembers).toHaveBeenCalled();
  });

  it("resetPassword still succeeds when the post-commit Redis sweep fails", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [tokenRow()] }) // consume reset token
      .mockResolvedValueOnce({ rows: [], rowCount: 1 }) // UPDATE users
      .mockResolvedValueOnce({ rows: [] }); // COMMIT
    const redis = createRedisMock();
    redis.sMembers.mockRejectedValue(new Error("redis down"));

    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: redis as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
    });

    // The epoch bump already revoked every session; a Redis failure must not
    // surface an error for a reset that committed (the token is consumed).
    await expect(
      service.resetPassword({ token: "raw-token", password: "brand-new-password-456" })
    ).resolves.toBeUndefined();
  });

  it("re-registering an unverified address bumps the epoch so pre-registration sessions die", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [userRow({ email_verified: false })] }) // existing unverified FOR UPDATE
      .mockResolvedValueOnce({ rows: [userRow({ email_verified: false, session_epoch: 2 })] }) // refresh UPDATE
      .mockResolvedValueOnce({ rows: [] }) // INSERT terms acceptance
      .mockResolvedValueOnce({ rows: [] }) // void outstanding tokens
      .mockResolvedValueOnce({ rows: [{ id: "token-id" }] }) // issue verification token
      .mockResolvedValueOnce({ rows: [] }); // COMMIT

    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: createRedisMock() as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
    });

    await service.register({
      email: "user@example.com",
      password: "brand-new-password-456",
      acceptedTermsVersion: CURRENT_TERMS_VERSION,
    });

    const refreshCall = client.query.mock.calls.find((call) => String(call[0]).includes("password_hash = $3"));
    expect(String(refreshCall?.[0])).toContain("session_epoch = session_epoch + 1");
  });

  it("verifyEmail bumps the epoch so pre-verification sessions cannot become verified ones", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({
        rows: [
          {
            id: "token-1",
            user_id: USER_ID,
            token_hash: "hash",
            purpose: "email_verify",
            new_email: null,
            expires_at: new Date(Date.now() + 60_000),
            consumed_at: null,
            created_at: new Date(),
          },
        ],
      }) // consume verify token
      .mockResolvedValueOnce({ rows: [], rowCount: 1 }) // UPDATE users
      .mockResolvedValueOnce({ rows: [] }); // COMMIT

    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: createRedisMock() as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
    });

    await service.verifyEmail({ token: "raw-token" });

    const verifyCall = client.query.mock.calls.find((call) => String(call[0]).includes("email_verified = true"));
    expect(String(verifyCall?.[0])).toContain("session_epoch = session_epoch + 1");
  });

  it("login stores the user's current session epoch in the new session", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [] }) // last_logged_in update
      .mockResolvedValueOnce({ rows: [] }); // COMMIT
    const db = createDbMock(client);
    db.query.mockResolvedValueOnce({ rows: [userRow({ session_epoch: 7 })] }); // findActiveUserByEmail
    const redis = createRedisMock();

    const service = createAuthService({
      db: db as never,
      redis: redis as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
    });

    await service.login({ email: "user@example.com", password: CURRENT_PASSWORD });

    const storedValue = redis.setEx.mock.calls[0]?.[2] as string;
    expect(storedValue).toBe(`${USER_ID}:7`);
  });
});
