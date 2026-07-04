import { beforeAll, describe, expect, it, vi } from "vitest";

import { createAuthService } from "../../src/auth/authService.js";
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
      acceptedTermsVersion: "1.0",
    });

    const insertCall = client.query.mock.calls.find((call) => String(call[0]).includes("INSERT INTO public.users"));
    expect(String(insertCall?.[0])).toContain("accepted_terms_version");
    expect(String(insertCall?.[0])).toContain("accepted_terms_at");
    expect(insertCall?.[1]?.[3]).toBe("1.0");
  });

  it("stamps acceptance on the unverified-refresh update path too", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [userRow({ email_verified: false })] }) // existing unverified user
      .mockResolvedValueOnce({ rows: [userRow({ email_verified: false })] }) // UPDATE refresh
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
      acceptedTermsVersion: "1.0",
    });

    const updateCall = client.query.mock.calls.find((call) => String(call[0]).includes("UPDATE public.users"));
    expect(String(updateCall?.[0])).toContain("accepted_terms_version = $4");
    expect(String(updateCall?.[0])).toContain("accepted_terms_at = now()");
    expect(updateCall?.[1]?.[3]).toBe("1.0");
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
    ).rejects.toThrow("current terms version");
    // Defense-in-depth: even a direct caller bypassing the API layer cannot
    // persist acceptance of a superseded version.
    await expect(
      service.register({
        email: "new@example.com",
        password: "correct horse battery staple",
        acceptedTermsVersion: "0.9",
      })
    ).rejects.toThrow("current terms version");
    expect(db.connect).not.toHaveBeenCalled();
  });
});

describe("createAuthService changePassword", () => {
  it("updates the hash, rotates every session, and returns a fresh one", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [userRow()] }) // user FOR UPDATE
      .mockResolvedValueOnce({ rows: [], rowCount: 1 }) // UPDATE password_hash
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
  it("soft-deletes, voids outstanding tokens, and destroys all sessions", async () => {
    const client = createDbClientMock();
    client.query
      .mockResolvedValueOnce({ rows: [] }) // BEGIN
      .mockResolvedValueOnce({ rows: [userRow()] }) // user FOR UPDATE
      .mockResolvedValueOnce({ rows: [], rowCount: 1 }) // soft delete
      .mockResolvedValueOnce({ rows: [] }) // void tokens
      .mockResolvedValueOnce({ rows: [] }); // COMMIT
    const redis = createRedisMock();

    const service = createAuthService({
      db: createDbMock(client) as never,
      redis: redis as never,
      mailer: createMailerMock(),
      publicBaseUrl: "https://example.com",
    });

    await service.deleteAccount({ userId: USER_ID, password: CURRENT_PASSWORD });

    const deleteCall = client.query.mock.calls.find((call) => String(call[0]).includes("SET deleted_at = now()"));
    expect(deleteCall?.[1]).toEqual([USER_ID]);
    const voidCall = client.query.mock.calls.find((call) => String(call[0]).includes("UPDATE public.user_auth_tokens"));
    expect(voidCall?.[1]).toEqual([USER_ID]);
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
    expect(client.query.mock.calls.some((call) => String(call[0]).includes("SET deleted_at"))).toBe(false);
    expect(redis.sMembers).not.toHaveBeenCalled();
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
});
