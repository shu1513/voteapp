import { describe, expect, it, vi } from "vitest";

import { createAuthService } from "../../src/auth/authService.js";

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

    const mailer = {
      sendVerificationEmail: vi.fn().mockResolvedValue(undefined),
      sendPasswordResetEmail: vi.fn().mockResolvedValue(undefined),
    };

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

    const mailer = {
      sendVerificationEmail: vi.fn().mockResolvedValue(undefined),
      sendPasswordResetEmail: vi.fn().mockResolvedValue(undefined),
    };

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
