import { describe, expect, it, vi } from "vitest";

import {
  consumeUserAuthToken,
  issueUserAuthToken,
} from "../../src/auth/authTokenStore.js";
import { generateAuthToken } from "../../src/auth/authPrimitives.js";

const userId = "11111111-1111-4111-8111-111111111111";
const now = new Date("2026-06-30T12:00:00.000Z");

function createDbMock(rows: unknown[] = []) {
  return {
    query: vi.fn().mockResolvedValue({ rows }),
  };
}

describe("userAuthTokenStore", () => {
  it("issues hashed auth tokens without exposing raw token material", async () => {
    const db = createDbMock([
      {
        id: "22222222-2222-4222-8222-222222222222",
        user_id: userId,
        token_hash: "b".repeat(64),
        purpose: "email_verify",
        expires_at: now,
        consumed_at: null,
        created_at: now,
      },
    ]);

    const result = await issueUserAuthToken(db, {
      userId,
      tokenHash: "b".repeat(64),
      purpose: "email_verify",
      expiresAt: new Date("2026-07-01T00:00:00.000Z"),
    });

    expect(result).toMatchObject({
      userId,
      tokenHash: "b".repeat(64),
      purpose: "email_verify",
    });
    expect(db.query).toHaveBeenCalledWith(
      expect.stringContaining("INSERT INTO public.user_auth_tokens"),
      [userId, "b".repeat(64), "email_verify", new Date("2026-07-01T00:00:00.000Z")]
    );
  });

  it("voids outstanding same-purpose tokens before issuing a new one", async () => {
    const db = createDbMock([
      {
        id: "22222222-2222-4222-8222-222222222222",
        user_id: userId,
        token_hash: "c".repeat(64),
        purpose: "email_verify",
        expires_at: now,
        consumed_at: null,
        created_at: now,
      },
    ]);

    await issueUserAuthToken(db, {
      userId,
      tokenHash: "c".repeat(64),
      purpose: "email_verify",
      expiresAt: new Date("2026-07-01T00:00:00.000Z"),
    });

    expect(db.query).toHaveBeenCalledTimes(2);
    const voidSql = String(db.query.mock.calls[0]?.[0]);
    expect(voidSql).toContain("UPDATE public.user_auth_tokens");
    expect(voidSql).toContain("consumed_at IS NULL");
    expect(db.query.mock.calls[0]?.[1]).toEqual([userId, "email_verify"]);
    expect(String(db.query.mock.calls[1]?.[0])).toContain("INSERT INTO public.user_auth_tokens");
  });

  it("consumes an unexpired auth token exactly once", async () => {
    const token = generateAuthToken();
    const db = createDbMock([
      {
        id: "33333333-3333-4333-8333-333333333333",
        user_id: userId,
        token_hash: token.tokenHash,
        purpose: "password_reset",
        expires_at: new Date("2026-07-01T00:00:00.000Z"),
        consumed_at: now,
        created_at: new Date("2026-06-30T00:00:00.000Z"),
      },
    ]);

    const consumed = await consumeUserAuthToken(db, {
      token: token.rawToken,
      purpose: "password_reset",
      now,
    });

    expect(consumed).toMatchObject({
      userId,
      tokenHash: token.tokenHash,
      purpose: "password_reset",
    });
    expect(db.query).toHaveBeenCalledWith(
      expect.stringContaining("UPDATE public.user_auth_tokens"),
      [token.tokenHash, "password_reset", now]
    );
  });

  it("returns null when a token is missing or expired", async () => {
    const token = generateAuthToken();
    const db = createDbMock([]);

    await expect(
      consumeUserAuthToken(db, {
        token: token.rawToken,
        purpose: "email_verify",
        now,
      })
    ).resolves.toBeNull();
  });

  it("returns null when a token has already expired", async () => {
    const token = generateAuthToken();
    const db = createDbMock([]);

    await expect(
      consumeUserAuthToken(db, {
        token: token.rawToken,
        purpose: "password_reset",
        now: new Date("2026-07-01T12:00:00.000Z"),
      })
    ).resolves.toBeNull();
    expect(db.query).toHaveBeenCalledWith(
      expect.stringContaining("UPDATE public.user_auth_tokens"),
      [token.tokenHash, "password_reset", new Date("2026-07-01T12:00:00.000Z")]
    );
  });

  it("rejects invalid user ids and hashes early", async () => {
    const db = createDbMock([]);

    await expect(
      issueUserAuthToken(db, {
        userId: "not-a-uuid",
        tokenHash: "a".repeat(64),
        purpose: "email_verify",
        expiresAt: new Date("2026-07-01T00:00:00.000Z"),
      })
    ).rejects.toThrow("User ID must be a valid UUID");
    await expect(
      issueUserAuthToken(db, {
        userId,
        tokenHash: "not-a-hash",
        purpose: "email_verify",
        expiresAt: new Date("2026-07-01T00:00:00.000Z"),
      })
    ).rejects.toThrow("Token hash must be a SHA-256 hex digest");
  });
});
