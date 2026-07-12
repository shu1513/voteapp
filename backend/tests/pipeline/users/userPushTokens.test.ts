import { describe, expect, it, vi } from "vitest";

import {
  listActiveUserPushTokens,
  registerUserPushToken,
  revokeUserPushToken,
  revokeUserPushTokenByToken,
  UserPushTokensError,
} from "../../../src/pipeline/users/userPushTokens.js";

const USER_ID = "11111111-1111-4111-8111-111111111111";
const TOKEN = "ExponentPushToken[abc123]";

describe("userPushTokens", () => {
  it("registerUserPushToken upserts on the token and clears revocation", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 1 });

    await registerUserPushToken({ query } as never, USER_ID, {
      expoPushToken: TOKEN,
      nativeToken: "apns-native-token",
      platform: "ios",
    });

    const sql = String(query.mock.calls[0][0]);
    expect(sql).toContain("ON CONFLICT (expo_push_token) DO UPDATE");
    expect(sql).toContain("revoked_at = NULL");
    expect(sql).toContain("user_id = EXCLUDED.user_id");
    expect(query.mock.calls[0][1]).toEqual([USER_ID, TOKEN, "apns-native-token", "ios"]);
  });

  it("register rejects a non-UUID userId without querying", async () => {
    const query = vi.fn();

    await expect(
      registerUserPushToken({ query } as never, "bob", {
        expoPushToken: TOKEN,
        nativeToken: null,
        platform: "android",
      })
    ).rejects.toBeInstanceOf(UserPushTokensError);
    expect(query).not.toHaveBeenCalled();
  });

  it("register maps a user foreign-key violation to user_not_found", async () => {
    const fkError = Object.assign(new Error("violates foreign key"), { code: "23503" });
    const query = vi.fn().mockRejectedValue(fkError);

    await expect(
      registerUserPushToken({ query } as never, USER_ID, {
        expoPushToken: TOKEN,
        nativeToken: null,
        platform: "ios",
      })
    ).rejects.toMatchObject({ code: "user_not_found" });
  });

  it("register rethrows non-FK database errors untouched", async () => {
    const dbError = new Error("connection reset");
    const query = vi.fn().mockRejectedValue(dbError);

    await expect(
      registerUserPushToken({ query } as never, USER_ID, {
        expoPushToken: TOKEN,
        nativeToken: null,
        platform: "ios",
      })
    ).rejects.toBe(dbError);
  });

  it("revokeUserPushToken scopes the revocation to the user", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 1 });

    await revokeUserPushToken({ query } as never, USER_ID, TOKEN);

    const sql = String(query.mock.calls[0][0]);
    expect(sql).toContain("SET revoked_at = now()");
    expect(sql).toContain("user_id = $1::uuid");
    expect(sql).toContain("revoked_at IS NULL");
    expect(query.mock.calls[0][1]).toEqual([USER_ID, TOKEN]);
  });

  it("revokeUserPushTokenByToken revokes regardless of owner", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 1 });

    await revokeUserPushTokenByToken({ query } as never, TOKEN);

    const sql = String(query.mock.calls[0][0]);
    expect(sql).not.toContain("user_id");
    expect(query.mock.calls[0][1]).toEqual([TOKEN]);
  });

  it("listActiveUserPushTokens returns only unrevoked tokens for the user", async () => {
    const query = vi
      .fn()
      .mockResolvedValue({ rows: [{ expo_push_token: TOKEN }, { expo_push_token: "ExponentPushToken[def]" }], rowCount: 2 });

    await expect(listActiveUserPushTokens({ query } as never, USER_ID)).resolves.toEqual([
      TOKEN,
      "ExponentPushToken[def]",
    ]);
    const sql = String(query.mock.calls[0][0]);
    expect(sql).toContain("revoked_at IS NULL");
    expect(query.mock.calls[0][1]).toEqual([USER_ID]);
  });
});
