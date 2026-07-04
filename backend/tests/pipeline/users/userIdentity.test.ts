import { describe, expect, it, vi } from "vitest";

import {
  getUserIdentity,
  MAX_FIRST_NAME_LENGTH,
  setUserFirstName,
  UserIdentityError,
} from "../../../src/pipeline/users/userIdentity.js";

const USER_ID = "11111111-1111-4111-8111-111111111111";
const IDENTITY = { email: "voter@example.com", first_name: "Val", email_verified: true };

describe("userIdentity", () => {
  it("getUserIdentity returns email, first_name, and email_verified", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [IDENTITY], rowCount: 1 });

    await expect(getUserIdentity({ query } as never, USER_ID)).resolves.toEqual(IDENTITY);
    expect(String(query.mock.calls[0][0])).toContain("deleted_at IS NULL");
    expect(query.mock.calls[0][1]).toEqual([USER_ID]);
  });

  it("throws user_not_found for unknown or deleted users", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 0 });

    await expect(getUserIdentity({ query } as never, USER_ID)).rejects.toMatchObject({
      code: "user_not_found",
    });
  });

  it("rejects a non-UUID userId without querying", async () => {
    const query = vi.fn();

    await expect(getUserIdentity({ query } as never, "bob")).rejects.toBeInstanceOf(UserIdentityError);
    expect(query).not.toHaveBeenCalled();
  });

  it("setUserFirstName trims, updates, and returns the identity", async () => {
    const updated = { ...IDENTITY, first_name: "Valerie" };
    const query = vi.fn().mockResolvedValue({ rows: [updated], rowCount: 1 });

    await expect(setUserFirstName({ query } as never, USER_ID, "  Valerie  ")).resolves.toEqual(updated);
    expect(query.mock.calls[0][1]).toEqual([USER_ID, "Valerie"]);
  });

  it("setUserFirstName rejects empty and over-long names without querying", async () => {
    const query = vi.fn();

    await expect(setUserFirstName({ query } as never, USER_ID, "   ")).rejects.toThrow(
      "first_name must be a non-empty string"
    );
    await expect(
      setUserFirstName({ query } as never, USER_ID, "x".repeat(MAX_FIRST_NAME_LENGTH + 1))
    ).rejects.toThrow(`at most ${MAX_FIRST_NAME_LENGTH} characters`);
    expect(query).not.toHaveBeenCalled();
  });

  it("setUserFirstName throws user_not_found for unknown or deleted users", async () => {
    const query = vi.fn().mockResolvedValue({ rows: [], rowCount: 0 });

    await expect(setUserFirstName({ query } as never, USER_ID, "Val")).rejects.toMatchObject({
      code: "user_not_found",
    });
  });
});
