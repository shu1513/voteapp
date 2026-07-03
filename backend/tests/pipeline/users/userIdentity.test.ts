import { describe, expect, it, vi } from "vitest";

import { getUserIdentity, UserIdentityError } from "../../../src/pipeline/users/userIdentity.js";

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
});
