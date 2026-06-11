import { describe, expect, it } from "vitest";

import { createTrustedUserIdResolver, parseTrustedUserIdHeader } from "../../src/api/addressApiAuth.js";

describe("parseTrustedUserIdHeader", () => {
  it("trims a trusted user ID header value", () => {
    expect(parseTrustedUserIdHeader(" 11111111-1111-4111-8111-111111111111 ")).toBe(
      "11111111-1111-4111-8111-111111111111"
    );
  });

  it("returns null for missing or empty header values", () => {
    expect(parseTrustedUserIdHeader(undefined)).toBeNull();
    expect(parseTrustedUserIdHeader("   ")).toBeNull();
  });
});

describe("createTrustedUserIdResolver", () => {
  it("fails closed when no trusted user header is configured", () => {
    const resolveUserId = createTrustedUserIdResolver(null);

    expect(
      resolveUserId({
        headers: { "x-user-id": "11111111-1111-4111-8111-111111111111" },
      })
    ).toBeNull();
  });

  it("uses the configured trusted header when present", () => {
    const resolveUserId = createTrustedUserIdResolver("X-User-Id");

    expect(
      resolveUserId({
        headers: { "x-user-id": "11111111-1111-4111-8111-111111111111" },
      })
    ).toBe("11111111-1111-4111-8111-111111111111");
  });

  it("uses the first value when Node exposes an array header", () => {
    const resolveUserId = createTrustedUserIdResolver("X-User-Id");

    expect(
      resolveUserId({
        headers: { "x-user-id": ["11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"] },
      })
    ).toBe("11111111-1111-4111-8111-111111111111");
  });

  it("returns null when the configured header is absent", () => {
    const resolveUserId = createTrustedUserIdResolver("X-User-Id");

    expect(
      resolveUserId({
        headers: { "x-other-user": "11111111-1111-4111-8111-111111111111" },
      })
    ).toBeNull();
  });
});
