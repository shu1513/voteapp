import { describe, expect, it } from "vitest";

import { createTrustedClientIpResolver, parseTrustedClientIpHeader } from "../../src/api/addressApiClientIp.js";

describe("parseTrustedClientIpHeader", () => {
  it("uses the first non-empty value from X-Forwarded-For style headers", () => {
    expect(parseTrustedClientIpHeader(" 203.0.113.10, 198.51.100.20 ")).toBe("203.0.113.10");
    expect(parseTrustedClientIpHeader(" , 198.51.100.20 ")).toBe("198.51.100.20");
  });

  it("returns null for missing or empty headers", () => {
    expect(parseTrustedClientIpHeader(undefined)).toBeNull();
    expect(parseTrustedClientIpHeader(" , ")).toBeNull();
  });
});

describe("createTrustedClientIpResolver", () => {
  it("uses socket remoteAddress when no trusted header is configured", () => {
    const resolveClientIp = createTrustedClientIpResolver(null);

    expect(
      resolveClientIp({
        headers: { "x-forwarded-for": "203.0.113.10" },
        remoteAddress: "127.0.0.1",
      })
    ).toBe("127.0.0.1");
  });

  it("uses a configured trusted header when present", () => {
    const resolveClientIp = createTrustedClientIpResolver("X-Forwarded-For");

    expect(
      resolveClientIp({
        headers: { "x-forwarded-for": "203.0.113.10, 198.51.100.20" },
        remoteAddress: "127.0.0.1",
      })
    ).toBe("203.0.113.10");
  });

  it("falls back to socket remoteAddress when configured trusted header is missing", () => {
    const resolveClientIp = createTrustedClientIpResolver("CF-Connecting-IP");

    expect(
      resolveClientIp({
        headers: { "x-forwarded-for": "203.0.113.10" },
        remoteAddress: "127.0.0.1",
      })
    ).toBe("127.0.0.1");
  });

  describe("edge shared secret", () => {
    const SECRET = "test-edge-shared-secret-0123456789abcdef";

    it("trusts the client-IP header when the request carries the matching secret", () => {
      const resolveClientIp = createTrustedClientIpResolver("CF-Connecting-IP", SECRET);

      expect(
        resolveClientIp({
          headers: { "cf-connecting-ip": "203.0.113.10", "x-edge-secret": SECRET },
          remoteAddress: "127.0.0.1",
        })
      ).toBe("203.0.113.10");
    });

    it("ignores a spoofed client-IP header when the secret is missing or wrong", () => {
      const resolveClientIp = createTrustedClientIpResolver("CF-Connecting-IP", SECRET);

      // Direct *.onrender.com hit: attacker controls every header except the
      // secret's value, so both variants must collapse to the socket IP.
      expect(
        resolveClientIp({
          headers: { "cf-connecting-ip": "203.0.113.10" },
          remoteAddress: "10.0.0.5",
        })
      ).toBe("10.0.0.5");
      expect(
        resolveClientIp({
          headers: { "cf-connecting-ip": "203.0.113.10", "x-edge-secret": "wrong-guess" },
          remoteAddress: "10.0.0.5",
        })
      ).toBe("10.0.0.5");
    });

    it("keeps legacy blind-trust behavior when no secret is configured", () => {
      const resolveClientIp = createTrustedClientIpResolver("CF-Connecting-IP", null);

      expect(
        resolveClientIp({
          headers: { "cf-connecting-ip": "203.0.113.10" },
          remoteAddress: "127.0.0.1",
        })
      ).toBe("203.0.113.10");
    });
  });
});
