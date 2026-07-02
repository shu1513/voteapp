import { Buffer } from "node:buffer";

import { describe, expect, it } from "vitest";

import {
  AUTH_PASSWORD_MAX_LENGTH,
  AUTH_PASSWORD_MIN_LENGTH,
  AUTH_SESSION_ID_BYTE_LENGTH,
  AUTH_TOKEN_BYTE_LENGTH,
  generateAuthToken,
  generateSessionId,
  hashAuthToken,
  hashPassword,
  hashSessionId,
  validatePasswordPolicy,
  verifyPassword,
} from "../../src/auth/authPrimitives.js";

function bufferLengthFromBase64Url(value: string): number {
  return Buffer.from(value, "base64url").length;
}

describe("authPrimitives", () => {
  it("validates password policy boundaries", () => {
    expect(() => validatePasswordPolicy("a".repeat(AUTH_PASSWORD_MIN_LENGTH - 1))).toThrow(
      `Password must be at least ${AUTH_PASSWORD_MIN_LENGTH} characters long`
    );
    expect(() => validatePasswordPolicy(" ".repeat(AUTH_PASSWORD_MIN_LENGTH))).toThrow(
      "Password must include at least one non-whitespace character"
    );
    expect(() => validatePasswordPolicy("a".repeat(AUTH_PASSWORD_MAX_LENGTH + 1))).toThrow(
      `Password must be at most ${AUTH_PASSWORD_MAX_LENGTH} characters long`
    );
    expect(() => validatePasswordPolicy("a".repeat(AUTH_PASSWORD_MIN_LENGTH))).not.toThrow();
  });

  it("hashes and verifies passwords with argon2id", async () => {
    const password = "correct horse battery staple";
    const passwordHash = await hashPassword(password);

    expect(passwordHash).toContain("$argon2id$");
    await expect(verifyPassword(passwordHash, password)).resolves.toBe(true);
    await expect(verifyPassword(passwordHash, "correct horse battery")).resolves.toBe(false);
    await expect(verifyPassword(passwordHash, "short")).resolves.toBe(false);
  });

  it("generates token hashes that match the raw token and use full entropy", () => {
    const token = generateAuthToken();

    expect(bufferLengthFromBase64Url(token.rawToken)).toBe(AUTH_TOKEN_BYTE_LENGTH);
    expect(token.tokenHash).toBe(hashAuthToken(token.rawToken));
    expect(token.tokenHash).toHaveLength(64);
  });

  it("generates opaque session ids", () => {
    const sessionId = generateSessionId();

    expect(bufferLengthFromBase64Url(sessionId)).toBe(AUTH_SESSION_ID_BYTE_LENGTH);
    expect(hashSessionId(sessionId)).toHaveLength(64);
  });
});
