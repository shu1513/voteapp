import { describe, expect, it } from "vitest";

import {
  createEmailUnsubscribeToken,
  verifyEmailUnsubscribeToken,
} from "../../../src/pipeline/users/emailUnsubscribeToken.js";

const USER_ID = "11111111-1111-4111-8111-111111111111";
const OTHER_USER_ID = "22222222-2222-4222-8222-222222222222";
const SECRET = "test-secret-with-at-least-32-characters!";

describe("emailUnsubscribeToken", () => {
  it("round-trips: a created token verifies back to its userId", () => {
    const token = createEmailUnsubscribeToken(USER_ID, SECRET);
    expect(token.startsWith(`v1.${USER_ID}.`)).toBe(true);
    expect(verifyEmailUnsubscribeToken(token, SECRET)).toBe(USER_ID);
  });

  it("rejects a token signed with a different secret", () => {
    const token = createEmailUnsubscribeToken(USER_ID, SECRET);
    expect(verifyEmailUnsubscribeToken(token, "another-secret-with-at-least-32-chars!!")).toBeNull();
  });

  it("rejects a token whose userId was swapped after signing", () => {
    const token = createEmailUnsubscribeToken(USER_ID, SECRET);
    const tampered = token.replace(USER_ID, OTHER_USER_ID);
    expect(verifyEmailUnsubscribeToken(tampered, SECRET)).toBeNull();
  });

  it("rejects malformed tokens", () => {
    expect(verifyEmailUnsubscribeToken("", SECRET)).toBeNull();
    expect(verifyEmailUnsubscribeToken("v1.not-a-uuid.sig", SECRET)).toBeNull();
    expect(verifyEmailUnsubscribeToken(`v2.${USER_ID}.sig`, SECRET)).toBeNull();
    expect(verifyEmailUnsubscribeToken(`v1.${USER_ID}`, SECRET)).toBeNull();
    expect(verifyEmailUnsubscribeToken(`v1.${USER_ID}.@@@`, SECRET)).toBeNull();
  });

  it("refuses secrets shorter than 32 characters", () => {
    expect(() => createEmailUnsubscribeToken(USER_ID, "short")).toThrow("at least 32 characters");
    expect(() => verifyEmailUnsubscribeToken("v1.x.y", "short")).toThrow("at least 32 characters");
  });

  it("refuses to sign a non-UUID userId", () => {
    expect(() => createEmailUnsubscribeToken("bob", SECRET)).toThrow("userId must be a UUID");
  });
});
