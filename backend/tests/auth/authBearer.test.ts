import { describe, expect, it } from "vitest";

import { parseBearerAuthorizationValue } from "../../src/auth/authBearer.js";

describe("parseBearerAuthorizationValue", () => {
  it("extracts the session id from a Bearer header", () => {
    expect(parseBearerAuthorizationValue("Bearer session-abc")).toBe("session-abc");
  });

  it("matches the scheme case-insensitively", () => {
    expect(parseBearerAuthorizationValue("bearer session-abc")).toBe("session-abc");
    expect(parseBearerAuthorizationValue("BEARER session-abc")).toBe("session-abc");
  });

  it("tolerates surrounding whitespace and multiple scheme separators", () => {
    expect(parseBearerAuthorizationValue("  Bearer   session-abc  ")).toBe("session-abc");
  });

  it("uses the first value when the header arrives as an array", () => {
    expect(parseBearerAuthorizationValue(["Bearer session-abc", "Bearer other"])).toBe("session-abc");
  });

  it("returns null for missing or empty values", () => {
    expect(parseBearerAuthorizationValue(undefined)).toBeNull();
    expect(parseBearerAuthorizationValue("")).toBeNull();
    expect(parseBearerAuthorizationValue([])).toBeNull();
  });

  it("returns null for non-Bearer schemes", () => {
    expect(parseBearerAuthorizationValue("Basic dXNlcjpwYXNz")).toBeNull();
    expect(parseBearerAuthorizationValue("Bearersession-abc")).toBeNull();
  });

  it("returns null for a Bearer scheme without credentials", () => {
    expect(parseBearerAuthorizationValue("Bearer")).toBeNull();
    expect(parseBearerAuthorizationValue("Bearer   ")).toBeNull();
  });

  it("returns null when the credentials contain spaces", () => {
    expect(parseBearerAuthorizationValue("Bearer session abc")).toBeNull();
  });
});
