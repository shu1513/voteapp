import { describe, expect, it } from "vitest";
import { normalizeHttpUrl } from "../../src/utils/normalizeHttpUrl.ts";

describe("normalizeHttpUrl", () => {
  it("normalizes trailing slash and hash", () => {
    expect(normalizeHttpUrl("https://example.org/path/#section")).toBe("https://example.org/path");
  });

  it("resolves relative URLs with base URL", () => {
    expect(normalizeHttpUrl("/register", { baseUrl: "https://example.org/polling" })).toBe(
      "https://example.org/register"
    );
  });

  it("returns null for non-http URLs", () => {
    expect(normalizeHttpUrl("ftp://example.org/file")).toBeNull();
    expect(normalizeHttpUrl("notaurl")).toBeNull();
  });
});

