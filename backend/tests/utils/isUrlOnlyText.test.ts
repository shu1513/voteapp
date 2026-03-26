import { describe, expect, it } from "vitest";
import { isUrlOnlyText } from "../../src/utils/isUrlOnlyText.ts";

describe("isUrlOnlyText", () => {
  it("returns true for pure URL strings", () => {
    expect(isUrlOnlyText("https://example.org/path")).toBe(true);
    expect(isUrlOnlyText("www.example.org/path")).toBe(true);
  });

  it("returns false for plain-language text", () => {
    expect(isUrlOnlyText("Polls are open from 7am to 8pm on election day.")).toBe(false);
  });

  it("returns false for text that contains a URL but is not URL-only", () => {
    expect(isUrlOnlyText("See https://example.org for details and deadlines.")).toBe(false);
  });

  it("handles empty, whitespace, and wrapped URL cases", () => {
    expect(isUrlOnlyText("")).toBe(false);
    expect(isUrlOnlyText("   ")).toBe(false);
    expect(isUrlOnlyText("\n  https://example.org/path  \n")).toBe(true);
    expect(isUrlOnlyText("(https://example.org)")).toBe(false);
  });

  it("handles scheme casing and domain-like non-urls", () => {
    expect(isUrlOnlyText("HTTPS://EXAMPLE.ORG/Path?Q=1")).toBe(true);
    expect(isUrlOnlyText("example.org")).toBe(false);
    expect(isUrlOnlyText("www.example.org:8080/path")).toBe(true);
    expect(isUrlOnlyText("https://a.example.org https://b.example.org")).toBe(false);
  });
});
