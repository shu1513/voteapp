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
});

