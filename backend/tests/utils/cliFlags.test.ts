import { describe, expect, it } from "vitest";

import { readPositiveIntegerFlag, readStrictFlagValue, readStrictPositiveIntegerFlag } from "../../src/utils/cliFlags.js";

describe("readStrictFlagValue", () => {
  it("reads the space and inline forms, trimmed", () => {
    expect(readStrictFlagValue(["--cache-dir", " ./cache "], "--cache-dir")).toBe("./cache");
    expect(readStrictFlagValue(["--cache-dir= ./cache "], "--cache-dir")).toBe("./cache");
  });

  it("returns null when the flag is absent", () => {
    expect(readStrictFlagValue(["--dry-run"], "--cache-dir")).toBeNull();
    expect(readStrictFlagValue([], "--cache-dir")).toBeNull();
  });

  it("does not match a flag that merely shares a prefix", () => {
    expect(readStrictFlagValue(["--cache-dir-extra", "x"], "--cache-dir")).toBeNull();
  });

  it("rejects a flag given without a value", () => {
    expect(() => readStrictFlagValue(["--cache-dir"], "--cache-dir")).toThrow("Missing --cache-dir value");
    expect(() => readStrictFlagValue(["--cache-dir", "--dry-run"], "--cache-dir")).toThrow(
      "Missing --cache-dir value"
    );
    expect(() => readStrictFlagValue(["--cache-dir", "  "], "--cache-dir")).toThrow("Missing --cache-dir value");
    expect(() => readStrictFlagValue(["--cache-dir="], "--cache-dir")).toThrow("Missing --cache-dir value");
    expect(() => readStrictFlagValue(["--cache-dir=  "], "--cache-dir")).toThrow("Missing --cache-dir value");
  });

  it("rejects a flag given more than once, in any mix of forms", () => {
    expect(() => readStrictFlagValue(["--cache-dir", "a", "--cache-dir", "b"], "--cache-dir")).toThrow(
      "Provide --cache-dir at most once"
    );
    expect(() => readStrictFlagValue(["--cache-dir=a", "--cache-dir", "b"], "--cache-dir")).toThrow(
      "Provide --cache-dir at most once"
    );
    expect(() => readStrictFlagValue(["--cache-dir=a", "--cache-dir=a"], "--cache-dir")).toThrow(
      "Provide --cache-dir at most once"
    );
  });

  it("consumes the space-form value so it is not re-read as a flag", () => {
    // The value "--cache-dir" is consumed by --label; only the later real flag counts.
    expect(readStrictFlagValue(["--label", "x", "--cache-dir", "y"], "--cache-dir")).toBe("y");
  });
});

describe("readStrictPositiveIntegerFlag", () => {
  it("parses a positive integer from either form", () => {
    expect(readStrictPositiveIntegerFlag(["--max-candidates", "25"], "--max-candidates")).toBe(25);
    expect(readStrictPositiveIntegerFlag(["--max-candidates=25"], "--max-candidates")).toBe(25);
    expect(readStrictPositiveIntegerFlag(["--max-candidates", "9007199254740991"], "--max-candidates")).toBe(
      Number.MAX_SAFE_INTEGER
    );
  });

  it("returns undefined when the flag is absent", () => {
    expect(readStrictPositiveIntegerFlag(["--dry-run"], "--max-candidates")).toBeUndefined();
  });

  it("rejects zero, negatives, leading zeros, decimals, and non-digits", () => {
    for (const raw of ["0", "-1", "05", "1.5", "1e3", "abc", "12abc", "+3"]) {
      expect(() => readStrictPositiveIntegerFlag(["--max-candidates", raw], "--max-candidates")).toThrow(
        `Invalid --max-candidates value: ${raw}`
      );
    }
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    // Number("9007199254740993") is 9007199254740992: it still passes
    // Number.isInteger, so only the safe-integer check catches it.
    expect(() => readStrictPositiveIntegerFlag(["--max-candidates", "9007199254740993"], "--max-candidates")).toThrow(
      "Invalid --max-candidates value: 9007199254740993"
    );
    expect(() => readStrictPositiveIntegerFlag(["--max-candidates=99999999999999999999"], "--max-candidates")).toThrow(
      "Invalid --max-candidates value: 99999999999999999999"
    );
  });

  it("propagates the strict reader's missing and duplicate errors", () => {
    expect(() => readStrictPositiveIntegerFlag(["--max-candidates"], "--max-candidates")).toThrow(
      "Missing --max-candidates value"
    );
    expect(() => readStrictPositiveIntegerFlag(["--max-candidates=1", "--max-candidates=2"], "--max-candidates")).toThrow(
      "Provide --max-candidates at most once"
    );
  });
});

describe("readPositiveIntegerFlag", () => {
  it("parses a positive integer from either form and falls back when absent", () => {
    expect(readPositiveIntegerFlag(["--limit", "42"], "--limit", 7)).toBe(42);
    expect(readPositiveIntegerFlag(["--limit=42"], "--limit", 7)).toBe(42);
    expect(readPositiveIntegerFlag(["--other=1"], "--limit", 7)).toBe(7);
    expect(readPositiveIntegerFlag(["--limit=9007199254740991"], "--limit", 7)).toBe(Number.MAX_SAFE_INTEGER);
  });

  it("rejects a flag given without a value", () => {
    expect(() => readPositiveIntegerFlag(["--limit"], "--limit", 7)).toThrow("--limit requires a value");
  });

  it("rejects zero, negatives, leading zeros, decimals, and non-digits", () => {
    for (const raw of ["0", "-1", "05", "1.5", "1e3", "abc", "12abc"]) {
      expect(() => readPositiveIntegerFlag([`--limit=${raw}`], "--limit", 7)).toThrow(
        `--limit must be a positive integer, got: ${raw}`
      );
    }
  });

  it("rejects digit-only values above Number.MAX_SAFE_INTEGER instead of rounding them", () => {
    expect(() => readPositiveIntegerFlag(["--limit=9007199254740993"], "--limit", 7)).toThrow(
      "--limit must be a positive integer, got: 9007199254740993"
    );
    expect(() => readPositiveIntegerFlag(["--limit", "99999999999999999999"], "--limit", 7)).toThrow(
      "--limit must be a positive integer, got: 99999999999999999999"
    );
  });
});
