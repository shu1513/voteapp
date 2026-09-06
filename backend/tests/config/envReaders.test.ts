import { afterEach, describe, expect, it } from "vitest";

import { readBooleanEnv, readPositiveIntegerEnv } from "../../src/config/envReaders.js";

const NAME = "ENV_READERS_TEST_VALUE";

afterEach(() => {
  delete process.env[NAME];
});

describe("readBooleanEnv", () => {
  it("falls back when unset, empty or whitespace", () => {
    expect(readBooleanEnv(NAME, true)).toBe(true);
    process.env[NAME] = "";
    expect(readBooleanEnv(NAME, false)).toBe(false);
    process.env[NAME] = "   ";
    expect(readBooleanEnv(NAME, true)).toBe(true);
  });

  it("accepts the documented spellings, case-insensitively and trimmed", () => {
    for (const value of ["1", "true", "YES", " y ", "On"]) {
      process.env[NAME] = value;
      expect(readBooleanEnv(NAME, false)).toBe(true);
    }
    for (const value of ["0", "false", "NO", " n ", "Off"]) {
      process.env[NAME] = value;
      expect(readBooleanEnv(NAME, true)).toBe(false);
    }
  });

  it("rejects anything else with the established message", () => {
    process.env[NAME] = "maybe";
    expect(() => readBooleanEnv(NAME, true)).toThrow(`Invalid boolean env ${NAME}: maybe`);
  });
});

describe("readPositiveIntegerEnv", () => {
  it("falls back when unset, empty or whitespace", () => {
    expect(readPositiveIntegerEnv(NAME, 7)).toBe(7);
    process.env[NAME] = " ";
    expect(readPositiveIntegerEnv(NAME, 7)).toBe(7);
  });

  it("reads a whole decimal positive integer, trimmed", () => {
    process.env[NAME] = " 42 ";
    expect(readPositiveIntegerEnv(NAME, 7)).toBe(42);
    process.env[NAME] = String(Number.MAX_SAFE_INTEGER);
    expect(readPositiveIntegerEnv(NAME, 7)).toBe(Number.MAX_SAFE_INTEGER);
  });

  it("rejects zero, negatives, fractions, units, exponents, leading zeros and unsafe magnitudes", () => {
    for (const value of ["0", "-1", "1.5", "10ms", "1e3", "007", "abc", "9007199254740993"]) {
      process.env[NAME] = value;
      expect(() => readPositiveIntegerEnv(NAME, 7)).toThrow(`Invalid positive integer env ${NAME}: ${value}`);
    }
  });
});
