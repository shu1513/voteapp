import { describe, expect, it } from "vitest";

import {
  ocrTextContainsAmountCents,
  parseKansasPhaseZeroArgs,
} from "../../src/scripts/probeKansasCandidateFinance.js";

describe("parseKansasPhaseZeroArgs", () => {
  it("applies defaults", () => {
    const args = parseKansasPhaseZeroArgs([]);
    expect(args.candidates).toEqual(["Holscher", "Schmidt"]);
    expect(args.skipCapTest).toBe(false);
  });

  it("parses overrides", () => {
    const args = parseKansasPhaseZeroArgs([
      "--candidates",
      "Holscher",
      "--skip-cap",
      "--skip-concurrency",
      "--spacing-ms",
      "500",
    ]);
    expect(args.candidates).toEqual(["Holscher"]);
    expect(args.skipCapTest).toBe(true);
    expect(args.skipConcurrency).toBe(true);
    expect(args.spacingMs).toBe(500);
  });

  it("rejects unknown flags", () => {
    expect(() => parseKansasPhaseZeroArgs(["--nope"])).toThrow("Unknown argument");
  });

  it("rejects non-numeric, negative, and numeric-prefixed values", () => {
    expect(() => parseKansasPhaseZeroArgs(["--timeout-ms", "abc"])).toThrow(
      "Invalid value for --timeout-ms: abc"
    );
    expect(() => parseKansasPhaseZeroArgs(["--spacing-ms", "-5"])).toThrow(
      "Invalid value for --spacing-ms: -5"
    );
    // parseInt would silently truncate these to 500 / 1 / 10.
    expect(() => parseKansasPhaseZeroArgs(["--timeout-ms", "500ms"])).toThrow(
      "Invalid value for --timeout-ms: 500ms"
    );
    expect(() => parseKansasPhaseZeroArgs(["--spacing-ms", "1.5"])).toThrow(
      "Invalid value for --spacing-ms: 1.5"
    );
    expect(() => parseKansasPhaseZeroArgs(["--spacing-ms", "10junk"])).toThrow(
      "Invalid value for --spacing-ms: 10junk"
    );
  });
});

describe("ocrTextContainsAmountCents", () => {
  it("matches clean and OCR-mangled renderings of an amount", () => {
    expect(ocrTextContainsAmountCents("total $ 359,633.00 media", 35963300)).toBe(true);
    expect(ocrTextContainsAmountCents("$ 138,270 ,00", 13827000)).toBe(true);
    expect(ocrTextContainsAmountCents("$1,544.08", 154408)).toBe(true);
  });

  it("does not match a different amount", () => {
    expect(ocrTextContainsAmountCents("$ 359,633.00", 35963301)).toBe(false);
  });

  it("does not match inside a longer number", () => {
    expect(ocrTextContainsAmountCents("$21,544.08", 154408)).toBe(false);
    expect(ocrTextContainsAmountCents("$1,544.089", 154408)).toBe(false);
    expect(ocrTextContainsAmountCents("2,544.08", 54408)).toBe(false);
  });
});
