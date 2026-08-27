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
});
