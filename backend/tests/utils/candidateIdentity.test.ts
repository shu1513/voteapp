import { describe, expect, it } from "vitest";

import {
  hasNormalizedIntersection,
  normalizeCandidateName,
  normalizeTwitterHandle,
  splitDisplayNameToFirstLast,
} from "../../src/utils/candidateIdentity.js";

describe("candidateIdentity", () => {
  it("normalizes candidate names", () => {
    expect(normalizeCandidateName(" Jane  Q. Doe ")).toBe("jane q doe");
  });

  it("normalizes twitter handle", () => {
    expect(normalizeTwitterHandle("@JaneDoe")).toBe("janedoe");
  });

  it("splits display name", () => {
    expect(splitDisplayNameToFirstLast("Jane Q Doe")).toEqual({
      firstName: "Jane",
      lastName: "Doe",
    });
  });

  it("finds normalized intersection", () => {
    expect(hasNormalizedIntersection(["a", "b"], ["x", "b"])).toBe(true);
    expect(hasNormalizedIntersection(["a"], ["x"])).toBe(false);
  });
});
