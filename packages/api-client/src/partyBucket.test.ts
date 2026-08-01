import { describe, expect, it } from "vitest";

import { partyBucket } from "./partyBucket";

describe("partyBucket", () => {
  it("buckets canonical major-party labels", () => {
    expect(partyBucket("Democratic")).toBe("democratic");
    expect(partyBucket("Republican")).toBe("republican");
  });

  it("buckets state affiliates and registration labels with their party", () => {
    expect(partyBucket("Democratic-Farmer-Labor")).toBe("democratic");
    expect(partyBucket("Democratic-NPL")).toBe("democratic");
    expect(partyBucket("Registered Democrat")).toBe("democratic");
    expect(partyBucket("Registered Republican")).toBe("republican");
  });

  it("is case- and whitespace-insensitive", () => {
    expect(partyBucket("  DEMOCRATIC ")).toBe("democratic");
    expect(partyBucket("republican")).toBe("republican");
  });

  it("buckets everything unlisted as other", () => {
    expect(partyBucket("Independent")).toBe("other");
    expect(partyBucket("Nonpartisan")).toBe("other");
    expect(partyBucket("Libertarian")).toBe("other");
    expect(partyBucket("Green")).toBe("other");
    expect(partyBucket("Unknown")).toBe("other");
    expect(partyBucket("Unaffiliated")).toBe("other");
    // Exact-match guards: neither is a Democratic Party label, and
    // "Independent Party" is a registered party, not "Independent".
    expect(partyBucket("Moderate Democrat")).toBe("other");
    expect(partyBucket("Independent Party")).toBe("other");
  });

  it("buckets null, undefined, and blank as other", () => {
    expect(partyBucket(null)).toBe("other");
    expect(partyBucket(undefined)).toBe("other");
    expect(partyBucket("")).toBe("other");
  });
});
