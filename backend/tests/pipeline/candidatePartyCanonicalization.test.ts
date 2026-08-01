import { describe, expect, it } from "vitest";

import { canonicalizeParty } from "../../src/pipeline/candidates/candidatePartyCanonicalization.js";

describe("canonicalizeParty", () => {
  it("maps every observed spelling variant to its canonical form", () => {
    const variants: [string, string][] = [
      ["DEM", "Democratic"],
      ["Democrat", "Democratic"],
      ["Democratic Party", "Democratic"],
      ["Registered Democrat", "Democratic"],
      ["DEMOCRATIC", "Democratic"],
      ["REP", "Republican"],
      ["Republican Party", "Republican"],
      ["Registered Republican", "Republican"],
      ["REPUBLICAN", "Republican"],
      ["LIB", "Libertarian"],
      ["Libertarian Party", "Libertarian"],
      ["Registered Libertarian", "Libertarian"],
      ["GRE", "Green"],
      ["IND", "Independent"],
      ["INDEPENDENT", "Independent"],
      ["NONPARTISAN", "Nonpartisan"],
      ["Constitution Party", "Constitution"],
      ["Independent American Party", "Independent American"],
      ["No party preference", "No Party Preference"],
      ["STATES NO PARTY PREFERENCE", "No Party Preference"],
    ];
    for (const [raw, canonical] of variants) {
      expect(canonicalizeParty(raw), raw).toBe(canonical);
    }
  });

  it("trims and collapses whitespace before matching", () => {
    expect(canonicalizeParty("  Democratic   Party ")).toBe("Democratic");
    expect(canonicalizeParty(" Alaskan   Independence ")).toBe("Alaskan Independence");
  });

  it("never collapses real parties that merely end in 'Party'", () => {
    // These are the traps a generic "strip ' Party'" rule would fall into.
    expect(canonicalizeParty("Tea Party")).toBe("Tea Party");
    expect(canonicalizeParty("Kentucky Party")).toBe("Kentucky Party");
    // "Independent Party" is a registered party in several states — not the
    // same thing as "Independent" (no party).
    expect(canonicalizeParty("Independent Party")).toBe("Independent Party");
    expect(canonicalizeParty("Libertarian Party of Florida")).toBe("Libertarian Party of Florida");
  });

  it("keeps distinct affiliates and official phrasings as themselves", () => {
    expect(canonicalizeParty("Democratic-Farmer-Labor")).toBe("Democratic-Farmer-Labor");
    expect(canonicalizeParty("Democratic-NPL")).toBe("Democratic-NPL");
    expect(canonicalizeParty("No Political Party")).toBe("No Political Party");
    expect(canonicalizeParty("Moderate Democrat")).toBe("Moderate Democrat");
    expect(canonicalizeParty("D.C. Statehood Green")).toBe("D.C. Statehood Green");
  });

  it("passes unknown labels through cleaned but untouched", () => {
    expect(canonicalizeParty("Working Families")).toBe("Working Families");
    // Parenthesis-mangled import defects are not repaired by guessing.
    expect(canonicalizeParty("Independent) (Write-in")).toBe("Independent) (Write-in");
  });

  it("is idempotent for canonical forms and pass-throughs alike", () => {
    for (const value of [
      "Democratic",
      "Republican",
      "Libertarian",
      "Green",
      "Independent",
      "Nonpartisan",
      "Unknown",
      "Tea Party",
      "Democratic-Farmer-Labor",
    ]) {
      expect(canonicalizeParty(canonicalizeParty(value))).toBe(canonicalizeParty(value));
    }
  });
});
