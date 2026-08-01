import { describe, expect, it } from "vitest";

import { canonicalizeParty } from "../../src/pipeline/candidates/candidatePartyCanonicalization.js";

describe("canonicalizeParty", () => {
  it("maps every observed spelling variant to its canonical form", () => {
    const variants: [string, string][] = [
      ["DEM", "Democratic"],
      ["Democrat", "Democratic"],
      ["Democratic Party", "Democratic"],
      ["DEMOCRATIC", "Democratic"],
      ["REP", "Republican"],
      ["Republican Party", "Republican"],
      ["REPUBLICAN", "Republican"],
      ["LIB", "Libertarian"],
      ["Libertarian Party", "Libertarian"],
      ["GRE", "Green"],
      ["IND", "Independent"],
      ["INDEPENDENT", "Independent"],
      ["NONPARTISAN", "Nonpartisan"],
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

  it("keeps Alaska's registration labels — a registration is not an affiliation", () => {
    // AK's top-four ballot prints the candidate's voter registration exactly
    // because it is not a party nomination; "Registered Republican" must not
    // become the affiliation claim "Republican".
    expect(canonicalizeParty("Registered Republican")).toBe("Registered Republican");
    expect(canonicalizeParty("Registered Democrat")).toBe("Registered Democrat");
    expect(canonicalizeParty("Registered Libertarian")).toBe("Registered Libertarian");
  });

  it("keeps minor parties whose official name carries the 'Party' suffix", () => {
    // Unlike the major-party adjective convention, these full names ARE the
    // names — and same-looking labels can be different state parties
    // ("Independent American Party" NV vs "Independent American" UT).
    expect(canonicalizeParty("Constitution Party")).toBe("Constitution Party");
    expect(canonicalizeParty("Independent American Party")).toBe("Independent American Party");
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
