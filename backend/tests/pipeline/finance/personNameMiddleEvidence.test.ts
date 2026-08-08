import { describe, expect, it } from "vitest";

import {
  hasMiddleNameConflict,
  middleNameEvidence,
  parsePersonNameCandidates,
  personNameParseVariants,
  personNamesMatchWithMiddleEvidence,
} from "../../../src/pipeline/finance/personNameMiddleEvidence.js";

// A representative state normalizer (the tennessee-pattern one): uppercase,
// strip punctuation and generational suffixes.
function normalizePersonName(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[̀-ͯ]/g, "")
    .toUpperCase()
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\b(JR|SR|II|III|IV|V)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

describe("parsePersonNameCandidates", () => {
  it("parses comma form into a single unambiguous split", () => {
    expect(parsePersonNameCandidates("Carr, Christopher M.", normalizePersonName)).toEqual([
      { first: "CHRISTOPHER", middles: ["M"], last: "CARR" },
    ]);
  });

  it("emits every surname split for space forms", () => {
    expect(parsePersonNameCandidates("Mary Van Dyke", normalizePersonName)).toEqual([
      { first: "MARY", middles: [], last: "VAN DYKE" },
      { first: "MARY", middles: ["VAN"], last: "DYKE" },
    ]);
  });

  it("treats a single token as first and last", () => {
    expect(parsePersonNameCandidates("Cher", normalizePersonName)).toEqual([
      { first: "CHER", middles: [], last: "CHER" },
    ]);
  });

  it("returns no parses for empty or comma-degenerate input", () => {
    expect(parsePersonNameCandidates("  ", normalizePersonName)).toEqual([]);
    expect(parsePersonNameCandidates("Smith,", normalizePersonName)).toEqual([]);
  });
});

describe("personNameParseVariants", () => {
  it("parses the outer name and each parenthetical alias", () => {
    const variants = personNameParseVariants("LEE, Bill (Bill Lee)", normalizePersonName);
    expect(variants).toContainEqual({ first: "BILL", middles: [], last: "LEE" });
    expect(variants.length).toBeGreaterThan(1);
  });
});

describe("middleNameEvidence", () => {
  it("is weak when either side lacks middles", () => {
    expect(middleNameEvidence([], [])).toBe("weak");
    expect(middleNameEvidence(["A"], [])).toBe("weak");
    expect(middleNameEvidence([], ["ANDREW"])).toBe("weak");
  });

  it("is strong on equal middles or a corroborating initial", () => {
    expect(middleNameEvidence(["ANDREW"], ["ANDREW"])).toBe("strong");
    expect(middleNameEvidence(["A"], ["ANDREW"])).toBe("strong");
    expect(middleNameEvidence(["ANDREW"], ["A"])).toBe("strong");
  });

  it("is conflict on contradicting middles", () => {
    expect(middleNameEvidence(["A"], ["B"])).toBe("conflict");
    expect(middleNameEvidence(["ANDREW"], ["BERNARD"])).toBe("conflict");
    expect(middleNameEvidence(["ANDREW"], ["ANN"])).toBe("conflict");
  });

  it("compares every shared middle position, not just the first", () => {
    expect(middleNameEvidence(["MICHAEL", "ANDREW"], ["MICHAEL", "BERNARD"])).toBe("conflict");
    expect(middleNameEvidence(["A", "B"], ["A", "C"])).toBe("conflict");
    expect(middleNameEvidence(["A", "B"], ["ANDREW", "BERNARD"])).toBe("strong");
    // Tokens past the shorter side carry no evidence.
    expect(middleNameEvidence(["MICHAEL"], ["MICHAEL", "BERNARD"])).toBe("strong");
  });
});

describe("hasMiddleNameConflict", () => {
  function conflict(candidateName: string, rowNames: string[]): boolean {
    return hasMiddleNameConflict({ candidateName, rowNames, normalizePersonName });
  }

  it("vetoes conflicting middle initials even when first and last agree", () => {
    expect(conflict("John A. Smith", ["Smith, John B."])).toBe(true);
    expect(conflict("John Andrew Smith", ["Smith, John Bernard"])).toBe(true);
  });

  it("does not veto an initial that corroborates the full middle name", () => {
    expect(conflict("John A. Smith", ["Smith, John Andrew"])).toBe(false);
    expect(conflict("John Andrew Smith", ["Smith, John A."])).toBe(false);
  });

  it("does not veto the first+last fallback when a side lacks middle info", () => {
    expect(conflict("John Smith", ["Smith, John B."])).toBe(false);
    expect(conflict("John A. Smith", ["Smith, John"])).toBe(false);
  });

  it("lets a middle conflict veto a middle-less variant of the same row", () => {
    // One row name lacks the middle, but a sibling name carries a
    // contradicting one: the conflict wins over the weak fallback.
    expect(conflict("John A. Smith", ["John Smith", "Smith, John B."])).toBe(true);
  });

  it("lets corroboration on any row name clear a conflict on another", () => {
    expect(conflict("John A. Smith", ["Smith, John B.", "John Andrew Smith"])).toBe(false);
  });

  it("stays out of the way when no variant pair aligns on first+last", () => {
    expect(conflict("John A. Smith", ["Jones, Mary B."])).toBe(false);
    expect(conflict("John A. Smith", ["Friends Of Somebody Else"])).toBe(false);
    expect(conflict("John A. Smith", [])).toBe(false);
  });

  it("vetoes a contradiction hidden past a matching first middle", () => {
    expect(conflict("John Michael Andrew Smith", ["Smith, John Michael Bernard"])).toBe(true);
    expect(conflict("John A. B. Smith", ["Smith, John A. C."])).toBe(true);
  });

  it("treats a single-token parenthetical as a call name that keeps outer middle evidence", () => {
    // "Glenn A. (Mike) Prax" is one person: Mike substitutes the first name,
    // the A stays. A filer "Prax, Mike B" contradicts it.
    expect(conflict("Glenn A. (Mike) Prax", ["Prax, Mike B"])).toBe(true);
    expect(conflict("Robert A. (Bob) Smith", ["Smith, Bob B"])).toBe(true);
    // Weak fallback and corroboration behave as usual through the call name.
    expect(conflict("Robert (Bob) Smith", ["Smith, Bob B"])).toBe(false);
    expect(conflict("Robert A. (Bob) Smith", ["Smith, Bob Andrew"])).toBe(false);
  });

  it("does not read a single-letter parenthetical as a call name", () => {
    // "(D)" is a party marker, not a nickname.
    expect(conflict("Jane Doe (D)", ["Doe, Jane B"])).toBe(false);
  });

  it("keeps multi-word surnames aligned across name orders", () => {
    expect(conflict("Mary Van Dyke", ["Van Dyke, Mary"])).toBe(false);
    expect(conflict("Mary A. Van Dyke", ["Van Dyke, Mary B."])).toBe(true);
  });

  it("ignores generational suffixes stripped by the normalizer", () => {
    expect(conflict("John Smith Jr.", ["Smith, John"])).toBe(false);
  });

  it("agrees with personNamesMatchWithMiddleEvidence on every shared case", () => {
    // Wherever parses align, the veto and the full matcher are complements.
    const cases: Array<[string, string[]]> = [
      ["John A. Smith", ["Smith, John B."]],
      ["John A. Smith", ["Smith, John Andrew"]],
      ["John Smith", ["Smith, John B."]],
      ["John A. Smith", ["John Smith", "Smith, John B."]],
      ["Mary A. Van Dyke", ["Van Dyke, Mary B."]],
    ];
    for (const [candidateName, rowNames] of cases) {
      expect(personNamesMatchWithMiddleEvidence({ candidateName, rowNames, normalizePersonName })).toBe(
        !hasMiddleNameConflict({ candidateName, rowNames, normalizePersonName })
      );
    }
  });

  it("honors a caller-supplied first-name equivalence for nickname states", () => {
    const nicknameEquivalent = (candidateFirst: string, rowFirst: string) =>
      candidateFirst === rowFirst || (candidateFirst === "MIKE" && rowFirst === "MICHAEL");
    expect(
      hasMiddleNameConflict({
        candidateName: "Mike A. Smith",
        rowNames: ["SMITH, MICHAEL B"],
        normalizePersonName,
        firstNamesEquivalent: nicknameEquivalent,
      })
    ).toBe(true);
    // Without the equivalence the pair never aligns, so no veto.
    expect(conflict("Mike A. Smith", ["SMITH, MICHAEL B"])).toBe(false);
  });
});

describe("personNamesMatchWithMiddleEvidence", () => {
  function matches(candidateName: string, rowNames: string[]): boolean {
    return personNamesMatchWithMiddleEvidence({ candidateName, rowNames, normalizePersonName });
  }

  it("recovers first+last alignments the exact-key states miss", () => {
    // The colorado-pattern recall gap: full-string keys never overlap when
    // only one side carries a middle.
    expect(matches("John A. Smith", ["Smith, John"])).toBe(true);
    expect(matches("John Smith", ["Smith, John A."])).toBe(true);
    expect(matches("John Smith", ["John Smith"])).toBe(true);
  });

  it("matches on corroborating middles and rejects contradicting ones", () => {
    expect(matches("John A. Smith", ["Smith, John Andrew"])).toBe(true);
    expect(matches("John A. Smith", ["Smith, John B."])).toBe(false);
  });

  it("requires an actual first+last alignment", () => {
    expect(matches("John Smith", ["Jones, Mary"])).toBe(false);
    expect(matches("John Smith", [])).toBe(false);
    expect(matches("John Smith", ["Smithers, John"])).toBe(false);
  });

  it("lets a conflict on one row name veto a weak alignment on another", () => {
    expect(matches("John A. Smith", ["John Smith", "Smith, John B."])).toBe(false);
  });
});
