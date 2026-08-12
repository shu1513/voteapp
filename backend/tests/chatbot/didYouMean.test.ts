import { describe, expect, it } from "vitest";

import {
  bestNameSimilarity,
  suggestClosestCandidates,
  trigramSimilarity,
  SUGGESTION_MIN_SIMILARITY,
} from "../../src/chatbot/didYouMean.js";
import type { CandidateEntityMatch } from "../../src/chatbot/retrieval.js";

function match(displayName: string, similarity: number, extras: Partial<CandidateEntityMatch> = {}): CandidateEntityMatch {
  return {
    candidateId: extras.candidateId ?? "00000000-0000-4000-a000-000000000001",
    displayName,
    party: extras.party ?? "Democratic",
    state: extras.state ?? "GA",
    currentOffice: extras.currentOffice ?? null,
    similarity,
  };
}

describe("trigramSimilarity mirrors pg_trgm", () => {
  it("scores identical strings 1 and disjoint strings 0", () => {
    expect(trigramSimilarity("jon ossoff", "jon ossoff")).toBe(1);
    expect(trigramSimilarity("zzz", "qqq")).toBe(0);
    expect(trigramSimilarity("", "jon")).toBe(0);
  });

  it("scores a typo of a whole name far above a shared-surname coincidence", () => {
    const typo = trigramSimilarity("jon osoff", "jon ossoff");
    const coincidence = trigramSimilarity("taylor swift", "tim taylor");
    expect(typo).toBeGreaterThanOrEqual(SUGGESTION_MIN_SIMILARITY);
    expect(coincidence).toBeLessThan(SUGGESTION_MIN_SIMILARITY);
  });
});

describe("bestNameSimilarity", () => {
  it("finds the typo span inside a full question", () => {
    expect(bestNameSimilarity("What party is Jon Osoff in?", "Jon Ossoff")).toBeGreaterThanOrEqual(
      SUGGESTION_MIN_SIMILARITY
    );
  });

  it("matches through the first+last form when the stored name has a middle initial", () => {
    expect(bestNameSimilarity("Tell me about Micheal Smith", "Michael L. Smith")).toBeGreaterThanOrEqual(
      SUGGESTION_MIN_SIMILARITY
    );
  });

  it("never scores single-token overlap (surname coincidence stays a refusal)", () => {
    // "Taylor" alone matches nothing: spans need 2+ consecutive non-stopword
    // tokens, and "is Taylor" is broken by the stopword.
    expect(bestNameSimilarity("What is Taylor Swift's net worth?", "Tim Taylor")).toBeLessThan(
      SUGGESTION_MIN_SIMILARITY
    );
    expect(bestNameSimilarity("Who is the mayor of Paris?", "Chris Parish")).toBeLessThan(SUGGESTION_MIN_SIMILARITY);
  });
});

describe("suggestClosestCandidates", () => {
  it("suggests the near-miss name for a typo question", () => {
    const suggestions = suggestClosestCandidates("What party is Jon Osoff in?", [
      match("Jon Ossoff", 0.69),
      match("Tim Taylor", 0.5, { candidateId: "00000000-0000-4000-a000-000000000002" }),
    ]);
    expect(suggestions.map((s) => s.displayName)).toEqual(["Jon Ossoff"]);
  });

  it("returns nothing for surname-coincidence questions", () => {
    expect(
      suggestClosestCandidates("What is Taylor Swift's net worth?", [match("Tim Taylor", 0.7)])
    ).toEqual([]);
  });

  it("dedupes same-person name variants and caps at three", () => {
    const suggestions = suggestClosestCandidates("Tell me about Micheal Smith", [
      match("Michael Smith", 0.7, { candidateId: "00000000-0000-4000-a000-000000000003" }),
      match("Michael L. Smith", 0.65, { candidateId: "00000000-0000-4000-a000-000000000004" }),
      match("Micheal Smyth", 0.6, { candidateId: "00000000-0000-4000-a000-000000000005" }),
      match("Michele Smith", 0.6, { candidateId: "00000000-0000-4000-a000-000000000006" }),
      match("Mitchell Smith", 0.55, { candidateId: "00000000-0000-4000-a000-000000000007" }),
    ]);
    // "Micheal Smyth" is literally closest to the typo (exact "micheal");
    // "Michael Smith" and "Michael L. Smith" collapse to one first+last key.
    const names = suggestions.map((s) => s.displayName);
    expect(names[0]).toBe("Micheal Smyth");
    expect(names).toContain("Michael Smith");
    expect(names).not.toContain("Michael L. Smith");
    expect(suggestions.length).toBeLessThanOrEqual(3);
  });

  it("returns nothing when there are no entity matches", () => {
    expect(suggestClosestCandidates("who won the 2020 election?", [])).toEqual([]);
  });
});
