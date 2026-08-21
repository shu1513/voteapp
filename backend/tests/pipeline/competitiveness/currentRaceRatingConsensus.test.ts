import { describe, expect, it } from "vitest";

import {
  deriveConsensusLabel,
  deriveMayoralLabelFromMargin,
  parseOutletRawRating,
  type CurrentRaceRatingObservation,
} from "../../../src/pipeline/competitiveness/currentRaceRatingConsensus.js";

function observation(
  overrides: Partial<CurrentRaceRatingObservation> = {}
): CurrentRaceRatingObservation {
  return {
    outlet: "inside_elections",
    raw_rating: "Tilt Democrat",
    favored: "D",
    intensity: 2,
    as_of: "2026-08-06",
    url: "https://insideelections.com/ratings/senate",
    ...overrides,
  };
}

describe("deriveConsensusLabel", () => {
  it("throws on an empty observation list", () => {
    expect(() => deriveConsensusLabel([])).toThrow(/at least one observation/);
  });

  it("derives competitive from IE Tilt D + Sabato Lean D (the plan's worked example)", () => {
    const result = deriveConsensusLabel([
      observation(),
      observation({
        outlet: "sabato",
        raw_rating: "Leans Democratic",
        intensity: 3,
        url: "https://centerforpolitics.org/crystalball/2026-senate",
      }),
    ]);
    expect(result).toEqual({
      competitiveness_label: "competitive",
      confidence: "high",
      mean_intensity: 2.5,
    });
  });

  it("grades a single toss-up observation as toss_up with medium confidence", () => {
    const result = deriveConsensusLabel([
      observation({ raw_rating: "Toss-up", favored: "none", intensity: 0 }),
    ]);
    expect(result.competitiveness_label).toBe("toss_up");
    expect(result.confidence).toBe("medium");
  });

  it("caps opposite-favored outlets at very_competitive with medium confidence", () => {
    const result = deriveConsensusLabel([
      observation({ raw_rating: "Solid Democrat", intensity: 5 }),
      observation({
        outlet: "sabato",
        raw_rating: "Safe Republican",
        favored: "R",
        intensity: 5,
        url: "https://centerforpolitics.org/crystalball/2026-senate",
      }),
    ]);
    expect(result.competitiveness_label).toBe("very_competitive");
    expect(result.confidence).toBe("medium");
  });

  it("marks opposite-favored tilts very_competitive at medium confidence", () => {
    const result = deriveConsensusLabel([
      observation({ raw_rating: "Tilt Democrat", intensity: 2 }),
      observation({
        outlet: "sabato",
        raw_rating: "Leans Republican",
        favored: "R",
        intensity: 3,
        url: "https://centerforpolitics.org/crystalball/2026-senate",
      }),
    ]);
    expect(result.competitiveness_label).toBe("very_competitive");
    expect(result.confidence).toBe("medium");
  });

  it("refuses safe unless every outlet is Solid on the same side", () => {
    const result = deriveConsensusLabel([
      observation({ raw_rating: "Likely Democrat", intensity: 4 }),
      observation({
        outlet: "sabato",
        raw_rating: "Safe Democratic",
        intensity: 5,
        url: "https://centerforpolitics.org/crystalball/2026-senate",
      }),
    ]);
    expect(result.competitiveness_label).toBe("somewhat_competitive");
    expect(result.confidence).toBe("high");
  });

  it("grades all-Solid same-side outlets as safe with high confidence", () => {
    const result = deriveConsensusLabel([
      observation({ raw_rating: "Solid Democrat", intensity: 5 }),
      observation({
        outlet: "sabato",
        raw_rating: "Safe Democratic",
        intensity: 5,
        url: "https://centerforpolitics.org/crystalball/2026-senate",
      }),
    ]);
    expect(result.competitiveness_label).toBe("safe");
    expect(result.confidence).toBe("high");
  });

  it("allows single-outlet safe at medium confidence (Sabato-absent House seats)", () => {
    const result = deriveConsensusLabel([
      observation({ raw_rating: "Solid Republican", favored: "R", intensity: 5 }),
    ]);
    expect(result.competitiveness_label).toBe("safe");
    expect(result.confidence).toBe("medium");
  });

  it("supports independent favored sides in the same-side safe guardrail", () => {
    const result = deriveConsensusLabel([
      observation({ raw_rating: "Solid Independent", favored: "I", intensity: 5 }),
      observation({
        outlet: "sabato",
        raw_rating: "Safe Independent",
        favored: "I",
        intensity: 5,
        url: "https://centerforpolitics.org/crystalball/2026-senate",
      }),
    ]);
    expect(result.competitiveness_label).toBe("safe");
    expect(result.confidence).toBe("high");
  });
});

describe("parseOutletRawRating", () => {
  it("parses the full outlet vocabulary", () => {
    expect(parseOutletRawRating("Toss-up")).toEqual({ favored: "none", intensity: 0 });
    expect(parseOutletRawRating("Tossup")).toEqual({ favored: "none", intensity: 0 });
    expect(parseOutletRawRating("Tilt Democrat")).toEqual({ favored: "D", intensity: 2 });
    expect(parseOutletRawRating("Lean Republican")).toEqual({ favored: "R", intensity: 3 });
    expect(parseOutletRawRating("Leans Democratic")).toEqual({ favored: "D", intensity: 3 });
    expect(parseOutletRawRating("Likely Republican")).toEqual({ favored: "R", intensity: 4 });
    expect(parseOutletRawRating("Solid Democrat")).toEqual({ favored: "D", intensity: 5 });
    expect(parseOutletRawRating("Safe Republican")).toEqual({ favored: "R", intensity: 5 });
    expect(parseOutletRawRating("Solid Independent")).toEqual({ favored: "I", intensity: 5 });
    expect(parseOutletRawRating("  safe   democratic  ")).toEqual({ favored: "D", intensity: 5 });
  });

  it("returns null for anything outside the vocabulary", () => {
    expect(parseOutletRawRating("Battleground")).toBeNull();
    expect(parseOutletRawRating("Lean")).toBeNull();
    expect(parseOutletRawRating("Democrat")).toBeNull();
    expect(parseOutletRawRating("Very Likely Republican")).toBeNull();
    expect(parseOutletRawRating("Solid Green")).toBeNull();
    expect(parseOutletRawRating("")).toBeNull();
  });
});

describe("deriveMayoralLabelFromMargin", () => {
  it("maps margins through the shared competitiveness bins", () => {
    expect(deriveMayoralLabelFromMargin(2)).toBe("toss_up");
    expect(deriveMayoralLabelFromMargin(5)).toBe("very_competitive");
    expect(deriveMayoralLabelFromMargin(10)).toBe("competitive");
    expect(deriveMayoralLabelFromMargin(15)).toBe("somewhat_competitive");
    expect(deriveMayoralLabelFromMargin(15.01)).toBe("safe");
  });
});
