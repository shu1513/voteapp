import { describe, expect, it } from "vitest";

import {
  calculateVotePower,
  decisivenessLevelFromContest,
  explainVotePower,
  representationLevelFromScore,
} from "../../../src/pipeline/address/votePower.js";

describe("representationLevelFromScore", () => {
  it("buckets representation scores into terciles", () => {
    expect(representationLevelFromScore(90)).toBe("high");
    expect(representationLevelFromScore(66)).toBe("high");
    expect(representationLevelFromScore(50)).toBe("medium");
    expect(representationLevelFromScore(33)).toBe("medium");
    expect(representationLevelFromScore(20)).toBe("low");
  });

  it("treats missing or invalid scores as unknown", () => {
    expect(representationLevelFromScore(null)).toBe("unknown");
    expect(representationLevelFromScore(undefined)).toBe("unknown");
    expect(representationLevelFromScore(Number.NaN)).toBe("unknown");
  });
});

describe("decisivenessLevelFromContest", () => {
  it("buckets historical competitiveness labels", () => {
    expect(decisivenessLevelFromContest({ raceType: "office", candidateCount: 2, competitivenessLabel: "toss_up" })).toBe(
      "high"
    );
    expect(
      decisivenessLevelFromContest({
        raceType: "office",
        candidateCount: 2,
        competitivenessLabel: "very_competitive",
      })
    ).toBe("high");
    expect(
      decisivenessLevelFromContest({ raceType: "office", candidateCount: 2, competitivenessLabel: "competitive" })
    ).toBe("medium");
    expect(
      decisivenessLevelFromContest({
        raceType: "office",
        candidateCount: 2,
        competitivenessLabel: "somewhat_competitive",
      })
    ).toBe("medium");
    expect(decisivenessLevelFromContest({ raceType: "office", candidateCount: 2, competitivenessLabel: "safe" })).toBe(
      "low"
    );
  });

  it("treats one-candidate office races as no decisiveness", () => {
    expect(decisivenessLevelFromContest({ raceType: "office", candidateCount: 1, competitivenessLabel: "toss_up" })).toBe(
      "none"
    );
  });

  it("does not treat zero-candidate office races as uncontested because rosters may be unloaded", () => {
    expect(
      decisivenessLevelFromContest({
        raceType: "office",
        candidateCount: 0,
        competitivenessLabel: null,
      })
    ).toBe("unknown");
  });

  it("does not treat zero-candidate ballot measures as uncontested", () => {
    expect(
      decisivenessLevelFromContest({
        raceType: "ballot_measure",
        candidateCount: 0,
        competitivenessLabel: null,
      })
    ).toBe("unknown");
  });

  it("treats unexpected competitiveness labels as unknown", () => {
    expect(
      decisivenessLevelFromContest({
        raceType: "office",
        candidateCount: 2,
        competitivenessLabel: "not_a_real_label" as never,
      })
    ).toBe("unknown");
  });
});

describe("calculateVotePower", () => {
  it.each([
    { representationPowerScore: 90, competitivenessLabel: "safe", label: "medium" },
    { representationPowerScore: 90, competitivenessLabel: "competitive", label: "high" },
    { representationPowerScore: 90, competitivenessLabel: "toss_up", label: "very_high" },
    { representationPowerScore: 50, competitivenessLabel: "safe", label: "low" },
    { representationPowerScore: 50, competitivenessLabel: "competitive", label: "medium" },
    { representationPowerScore: 50, competitivenessLabel: "toss_up", label: "high" },
    { representationPowerScore: 20, competitivenessLabel: "safe", label: "low" },
    { representationPowerScore: 20, competitivenessLabel: "competitive", label: "low" },
    { representationPowerScore: 20, competitivenessLabel: "toss_up", label: "medium" },
  ] as const)(
    "maps representation=$representationPowerScore and competitiveness=$competitivenessLabel to $label",
    ({ representationPowerScore, competitivenessLabel, label }) => {
      expect(
        calculateVotePower({
          raceType: "office",
          candidateCount: 2,
          representationPowerScore,
          competitivenessLabel,
        }).label
      ).toBe(label);
    }
  );

  it.each([
    { representationPowerScore: 90, label: "low" },
    { representationPowerScore: 50, label: "low" },
    { representationPowerScore: 20, label: "very_low" },
  ] as const)("maps uncontested representation=$representationPowerScore to $label", ({ representationPowerScore, label }) => {
    expect(
      calculateVotePower({
        raceType: "office",
        candidateCount: 1,
        representationPowerScore,
        competitivenessLabel: null,
      }).label
    ).toBe(label);
  });

  it("rates high representation plus high decisiveness as very high", () => {
    expect(
      calculateVotePower({
        raceType: "office",
        candidateCount: 2,
        representationPowerScore: 90,
        competitivenessLabel: "toss_up",
      })
    ).toMatchObject({
      score: 96,
      label: "very_high",
      confidence: "high",
      representation_level: "high",
      decisiveness_level: "high",
      factors: ["high_representation", "high_decisiveness"],
    });
  });

  it("rates high representation plus medium decisiveness as high", () => {
    expect(
      calculateVotePower({
        raceType: "office",
        candidateCount: 2,
        representationPowerScore: 90,
        competitivenessLabel: "competitive",
      })
    ).toMatchObject({
      score: 76,
      label: "high",
      confidence: "high",
      representation_level: "high",
      decisiveness_level: "medium",
    });
  });

  it("rates medium representation plus high decisiveness as high", () => {
    expect(
      calculateVotePower({
        raceType: "office",
        candidateCount: 2,
        representationPowerScore: 50,
        competitivenessLabel: "very_competitive",
      })
    ).toMatchObject({
      score: 69,
      label: "high",
      confidence: "high",
      representation_level: "medium",
      decisiveness_level: "high",
    });
  });

  it("caps one-candidate office races as low power", () => {
    expect(
      calculateVotePower({
        raceType: "office",
        candidateCount: 1,
        representationPowerScore: 90,
        competitivenessLabel: "toss_up",
      })
    ).toMatchObject({
      score: 25,
      label: "low",
      confidence: "high",
      decisiveness_level: "none",
      factors: ["high_representation", "uncontested_race"],
    });
  });

  it("applies a one-level direct democracy bonus to ballot measures", () => {
    expect(
      calculateVotePower({
        raceType: "ballot_measure",
        candidateCount: 0,
        representationPowerScore: 50,
        competitivenessLabel: "competitive",
      })
    ).toMatchObject({
      score: 70,
      label: "high",
      confidence: "high",
      factors: ["medium_representation", "medium_decisiveness", "direct_vote_on_policy"],
    });
  });

  it("caps the ballot measure bonus at very high", () => {
    expect(
      calculateVotePower({
        raceType: "ballot_measure",
        candidateCount: 0,
        representationPowerScore: 90,
        competitivenessLabel: "toss_up",
      })
    ).toMatchObject({
      score: 100,
      label: "very_high",
      factors: ["high_representation", "high_decisiveness", "direct_vote_on_policy"],
    });
  });

  it("keeps continuous scores more precise than display buckets", () => {
    const tossUp = calculateVotePower({
      raceType: "office",
      candidateCount: 2,
      representationPowerScore: 50,
      competitivenessLabel: "toss_up",
    });
    const veryCompetitive = calculateVotePower({
      raceType: "office",
      candidateCount: 2,
      representationPowerScore: 50,
      competitivenessLabel: "very_competitive",
    });

    expect(tossUp).toMatchObject({
      score: 78,
      label: "high",
      decisiveness_level: "high",
    });
    expect(veryCompetitive).toMatchObject({
      score: 69,
      label: "high",
      decisiveness_level: "high",
    });
  });

  it("allows the ballot measure bonus when representation is known and decisiveness is structurally unavailable", () => {
    expect(
      calculateVotePower({
        raceType: "ballot_measure",
        candidateCount: 0,
        representationPowerScore: 90,
        competitivenessLabel: null,
      })
    ).toMatchObject({
      score: 100,
      label: "very_high",
      confidence: "high",
      representation_level: "high",
      decisiveness_level: "unknown",
      factors: ["high_representation", "direct_vote_on_policy"],
    });
  });

  it("keeps ballot measure power unknown when representation is unknown", () => {
    expect(
      calculateVotePower({
        raceType: "ballot_measure",
        candidateCount: 0,
        representationPowerScore: null,
        competitivenessLabel: null,
      })
    ).toMatchObject({
      score: null,
      label: "unknown",
      confidence: "low",
      factors: ["missing_representation_data", "missing_decisiveness_data", "direct_vote_on_policy"],
    });
  });

  it("uses representation only when decisiveness is unknown", () => {
    expect(
      calculateVotePower({
        raceType: "office",
        candidateCount: 2,
        representationPowerScore: 90,
        competitivenessLabel: null,
      })
    ).toMatchObject({
      score: 79,
      label: "high",
      confidence: "medium",
      representation_level: "high",
      decisiveness_level: "unknown",
      factors: ["high_representation", "missing_decisiveness_data"],
    });
  });

  it("uses decisiveness only when representation is unknown", () => {
    expect(
      calculateVotePower({
        raceType: "office",
        candidateCount: 2,
        representationPowerScore: null,
        competitivenessLabel: "toss_up",
      })
    ).toMatchObject({
      score: 79,
      label: "high",
      confidence: "medium",
      representation_level: "unknown",
      decisiveness_level: "high",
      factors: ["missing_representation_data", "high_decisiveness"],
    });
  });

  it("returns unknown when both core inputs are unknown", () => {
    expect(
      calculateVotePower({
        raceType: "office",
        candidateCount: 2,
        representationPowerScore: null,
        competitivenessLabel: null,
      })
    ).toMatchObject({
      score: null,
      label: "unknown",
      confidence: "low",
      factors: ["missing_representation_data", "missing_decisiveness_data"],
    });
  });
});

describe("explainVotePower", () => {
  function explain(input: Parameters<typeof explainVotePower>[0]) {
    return explainVotePower(input, calculateVotePower(input));
  }

  it("explains a fully-known contested office race with graded parts, stats, and no caveat", () => {
    const explanation = explain({
      raceType: "office",
      candidateCount: 2,
      representationPowerScore: 90,
      competitivenessLabel: "toss_up",
      districtPopulation: 736081,
      representationScope: { maxPopulation: 39287377, minPopulation: 582397, description: "all statewide districts nationwide" },
      marginPercent: 1.8,
      marginElectionYears: [2022],
    });

    // The how copy explains the displayed label (grade combination), never
    // the internal 45/55 sorting-score formula.
    expect(explanation.how).toBe(
      "Vote power = representation (how much weight one vote carries here) + decisiveness (how likely this race is to be close). Each is graded low, medium, or high, and the two grades together set the rating."
    );
    expect(explanation.parts).toEqual([
      {
        title: "Representation",
        grade: "High",
        stat: "90 out of 100",
        detail:
          "Smaller districts give each vote more weight, and this district is small for its type. About 736,081 people live here.",
        formula:
          "score = 100 × ln(largest population ÷ this district's) ÷ ln(largest ÷ smallest) = 100 × ln(39,287,377 ÷ 736,081) ÷ ln(39,287,377 ÷ 582,397) = 90, comparing all statewide districts nationwide (grades: 66–100 high, 33–65 medium, 0–32 low)",
      },
      {
        title: "Decisiveness",
        grade: "High",
        stat: "1.8-point margin in 2022",
        detail: "Past results here were very close — a small number of votes could decide the winner.",
        formula:
          'margin = 1.8 points → "toss-up" → grade high (margins: 0–2 toss-up, 2–5 very competitive, 5–10 competitive, 10–15 somewhat competitive, over 15 safe; toss-up and very competitive grade high, competitive and somewhat competitive grade medium, safe grades low)',
      },
    ]);
    expect(explanation.result).toBe("High representation + high decisiveness → Very high vote power.");
    expect(explanation.caveat).toBeNull();
  });

  it("falls back to a symbolic representation formula when scope extremes are unavailable", () => {
    const explanation = explain({
      raceType: "office",
      candidateCount: 2,
      representationPowerScore: 90,
      competitivenessLabel: null,
      districtPopulation: 736081,
    });

    expect(explanation.parts[0]?.formula).toBe(
      "score = 100 × ln(largest population ÷ this district's) ÷ ln(largest ÷ smallest population among comparable districts) = 90 (grades: 66–100 high, 33–65 medium, 0–32 low)"
    );
  });

  it("shows the weighted blend arithmetic in the decisiveness formula", () => {
    const explanation = explain({
      raceType: "office",
      candidateCount: 2,
      representationPowerScore: 50,
      competitivenessLabel: "somewhat_competitive",
      marginPercent: 11.45,
      marginElectionYears: [2024, 2022],
      marginContests: [
        { marginPercent: 9.2, electionYear: 2024, weight: 0.6 },
        { marginPercent: 14.8, electionYear: 2022, weight: 0.4 },
      ],
    });

    expect(explanation.parts[1]?.formula).toBe(
      'margin = 0.6 × 9.2 (2024) + 0.4 × 14.8 (2022) = 11.5 points → "somewhat competitive" → grade medium (margins: 0–2 toss-up, 2–5 very competitive, 5–10 competitive, 10–15 somewhat competitive, over 15 safe; toss-up and very competitive grade high, competitive and somewhat competitive grade medium, safe grades low)'
    );
  });

  it("floors the representation stat so the displayed number stays in the grade's bucket", () => {
    // 65.6 is medium (< 66); rounding would display the high-threshold 66 and
    // contradict the stated grade.
    const explanation = explain({
      raceType: "office",
      candidateCount: 2,
      representationPowerScore: 65.6,
      competitivenessLabel: "safe",
    });

    expect(explanation.parts[0]).toMatchObject({ grade: "Medium", stat: "65 out of 100" });
    // Without a population the detail stays a single sentence.
    expect(explanation.parts[0]?.detail).toBe("This district is mid-sized for its type, so each vote carries average weight.");
  });

  it("drops the margin year from the stat when it is not provided", () => {
    const explanation = explain({
      raceType: "office",
      candidateCount: 2,
      representationPowerScore: 50,
      competitivenessLabel: "safe",
      marginPercent: 40,
    });

    expect(explanation.parts[1]).toMatchObject({ grade: "Low", stat: "40-point margin" });
    expect(explanation.result).toBe("Medium representation + low decisiveness → Low vote power.");
  });

  it("labels a multi-year margin as a weighted blend instead of pinning it on one year", () => {
    const explanation = explain({
      raceType: "office",
      candidateCount: 2,
      representationPowerScore: 50,
      competitivenessLabel: "somewhat_competitive",
      marginPercent: 11.45,
      marginElectionYears: [2024, 2022],
    });

    expect(explanation.parts[1]?.stat).toBe("11.5-point weighted margin across 2024 and 2022");
  });

  it("mirrors the parts as transitional reason bullets for pre-parts frontends", () => {
    const explanation = explain({
      raceType: "office",
      candidateCount: 2,
      representationPowerScore: 90,
      competitivenessLabel: "toss_up",
      marginPercent: 3.25,
      marginElectionYears: [2022],
    });

    expect(explanation.reasons).toEqual([
      "Representation: High (90 out of 100). Smaller districts give each vote more weight, and this district is small for its type.",
      "Decisiveness: High (3.3-point margin in 2022). Past results here were very close — a small number of votes could decide the winner.",
    ]);
  });

  it("explains an uncontested race without a margin stat", () => {
    const explanation = explain({
      raceType: "office",
      candidateCount: 1,
      representationPowerScore: 90,
      competitivenessLabel: "toss_up",
      marginPercent: 3.25,
      marginElectionYears: [2022],
    });

    expect(explanation.parts[1]).toEqual({
      title: "Decisiveness",
      grade: "None",
      stat: "only 1 candidate",
      detail: "One candidate is running unopposed, so votes can't change the outcome.",
      formula: null,
    });
    expect(explanation.result).toBe("High representation + an uncontested race → Low vote power.");
  });

  it("qualifies decisiveness when the historical results predate redistricting", () => {
    const explanation = explain({
      raceType: "office",
      candidateCount: 2,
      representationPowerScore: 90,
      competitivenessLabel: "toss_up",
      staleAfterRedistricting: true,
    });

    expect(explanation.parts[1]?.detail).toBe(
      "Past results here were very close — a small number of votes could decide the winner. District lines have changed since then, so older results are a weaker guide."
    );
    // Staleness qualifies historical evidence only; representation is a
    // present-day population measure.
    expect(explanation.parts[0]?.detail).not.toContain("District lines");
  });

  it("adds a ballot-measure boost part and skips the decisiveness row when history is structurally absent", () => {
    const explanation = explain({
      raceType: "ballot_measure",
      candidateCount: 0,
      representationPowerScore: 90,
      competitivenessLabel: null,
    });

    expect(explanation.parts.map((part) => part.title)).toEqual(["Representation", "Ballot measure"]);
    expect(explanation.parts[1]).toEqual({
      title: "Ballot measure",
      grade: "+1 step",
      stat: null,
      detail: "Your vote sets the policy directly, so the rating gets a one-step boost.",
      formula: null,
    });
    expect(explanation.result).toBe("High representation + a ballot-measure boost → Very high vote power.");
    expect(explanation.caveat).toBeNull();
  });

  it("keeps the decisiveness row for ballot measures that have history", () => {
    const explanation = explain({
      raceType: "ballot_measure",
      candidateCount: 0,
      representationPowerScore: 50,
      competitivenessLabel: "competitive",
    });

    expect(explanation.parts.map((part) => part.title)).toEqual(["Representation", "Decisiveness", "Ballot measure"]);
    expect(explanation.result).toBe(
      "Medium representation + medium decisiveness + a ballot-measure boost → High vote power."
    );
  });

  it("does not claim a boost when the measure was already rated very high", () => {
    // high/high matrixes to very_high before the bump; bumpLabel tops out
    // there, so no step was actually applied.
    const explanation = explain({
      raceType: "ballot_measure",
      candidateCount: 0,
      representationPowerScore: 90,
      competitivenessLabel: "toss_up",
    });

    expect(explanation.parts[2]).toEqual({
      title: "Ballot measure",
      grade: "Direct vote",
      stat: null,
      detail: "Your vote sets the policy directly, but it did not raise this rating further.",
      formula: null,
    });
    expect(explanation.result).toBe("High representation + high decisiveness → Very high vote power.");
  });

  it("does not claim a boost when the missing-data cap ate the bump", () => {
    // Unknown representation + high decisiveness rates high; the bump to
    // very_high is then capped back to high, so the label never moved.
    const explanation = explain({
      raceType: "ballot_measure",
      candidateCount: 0,
      representationPowerScore: null,
      competitivenessLabel: "toss_up",
    });

    expect(explanation.parts[2]).toMatchObject({ grade: "Direct vote" });
    expect(explanation.result).toBe("Unknown representation + high decisiveness → High vote power.");
  });

  it("carries a partial-data caveat when one core axis is missing", () => {
    const explanation = explain({
      raceType: "office",
      candidateCount: 2,
      representationPowerScore: 90,
      competitivenessLabel: null,
    });

    expect(explanation.parts[1]).toEqual({
      title: "Decisiveness",
      grade: "Unknown",
      stat: null,
      detail: "No past results for this contest yet.",
      formula: null,
    });
    expect(explanation.result).toBe("High representation + unknown decisiveness → High vote power.");
    expect(explanation.caveat).toContain("partial information");
  });

  it("reports no rating when both core axes are missing", () => {
    const explanation = explain({
      raceType: "office",
      candidateCount: 2,
      representationPowerScore: null,
      competitivenessLabel: null,
    });

    expect(explanation.parts).toEqual([
      {
        title: "Representation",
        grade: "Unknown",
        stat: null,
        detail: "We don't have a representation score for this district yet.",
        formula: null,
      },
      {
        title: "Decisiveness",
        grade: "Unknown",
        stat: null,
        detail: "No past results for this contest yet.",
        formula: null,
      },
    ]);
    expect(explanation.result).toBe("Not enough data → no rating yet.");
    expect(explanation.caveat).toBe("Not enough data to rate this election yet.");
  });
});
