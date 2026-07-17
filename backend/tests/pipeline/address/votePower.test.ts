import { describe, expect, it } from "vitest";

import {
  calculateVotePower,
  decisivenessLevelFromContest,
  explainVotePower,
  representationLevelFromScore,
  type VotePowerInput,
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
  function explain(input: VotePowerInput) {
    return explainVotePower(input, calculateVotePower(input));
  }

  it("explains a fully-known contested office race with one reason per factor and no caveat", () => {
    const explanation = explain({
      raceType: "office",
      candidateCount: 2,
      representationPowerScore: 90,
      competitivenessLabel: "toss_up",
    });

    expect(explanation.how).toContain("representation");
    expect(explanation.how).toContain("decisiveness");
    expect(explanation.how).toContain("55%");
    expect(explanation.reasons).toEqual([
      "Representation is high (90 out of 100): this district's population is small for its type, so each vote is a larger share of the outcome.",
      "Decisiveness is high: past results for this contest were very close, so a small number of votes could decide it.",
    ]);
    expect(explanation.caveat).toBeNull();
  });

  it("rounds the representation score in the reason text", () => {
    const explanation = explain({
      raceType: "office",
      candidateCount: 2,
      representationPowerScore: 64.25,
      competitivenessLabel: "safe",
    });

    expect(explanation.reasons[0]).toContain("(64 out of 100)");
  });

  it("explains an uncontested race without a margin reason", () => {
    const explanation = explain({
      raceType: "office",
      candidateCount: 1,
      representationPowerScore: 90,
      competitivenessLabel: "toss_up",
    });

    expect(explanation.reasons).toEqual([
      "Representation is high (90 out of 100): this district's population is small for its type, so each vote is a larger share of the outcome.",
      "Only one candidate is on the ballot, so the outcome will not turn on vote margin.",
    ]);
  });

  it("explains the ballot measure boost", () => {
    const explanation = explain({
      raceType: "ballot_measure",
      candidateCount: 0,
      representationPowerScore: 50,
      competitivenessLabel: "competitive",
    });

    expect(explanation.reasons).toContain(
      "This is a ballot measure: your vote sets policy directly instead of electing a representative, which raises the rating one step."
    );
  });

  it("carries a partial-data caveat when one core axis is missing", () => {
    const explanation = explain({
      raceType: "office",
      candidateCount: 2,
      representationPowerScore: 90,
      competitivenessLabel: null,
    });

    expect(explanation.reasons).toContain("No past-results data is available for this contest yet.");
    expect(explanation.caveat).toContain("partial information");
  });

  it("carries a low-confidence caveat when both core axes are missing", () => {
    const explanation = explain({
      raceType: "office",
      candidateCount: 2,
      representationPowerScore: null,
      competitivenessLabel: null,
    });

    expect(explanation.reasons).toEqual([
      "No representation score is available for this district yet.",
      "No past-results data is available for this contest yet.",
    ]);
    expect(explanation.caveat).toBe("Not enough data is available to rate vote power for this election.");
  });
});
