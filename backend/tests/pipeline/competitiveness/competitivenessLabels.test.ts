import { describe, expect, it } from "vitest";

import {
  COMPETITIVENESS_LABELS,
  HISTORICAL_CONTEST_COMPETITIVENESS_LABELS,
  calculateHistoricalContestMargin,
  classifyHistoricalContestMargin,
  roundHistoricalContestMarginPercent,
} from "../../../src/pipeline/competitiveness/competitivenessLabels.js";

describe("competitivenessLabels", () => {
  it("keeps the historical label export as an alias of the shared enum", () => {
    expect(HISTORICAL_CONTEST_COMPETITIVENESS_LABELS).toBe(COMPETITIVENESS_LABELS);
  });

  it("rounds historical contest margins to two decimals", () => {
    expect(roundHistoricalContestMarginPercent(0)).toBe(0);
    expect(roundHistoricalContestMarginPercent(1.234)).toBe(1.23);
    expect(roundHistoricalContestMarginPercent(1.235)).toBe(1.24);
    expect(roundHistoricalContestMarginPercent(100)).toBe(100);
  });

  it("rejects invalid margin percentages", () => {
    expect(() => roundHistoricalContestMarginPercent(-0.01)).toThrow("Invalid historical contest margin percent");
    expect(() => roundHistoricalContestMarginPercent(100.01)).toThrow("Invalid historical contest margin percent");
    expect(() => roundHistoricalContestMarginPercent(Number.NaN)).toThrow("Invalid historical contest margin percent");
  });

  it("classifies margins using inclusive upper bounds", () => {
    expect(classifyHistoricalContestMargin(0)).toBe("toss_up");
    expect(classifyHistoricalContestMargin(2)).toBe("toss_up");
    expect(classifyHistoricalContestMargin(2.01)).toBe("very_competitive");
    expect(classifyHistoricalContestMargin(5)).toBe("very_competitive");
    expect(classifyHistoricalContestMargin(5.01)).toBe("competitive");
    expect(classifyHistoricalContestMargin(10)).toBe("competitive");
    expect(classifyHistoricalContestMargin(10.01)).toBe("somewhat_competitive");
    expect(classifyHistoricalContestMargin(15)).toBe("somewhat_competitive");
    expect(classifyHistoricalContestMargin(15.01)).toBe("safe");
  });

  it("calculates margin percent and competitiveness label from top-two vote totals", () => {
    expect(calculateHistoricalContestMargin({ winnerVotes: 55, runnerUpVotes: 45, totalVotes: 100 })).toEqual({
      marginPercent: 10,
      competitivenessLabel: "competitive",
    });

    expect(calculateHistoricalContestMargin({ winnerVotes: 1_010, runnerUpVotes: 990, totalVotes: 2_500 })).toEqual({
      marginPercent: 0.8,
      competitivenessLabel: "toss_up",
    });
  });

  it("allows tied top-two vote totals", () => {
    expect(calculateHistoricalContestMargin({ winnerVotes: 100, runnerUpVotes: 100, totalVotes: 250 })).toEqual({
      marginPercent: 0,
      competitivenessLabel: "toss_up",
    });
  });

  it("returns null for invalid vote totals", () => {
    expect(calculateHistoricalContestMargin({ winnerVotes: 0, runnerUpVotes: 0, totalVotes: 0 })).toBeNull();
    expect(calculateHistoricalContestMargin({ winnerVotes: 99, runnerUpVotes: 100, totalVotes: 250 })).toBeNull();
    expect(calculateHistoricalContestMargin({ winnerVotes: 200, runnerUpVotes: 100, totalVotes: 250 })).toBeNull();
    expect(calculateHistoricalContestMargin({ winnerVotes: 1.5, runnerUpVotes: 1, totalVotes: 10 })).toBeNull();
    expect(calculateHistoricalContestMargin({ winnerVotes: -1, runnerUpVotes: 0, totalVotes: 10 })).toBeNull();
  });
});
