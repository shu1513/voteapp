export const HISTORICAL_CONTEST_COMPETITIVENESS_LABELS = [
  "toss_up",
  "very_competitive",
  "competitive",
  "somewhat_competitive",
  "safe",
] as const;

export type HistoricalContestCompetitivenessLabel =
  (typeof HISTORICAL_CONTEST_COMPETITIVENESS_LABELS)[number];

export type HistoricalContestMarginInput = {
  winnerVotes: number;
  runnerUpVotes: number;
  totalVotes: number;
};

export type HistoricalContestMarginResult = {
  marginPercent: number;
  competitivenessLabel: HistoricalContestCompetitivenessLabel;
};

function isNonNegativeInteger(value: number): boolean {
  return Number.isInteger(value) && value >= 0;
}

export function roundHistoricalContestMarginPercent(value: number): number {
  if (!Number.isFinite(value) || value < 0 || value > 100) {
    throw new Error(`Invalid historical contest margin percent: ${value}`);
  }
  return Math.round(value * 100) / 100;
}

export function classifyHistoricalContestMargin(
  marginPercent: number
): HistoricalContestCompetitivenessLabel {
  const roundedMargin = roundHistoricalContestMarginPercent(marginPercent);

  if (roundedMargin <= 2) {
    return "toss_up";
  }
  if (roundedMargin <= 5) {
    return "very_competitive";
  }
  if (roundedMargin <= 10) {
    return "competitive";
  }
  if (roundedMargin <= 15) {
    return "somewhat_competitive";
  }
  return "safe";
}

export function calculateHistoricalContestMargin(
  input: HistoricalContestMarginInput
): HistoricalContestMarginResult | null {
  if (
    !isNonNegativeInteger(input.winnerVotes) ||
    !isNonNegativeInteger(input.runnerUpVotes) ||
    !isNonNegativeInteger(input.totalVotes) ||
    input.totalVotes === 0 ||
    input.winnerVotes < input.runnerUpVotes ||
    input.winnerVotes + input.runnerUpVotes > input.totalVotes
  ) {
    return null;
  }

  const marginPercent = roundHistoricalContestMarginPercent(
    ((input.winnerVotes - input.runnerUpVotes) / input.totalVotes) * 100
  );

  return {
    marginPercent,
    competitivenessLabel: classifyHistoricalContestMargin(marginPercent),
  };
}
