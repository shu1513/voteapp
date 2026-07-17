import type { ElectionRaceType } from "../../types/election.js";
import type { HistoricalContestCompetitivenessLabel } from "../competitiveness/competitivenessLabels.js";

export type VotePowerLabel = "very_low" | "low" | "medium" | "high" | "very_high" | "unknown";

export type VotePowerConfidence = "high" | "medium" | "low";

export type VotePowerRepresentationLevel = "low" | "medium" | "high" | "unknown";

export type VotePowerDecisivenessLevel = "none" | "low" | "medium" | "high" | "unknown";

export type VotePowerFactor =
  | "high_representation"
  | "medium_representation"
  | "low_representation"
  | "high_decisiveness"
  | "medium_decisiveness"
  | "low_decisiveness"
  | "uncontested_race"
  | "direct_vote_on_policy"
  | "missing_representation_data"
  | "missing_decisiveness_data";

export type VotePowerInput = {
  raceType: ElectionRaceType;
  candidateCount: number;
  representationPowerScore: number | null | undefined;
  competitivenessLabel: HistoricalContestCompetitivenessLabel | null | undefined;
};

export type VotePowerResult = {
  // User-facing label is authoritative. Score is a continuous sorting signal;
  // clients should not re-bucket it into their own display labels.
  score: number | null;
  label: VotePowerLabel;
  confidence: VotePowerConfidence;
  representation_level: VotePowerRepresentationLevel;
  decisiveness_level: VotePowerDecisivenessLevel;
  factors: VotePowerFactor[];
};

// Detail-page-only companion to VotePowerResult: ready-to-render copy so
// clients never re-derive rating language. Kept off the ballot list payload
// (dozens of elections per response) — only the election detail lookup
// attaches it.
export type VotePowerExplanation = {
  how: string;
  reasons: string[];
  caveat: string | null;
};

const LABELS: readonly Exclude<VotePowerLabel, "unknown">[] = ["very_low", "low", "medium", "high", "very_high"];

const DECISIVENESS_SCORE_BY_LABEL: Record<HistoricalContestCompetitivenessLabel, number> = {
  toss_up: 1,
  very_competitive: 0.85,
  competitive: 0.65,
  somewhat_competitive: 0.45,
  safe: 0.2,
};

function clampScore(value: number): number {
  return Math.min(100, Math.max(0, Math.round(value)));
}

function normalizeRepresentationPowerScore(value: number | null | undefined): number | null {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return null;
  }
  return Math.min(100, Math.max(0, value));
}

export function representationLevelFromScore(value: number | null | undefined): VotePowerRepresentationLevel {
  const score = normalizeRepresentationPowerScore(value);
  if (score === null) {
    return "unknown";
  }
  if (score >= 66) {
    return "high";
  }
  if (score >= 33) {
    return "medium";
  }
  return "low";
}

export function decisivenessLevelFromContest(input: {
  raceType: ElectionRaceType;
  candidateCount: number;
  competitivenessLabel: HistoricalContestCompetitivenessLabel | null | undefined;
}): VotePowerDecisivenessLevel {
  // Only exactly one known candidate is uncontested; zero can mean the roster has not loaded yet.
  if (input.raceType === "office" && input.candidateCount === 1) {
    return "none";
  }

  switch (input.competitivenessLabel) {
    case "toss_up":
    case "very_competitive":
      return "high";
    case "competitive":
    case "somewhat_competitive":
      return "medium";
    case "safe":
      return "low";
    case null:
    case undefined:
      return "unknown";
    default:
      return "unknown";
  }
}

function matrixLabel(
  representationLevel: Exclude<VotePowerRepresentationLevel, "unknown">,
  decisivenessLevel: Exclude<VotePowerDecisivenessLevel, "unknown">
): Exclude<VotePowerLabel, "unknown"> {
  if (representationLevel === "high") {
    switch (decisivenessLevel) {
      case "none":
        return "low";
      case "low":
        return "medium";
      case "medium":
        return "high";
      case "high":
        return "very_high";
    }
  }

  if (representationLevel === "medium") {
    switch (decisivenessLevel) {
      case "none":
      case "low":
        return "low";
      case "medium":
        return "medium";
      case "high":
        return "high";
    }
  }

  switch (decisivenessLevel) {
    case "none":
      return "very_low";
    case "low":
    case "medium":
      return "low";
    case "high":
      return "medium";
  }
}

function labelFromKnownAxis(input: {
  representationLevel: VotePowerRepresentationLevel;
  decisivenessLevel: VotePowerDecisivenessLevel;
}): VotePowerLabel {
  if (input.representationLevel !== "unknown" && input.decisivenessLevel !== "unknown") {
    return matrixLabel(input.representationLevel, input.decisivenessLevel);
  }

  if (input.representationLevel !== "unknown") {
    return input.representationLevel;
  }

  if (input.decisivenessLevel !== "unknown") {
    return input.decisivenessLevel === "none" ? "low" : input.decisivenessLevel;
  }

  return "unknown";
}

function bumpLabel(label: VotePowerLabel): VotePowerLabel {
  if (label === "unknown") {
    return "unknown";
  }
  const index = LABELS.indexOf(label);
  return LABELS[Math.min(index + 1, LABELS.length - 1)] ?? label;
}

function capLabel(label: VotePowerLabel, maxLabel: Exclude<VotePowerLabel, "unknown">): VotePowerLabel {
  if (label === "unknown") {
    return label;
  }
  const labelIndex = LABELS.indexOf(label);
  const maxIndex = LABELS.indexOf(maxLabel);
  return LABELS[Math.min(labelIndex, maxIndex)] ?? label;
}

function calculateScore(input: {
  raceType: ElectionRaceType;
  representationPowerScore: number | null;
  representationLevel: VotePowerRepresentationLevel;
  decisivenessLevel: VotePowerDecisivenessLevel;
  competitivenessLabel: HistoricalContestCompetitivenessLabel | null | undefined;
  hasMissingCoreAxis: boolean;
}): number | null {
  const representationPowerScore = input.representationPowerScore;
  const decisivenessLevel = input.decisivenessLevel;
  const hasRepresentation = input.representationLevel !== "unknown" && representationPowerScore !== null;
  const hasDecisiveness = decisivenessLevel !== "unknown";

  if (!hasRepresentation && !hasDecisiveness) {
    return null;
  }

  let raw: number;
  if (representationPowerScore !== null && input.representationLevel !== "unknown" && decisivenessLevel !== "unknown") {
    const representationNorm = representationPowerScore / 100;
    const decisivenessNorm =
      decisivenessLevel === "none" ? 0 : DECISIVENESS_SCORE_BY_LABEL[input.competitivenessLabel!];
    raw = 100 * (0.45 * representationNorm + 0.55 * decisivenessNorm);
  } else if (representationPowerScore !== null && input.representationLevel !== "unknown") {
    raw = representationPowerScore;
  } else if (decisivenessLevel !== "unknown") {
    raw = decisivenessLevel === "none" ? 0 : 100 * DECISIVENESS_SCORE_BY_LABEL[input.competitivenessLabel!];
  } else {
    return null;
  }

  if (decisivenessLevel === "none") {
    raw = Math.min(raw, 25);
  }
  if (input.raceType === "ballot_measure") {
    raw = Math.min(100, raw + 12);
  }
  if (input.hasMissingCoreAxis) {
    raw = Math.min(raw, 79);
  }

  return clampScore(raw);
}

function confidenceFor(input: {
  representationLevel: VotePowerRepresentationLevel;
  decisivenessLevel: VotePowerDecisivenessLevel;
  hasMissingCoreAxis: boolean;
}): VotePowerConfidence {
  if (!input.hasMissingCoreAxis) {
    return "high";
  }
  if (input.representationLevel !== "unknown" || input.decisivenessLevel !== "unknown") {
    return "medium";
  }
  return "low";
}

function factorsFor(input: {
  raceType: ElectionRaceType;
  representationLevel: VotePowerRepresentationLevel;
  decisivenessLevel: VotePowerDecisivenessLevel;
  omitMissingDecisiveness: boolean;
}): VotePowerFactor[] {
  const factors: VotePowerFactor[] = [];

  switch (input.representationLevel) {
    case "high":
      factors.push("high_representation");
      break;
    case "medium":
      factors.push("medium_representation");
      break;
    case "low":
      factors.push("low_representation");
      break;
    case "unknown":
      factors.push("missing_representation_data");
      break;
  }

  switch (input.decisivenessLevel) {
    case "none":
      factors.push("uncontested_race");
      break;
    case "high":
      factors.push("high_decisiveness");
      break;
    case "medium":
      factors.push("medium_decisiveness");
      break;
    case "low":
      factors.push("low_decisiveness");
      break;
    case "unknown":
      if (!input.omitMissingDecisiveness) {
        factors.push("missing_decisiveness_data");
      }
      break;
  }

  if (input.raceType === "ballot_measure") {
    factors.push("direct_vote_on_policy");
  }

  return factors;
}

// Describes the LABEL algorithm (the level matrix), not the numeric score:
// the 45/55 weighted score is a sorting signal only and never shown on the
// detail page, so surfacing its formula here would misattribute the rating.
const HOW_CALCULATED =
  "Vote power combines two measures: representation (how much weight one vote carries in this district — districts with smaller populations for their type score higher) and decisiveness (how likely this race is to be decided by a narrow margin, based on past results for this contest). Each measure is graded low, medium, or high, and the two grades together set the rating — the more of each, the higher it goes.";

const STALE_HISTORY_QUALIFIER =
  " District boundaries have changed since those results, so they may be a weaker guide.";

function explanationReasonFor(
  factor: VotePowerFactor,
  representationPowerScore: number | null,
  staleAfterRedistricting: boolean
): string {
  // Representation factors only occur with a known score, but fall back to
  // level-only phrasing rather than rendering "null out of 100". Floor, not
  // round: the level thresholds are the integers 33 and 66, so flooring can
  // never display a number that sits in a higher bucket than the unrounded
  // value it summarizes (65.6 must not render as the high-threshold 66).
  const scoreSuffix =
    representationPowerScore === null ? "" : ` (${Math.floor(representationPowerScore)} out of 100)`;
  // Only decisiveness grades rest on historical results; staleness says
  // nothing about representation, uncontested races, or missing data.
  const staleSuffix = staleAfterRedistricting ? STALE_HISTORY_QUALIFIER : "";

  switch (factor) {
    case "high_representation":
      return `Representation is high${scoreSuffix}: this district's population is small for its type, so each vote is a larger share of the outcome.`;
    case "medium_representation":
      return `Representation is medium${scoreSuffix}: this district's population is mid-range for its type.`;
    case "low_representation":
      return `Representation is low${scoreSuffix}: this district's population is large for its type, so each vote is a smaller share of the outcome.`;
    case "missing_representation_data":
      return "No representation score is available for this district yet.";
    case "uncontested_race":
      return "Only one candidate is on the ballot, so the outcome will not turn on vote margin.";
    case "high_decisiveness":
      return `Decisiveness is high: past results for this contest were very close, so a small number of votes could decide it.${staleSuffix}`;
    case "medium_decisiveness":
      return `Decisiveness is medium: past results for this contest were moderately competitive.${staleSuffix}`;
    case "low_decisiveness":
      return `Decisiveness is low: past results for this contest were decided by wide margins.${staleSuffix}`;
    case "missing_decisiveness_data":
      return "No past-results data is available for this contest yet.";
    case "direct_vote_on_policy":
      return "This is a ballot measure: your vote sets policy directly instead of electing a representative, which raises the rating one step.";
  }
}

function explanationCaveatFor(confidence: VotePowerConfidence): string | null {
  switch (confidence) {
    case "high":
      return null;
    case "medium":
      return 'Some underlying data is missing, so this rating is based on partial information and is capped at "high".';
    case "low":
      return "Not enough data is available to rate vote power for this election.";
  }
}

// Deterministic, backend-owned explanation of a computed rating. Reasons
// mirror result.factors one-to-one so the explanation can never drift from
// the rating logic that produced it. staleAfterRedistricting is explanation
// context only — the rating deliberately keeps ignoring it, so it lives
// outside VotePowerInput rather than implying calculateVotePower reads it.
export function explainVotePower(
  input: VotePowerInput & { staleAfterRedistricting?: boolean },
  result: VotePowerResult
): VotePowerExplanation {
  const representationPowerScore = normalizeRepresentationPowerScore(input.representationPowerScore);
  return {
    how: HOW_CALCULATED,
    reasons: result.factors.map((factor) =>
      explanationReasonFor(factor, representationPowerScore, input.staleAfterRedistricting === true)
    ),
    caveat: explanationCaveatFor(result.confidence),
  };
}

export function calculateVotePower(input: VotePowerInput): VotePowerResult {
  const representationPowerScore = normalizeRepresentationPowerScore(input.representationPowerScore);
  const representationLevel = representationLevelFromScore(representationPowerScore);
  const decisivenessLevel = decisivenessLevelFromContest(input);
  const ballotMeasureWithRepresentation =
    input.raceType === "ballot_measure" && representationLevel !== "unknown" && decisivenessLevel === "unknown";
  const missingCoreAxis =
    representationLevel === "unknown" || (decisivenessLevel === "unknown" && !ballotMeasureWithRepresentation);

  let label = labelFromKnownAxis({ representationLevel, decisivenessLevel });
  if (input.raceType === "ballot_measure") {
    label = bumpLabel(label);
  }
  if (missingCoreAxis) {
    label = capLabel(label, "high");
  }

  return {
    score: calculateScore({
      raceType: input.raceType,
      representationPowerScore,
      representationLevel,
      decisivenessLevel,
      competitivenessLabel: input.competitivenessLabel,
      hasMissingCoreAxis: missingCoreAxis,
    }),
    label,
    confidence: confidenceFor({ representationLevel, decisivenessLevel, hasMissingCoreAxis: missingCoreAxis }),
    representation_level: representationLevel,
    decisiveness_level: decisivenessLevel,
    factors: factorsFor({
      raceType: input.raceType,
      representationLevel,
      decisivenessLevel,
      omitMissingDecisiveness: ballotMeasureWithRepresentation,
    }),
  };
}
