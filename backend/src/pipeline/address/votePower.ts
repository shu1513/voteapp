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
      decisivenessLevel === "none" ? 0 : DECISIVENESS_SCORE_BY_LABEL[input.competitivenessLabel ?? "safe"];
    raw = 100 * (0.45 * representationNorm + 0.55 * decisivenessNorm);
  } else if (representationPowerScore !== null && input.representationLevel !== "unknown") {
    raw = representationPowerScore;
  } else if (decisivenessLevel !== "unknown") {
    raw =
      decisivenessLevel === "none" ? 0 : 100 * DECISIVENESS_SCORE_BY_LABEL[input.competitivenessLabel ?? "safe"];
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
