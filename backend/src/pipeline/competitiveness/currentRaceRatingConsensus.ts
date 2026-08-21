import {
  COMPETITIVENESS_LABELS,
  classifyHistoricalContestMargin,
  type CompetitivenessLabel,
} from "./competitivenessLabels.js";

export const CURRENT_RACE_RATING_SCHEMA_VERSION = "current_race_rating.v1" as const;

export const CURRENT_RACE_RATING_METHODS = ["outlet_consensus", "mayoral_rubric"] as const;
export type CurrentRaceRatingMethod = (typeof CURRENT_RACE_RATING_METHODS)[number];

export const CURRENT_RACE_RATING_EVIDENCE_STATUSES = ["rated", "none_found"] as const;
export type CurrentRaceRatingEvidenceStatus = (typeof CURRENT_RACE_RATING_EVIDENCE_STATUSES)[number];

export const CURRENT_RACE_RATING_CONFIDENCES = ["high", "medium", "low"] as const;
export type CurrentRaceRatingConfidence = (typeof CURRENT_RACE_RATING_CONFIDENCES)[number];

export const CURRENT_RACE_RATING_OUTLETS = ["inside_elections", "sabato"] as const;
export type CurrentRaceRatingOutlet = (typeof CURRENT_RACE_RATING_OUTLETS)[number];

export const CURRENT_RACE_RATING_FAVORED_SIDES = ["D", "R", "I", "none"] as const;
export type CurrentRaceRatingFavoredSide = (typeof CURRENT_RACE_RATING_FAVORED_SIDES)[number];

// The distance ladder: toss_up=0, tilt=2, lean=3, likely=4, solid=5. Value 1
// is intentionally unused — tilt is the closest non-toss-up step on the
// 9-point Inside Elections scale, and 7-point outlets have no tilt at all.
export const CURRENT_RACE_RATING_INTENSITIES = [0, 2, 3, 4, 5] as const;
export type CurrentRaceRatingIntensity = (typeof CURRENT_RACE_RATING_INTENSITIES)[number];

export type CurrentRaceRatingObservation = {
  outlet: CurrentRaceRatingOutlet;
  raw_rating: string;
  favored: CurrentRaceRatingFavoredSide;
  intensity: CurrentRaceRatingIntensity;
  as_of: string;
  url: string;
};

export type CurrentRaceRatingConsensus = {
  competitiveness_label: CompetitivenessLabel;
  confidence: Extract<CurrentRaceRatingConfidence, "high" | "medium">;
  mean_intensity: number;
};

function labelIndex(label: CompetitivenessLabel): number {
  return COMPETITIVENESS_LABELS.indexOf(label);
}

function binMeanIntensity(mean: number): CompetitivenessLabel {
  if (mean < 1) {
    return "toss_up";
  }
  if (mean < 2.5) {
    return "very_competitive";
  }
  if (mean < 3.5) {
    return "competitive";
  }
  if (mean < 4.5) {
    return "somewhat_competitive";
  }
  return "safe";
}

/**
 * Derives the consensus label from raw per-outlet observations. Pure and
 * deterministic — the research payload never carries a label or confidence;
 * both are outputs of this function.
 */
export function deriveConsensusLabel(
  observations: readonly CurrentRaceRatingObservation[]
): CurrentRaceRatingConsensus {
  if (observations.length === 0) {
    throw new Error("deriveConsensusLabel requires at least one observation");
  }

  const meanIntensity =
    observations.reduce((sum, observation) => sum + observation.intensity, 0) / observations.length;
  let label = binMeanIntensity(meanIntensity);

  // "safe" requires every outlet at Solid AND agreeing on the favored side;
  // anything less settles at somewhat_competitive.
  const partisanSides = new Set(
    observations.map((observation) => observation.favored).filter((side) => side !== "none")
  );
  const allSolidSameSide =
    observations.every((observation) => observation.intensity === 5) && partisanSides.size === 1;
  if (label === "safe" && !allSolidSameSide) {
    label = "somewhat_competitive";
  }

  // Outlets favoring opposite sides = genuinely contested race, whatever the
  // naive mean says (fixes "Safe D" + "Safe R" averaging to safe).
  const opposingSides = partisanSides.size > 1;
  if (opposingSides && labelIndex(label) > labelIndex("very_competitive")) {
    label = "very_competitive";
  }

  const confidence = opposingSides || observations.length < 2 ? "medium" : "high";

  return {
    competitiveness_label: label,
    confidence,
    mean_intensity: meanIntensity,
  };
}

/**
 * Mayoral rubric (v1.1): a numeric current-cycle margin — completed
 * first-round margin or averaged verified polls — mapped through the same
 * margin bins as historical contests. Structural signals never touch this.
 */
export function deriveMayoralLabelFromMargin(marginPercent: number): CompetitivenessLabel {
  return classifyHistoricalContestMargin(marginPercent);
}
