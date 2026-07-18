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
//
// parts render as a compact formula: one row per graded measure with this
// election's actual numbers (score, population, past margin), then `result`
// states how the grades combine into the displayed rating.
export type VotePowerExplanationPart = {
  title: string;
  grade: string;
  stat: string | null;
  detail: string;
  // The exact scoring formula with this election's real numbers plugged in,
  // plus the grade cutoffs. null when the measure has no numeric input
  // (unknown data, uncontested, the ballot-measure step).
  formula: string | null;
};

export type VotePowerExplanation = {
  how: string;
  parts: VotePowerExplanationPart[];
  result: string;
  caveat: string | null;
};

// Extra context that qualifies the explanation copy only — the rating math
// in calculateVotePower deliberately ignores all of it, so it lives outside
// VotePowerInput rather than implying the rating reads it.
export type VotePowerExplanationContext = VotePowerInput & {
  staleAfterRedistricting?: boolean;
  districtPopulation?: number | null;
  marginPercent?: number | null;
  // Every contest year behind marginPercent: one entry for a plain margin,
  // several when it is a weighted multi-year blend (the stat must not pin a
  // blended number on a single year).
  marginElectionYears?: number[] | null;
  // Population extremes of the district's comparison group (the same scope
  // the loader's representation-score SQL uses) plus a human name for the
  // group, so the formula can show the real numbers.
  representationScope?: {
    maxPopulation: number;
    minPopulation: number;
    description: string;
  } | null;
  // Per-contest inputs behind a weighted multi-year margin, so the formula
  // can show the actual blend arithmetic.
  marginContests?: { marginPercent: number; electionYear: number; weight: number }[] | null;
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
  "Vote power = representation (how much weight one vote carries here, the smaller the district's population, the higher the representation) + decisiveness (how likely this race is to be close, based on past results and number of candidates).";

function capitalize(text: string): string {
  return text.charAt(0).toUpperCase() + text.slice(1);
}

// Display word for an axis level or rating: "medium" ships as "average"
// (the user-facing scale speaks in average-relative terms; see the
// api-client's formatVotePowerLabel). Internal level keys stay "medium".
function levelDisplayWord(level: string): string {
  return level === "medium" ? "average" : level;
}

// 12 -> "12", 3.25 -> "3.25", 2.04 -> "2.04": two decimals at most — the
// SAME precision classifyHistoricalContestMargin grades on. Coarser display
// rounding would let a 2.04 margin render as "2" beside a "very competitive"
// grade whose own rule says ≤2 is a toss-up.
function formatMarginPoints(marginPercent: number): string {
  return `${Math.round(marginPercent * 100) / 100}`;
}

function formatCount(value: number): string {
  return value.toLocaleString("en-US");
}

// First-match thresholds, not ranges: "33–65 average" would leave a 65.6
// score in no bucket, and the grader itself works on >= comparisons.
const REPRESENTATION_GRADE_SCALE = "grades: 66+ high, 33+ average, otherwise low";

// The loader's log-scaled inverse-population model, spelled out with this
// district's real numbers (see recomputeRepresentationPowerScores in
// districtsLoader.ts — this string must describe that SQL faithfully,
// including its two-decimal rounding and its equal-extremes midpoint rule).
//
// The value after "=" is the STORED score the rating graded on, while the
// expression's inputs are the CURRENT extremes from a live subselect. The
// numeric equation therefore only renders after re-evaluating the model
// here and confirming it reproduces the stored score; any drift (population
// edits since the last scoring run, or SQL-numeric vs float rounding at a
// .005 boundary) degrades to the symbolic form, which shows the stored
// score without claiming the live inputs derive it.
function representationFormula(input: {
  representationPowerScore: number;
  districtPopulation: number | null;
  representationScope: { maxPopulation: number; minPopulation: number; description: string } | null;
}): string {
  const score = Math.round(input.representationPowerScore * 100) / 100;
  if (input.districtPopulation !== null && input.representationScope !== null) {
    const scope = input.representationScope;
    if (scope.maxPopulation === scope.minPopulation) {
      // ln(x ÷ x) ÷ ln(x ÷ x) is 0/0 — the SQL never evaluates it and
      // assigns the midpoint instead. Equal extremes can also mean several
      // districts with identical populations, so don't claim "only one".
      if (score === 50) {
        return `score = 50 by rule: ${scope.description} currently all have the same population, so the model assigns the midpoint of 50 (${REPRESENTATION_GRADE_SCALE})`;
      }
    } else {
      const recomputed =
        Math.round(
          Math.min(
            100,
            Math.max(
              0,
              (100 * (Math.log(scope.maxPopulation) - Math.log(input.districtPopulation))) /
                (Math.log(scope.maxPopulation) - Math.log(scope.minPopulation))
            )
          ) * 100
        ) / 100;
      if (recomputed === score) {
        return `score = 100 × ln(largest population ÷ this district's) ÷ ln(largest ÷ smallest), rounded to 2 decimals = 100 × ln(${formatCount(scope.maxPopulation)} ÷ ${formatCount(input.districtPopulation)}) ÷ ln(${formatCount(scope.maxPopulation)} ÷ ${formatCount(scope.minPopulation)}) = ${score}, comparing ${scope.description} (${REPRESENTATION_GRADE_SCALE})`;
      }
    }
  }
  return `score = 100 × ln(largest population ÷ this district's) ÷ ln(largest ÷ smallest population among comparable districts), rounded to 2 decimals = ${score} (${REPRESENTATION_GRADE_SCALE})`;
}

function representationPart(input: {
  representationLevel: VotePowerRepresentationLevel;
  representationPowerScore: number | null;
  districtPopulation: number | null;
  representationScope: { maxPopulation: number; minPopulation: number; description: string } | null;
}): VotePowerExplanationPart {
  if (input.representationLevel === "unknown" || input.representationPowerScore === null) {
    return {
      title: "Representation",
      grade: "Unknown",
      stat: null,
      detail: "We don't have a representation score for this district yet.",
      formula: null,
    };
  }

  const detailByLevel: Record<Exclude<VotePowerRepresentationLevel, "unknown">, string> = {
    high: "Smaller districts give each vote more weight, and this district is small for its type.",
    medium: "This district is mid-sized for its type, so each vote carries average weight.",
    low: "This district is large for its type, so each vote is a smaller slice of the outcome.",
  };
  const populationSuffix =
    input.districtPopulation === null ? "" : ` About ${input.districtPopulation.toLocaleString("en-US")} people live here.`;

  return {
    title: "Representation",
    grade: capitalize(levelDisplayWord(input.representationLevel)),
    // Floor, not round: the grade thresholds are the integers 33 and 66, so
    // flooring can never display a number that sits in a higher bucket than
    // the unrounded value (65.6 must not render as the high-threshold 66).
    stat: `${Math.floor(input.representationPowerScore)} out of 100`,
    detail: `${detailByLevel[input.representationLevel]}${populationSuffix}`,
    formula: representationFormula({
      representationPowerScore: input.representationPowerScore,
      districtPopulation: input.districtPopulation,
      representationScope: input.representationScope,
    }),
  };
}

// First-match thresholds so boundary margins read unambiguously: a 2.04
// margin is "not ≤2, so ≤5 → very competitive", never inside a "0–2" range.
const MARGIN_GRADE_SCALE =
  "margins, first match: ≤2 toss-up, ≤5 very competitive, ≤10 competitive, ≤15 somewhat competitive, otherwise safe; toss-up and very competitive grade high, competitive and somewhat competitive grade average, safe grades low";

// The margin-to-grade pipeline with this contest's real numbers (see
// classifyHistoricalContestMargin — this string must match its cutoffs).
function decisivenessFormula(input: {
  decisivenessLevel: "low" | "medium" | "high";
  competitivenessLabel: HistoricalContestCompetitivenessLabel | null | undefined;
  marginPercent: number | null;
  marginContests: { marginPercent: number; electionYear: number; weight: number }[] | null;
}): string | null {
  if (input.marginPercent === null || input.competitivenessLabel == null) {
    return null;
  }
  const labelText = input.competitivenessLabel === "toss_up" ? "toss-up" : input.competitivenessLabel.replace(/_/g, " ");
  const contests = input.marginContests ?? [];
  const marginExpression =
    contests.length > 1
      ? `margin = ${contests
          .map((contest) => `${formatWeight(contest.weight)} × ${formatMarginPoints(contest.marginPercent)} (${contest.electionYear})`)
          .join(" + ")} = ${formatMarginPoints(input.marginPercent)} points`
      : `margin = ${formatMarginPoints(input.marginPercent)} points`;
  return `${marginExpression} → "${labelText}" → grade ${levelDisplayWord(input.decisivenessLevel)} (${MARGIN_GRADE_SCALE})`;
}

// 0.6 -> "0.6", 0.625 -> "0.625": up to four decimals, because rounding a
// real 0.625/0.375 pair to 0.63/0.38 would display arithmetic whose product
// sum no longer equals the blended total on the right of the "=".
function formatWeight(weight: number): string {
  return `${Math.round(weight * 10000) / 10000}`;
}

function decisivenessPart(input: {
  decisivenessLevel: VotePowerDecisivenessLevel;
  competitivenessLabel: HistoricalContestCompetitivenessLabel | null | undefined;
  marginPercent: number | null;
  marginElectionYears: number[] | null;
  marginContests: { marginPercent: number; electionYear: number; weight: number }[] | null;
  staleAfterRedistricting: boolean;
}): VotePowerExplanationPart {
  if (input.decisivenessLevel === "none") {
    return {
      title: "Decisiveness",
      grade: "None",
      stat: "only 1 candidate",
      detail: "One candidate is running unopposed, so votes can't change the outcome.",
      formula: null,
    };
  }
  if (input.decisivenessLevel === "unknown") {
    return {
      title: "Decisiveness",
      grade: "Unknown",
      stat: null,
      detail: "No past results for this contest yet.",
      formula: null,
    };
  }

  const detailByLevel: Record<"low" | "medium" | "high", string> = {
    high: "Past results here were very close — a small number of votes could decide the winner.",
    medium: "Past results here were somewhat close.",
    low: "Past results here were one-sided.",
  };
  // Only these grades rest on historical results, so only they carry the
  // redistricting qualifier; staleness says nothing about representation,
  // uncontested races, or missing data.
  const staleSuffix = input.staleAfterRedistricting
    ? " District lines have changed since then, so older results are a weaker guide."
    : "";
  return {
    title: "Decisiveness",
    grade: capitalize(levelDisplayWord(input.decisivenessLevel)),
    stat: marginStat(input.marginPercent, input.marginElectionYears),
    detail: `${detailByLevel[input.decisivenessLevel]}${staleSuffix}`,
    formula: decisivenessFormula({
      decisivenessLevel: input.decisivenessLevel,
      competitivenessLabel: input.competitivenessLabel,
      marginPercent: input.marginPercent,
      marginContests: input.marginContests,
    }),
  };
}

// "12 and 2022" reads as a typo; spell the blend out. Years arrive in the
// lookup's order (latest first) and render as given.
function formatYearList(years: number[]): string {
  if (years.length === 1) {
    return `${years[0]}`;
  }
  if (years.length === 2) {
    return `${years[0]} and ${years[1]}`;
  }
  return `${years.slice(0, -1).join(", ")}, and ${years[years.length - 1]}`;
}

// A multi-year margin is a weighted blend; pinning it on the single latest
// year would claim a number that election never produced.
function marginStat(marginPercent: number | null, marginElectionYears: number[] | null): string | null {
  if (marginPercent === null) {
    return null;
  }
  const points = `${formatMarginPoints(marginPercent)}-point`;
  const years = marginElectionYears ?? [];
  if (years.length === 0) {
    return `${points} margin`;
  }
  if (years.length === 1) {
    return `${points} margin in ${years[0]}`;
  }
  return `${points} weighted margin across ${formatYearList(years)}`;
}

// The bump can no-op (already at very_high, or eaten by the missing-data
// cap), so the copy must not promise a boost the label never received.
function ballotMeasurePart(boostApplied: boolean): VotePowerExplanationPart {
  if (boostApplied) {
    return {
      title: "Ballot measure",
      grade: "+1 step",
      stat: null,
      detail: "Your vote sets the policy directly, so the rating gets a one-step boost.",
      formula: null,
    };
  }
  return {
    title: "Ballot measure",
    grade: "Direct vote",
    stat: null,
    detail: "Your vote sets the policy directly, but it did not raise this rating further.",
    formula: null,
  };
}

function explanationResultFor(result: VotePowerResult, boostApplied: boolean, skipDecisiveness: boolean): string {
  if (result.label === "unknown") {
    return "Not enough data → no rating yet.";
  }

  const pieces: string[] = [
    result.representation_level === "unknown"
      ? "unknown representation"
      : `${levelDisplayWord(result.representation_level)} representation`,
  ];
  if (!skipDecisiveness) {
    if (result.decisiveness_level === "none") {
      pieces.push("an uncontested race");
    } else if (result.decisiveness_level === "unknown") {
      pieces.push("unknown decisiveness");
    } else {
      pieces.push(`${levelDisplayWord(result.decisiveness_level)} decisiveness`);
    }
  }
  if (boostApplied) {
    pieces.push("a ballot-measure boost");
  }

  return `${capitalize(pieces.join(" + "))} → ${capitalize(RESULT_LABEL_TEXT[result.label])} vote power.`;
}

// Display words for the rating in the result line. "low" reads as a verdict
// on the voter and "medium" as a size word, so they ship as "below average"
// and "average" (mirrors the api-client's formatVotePowerLabel chip copy).
const RESULT_LABEL_TEXT: Record<Exclude<VotePowerLabel, "unknown">, string> = {
  very_low: "very low",
  low: "below average",
  medium: "average",
  high: "high",
  very_high: "very high",
};

function explanationCaveatFor(confidence: VotePowerConfidence): string | null {
  switch (confidence) {
    case "high":
      return null;
    case "medium":
      return 'Some data is missing, so this rating is based on partial information and capped at "High".';
    case "low":
      return "Not enough data to rate this election yet.";
  }
}

// Deterministic, backend-owned explanation of a computed rating. Parts derive
// from the same levels the rating used, so the explanation can never drift
// from the rating logic that produced it.
export function explainVotePower(input: VotePowerExplanationContext, result: VotePowerResult): VotePowerExplanation {
  const isBallotMeasure = input.raceType === "ballot_measure";
  // Measures with a known representation grade rate fine without history
  // (mirrors factorsFor's omitMissingDecisiveness): a "no past results"
  // row would read as a data gap the rating doesn't actually suffer from.
  const skipDecisiveness =
    isBallotMeasure && result.decisiveness_level === "unknown" && result.representation_level !== "unknown";

  const parts: VotePowerExplanationPart[] = [
    representationPart({
      representationLevel: result.representation_level,
      representationPowerScore: normalizeRepresentationPowerScore(input.representationPowerScore),
      districtPopulation: input.districtPopulation ?? null,
      representationScope: input.representationScope ?? null,
    }),
  ];
  if (!skipDecisiveness) {
    parts.push(
      decisivenessPart({
        decisivenessLevel: result.decisiveness_level,
        competitivenessLabel: input.competitivenessLabel,
        marginPercent: input.marginPercent ?? null,
        marginElectionYears: input.marginElectionYears ?? null,
        marginContests: input.marginContests ?? null,
        staleAfterRedistricting: input.staleAfterRedistricting === true,
      })
    );
  }

  // Re-run the label pipeline without the measure bump: the boost is only
  // claimable when it actually moved the displayed label (bumpLabel tops out
  // at very_high, and the missing-data cap runs after the bump).
  let boostApplied = false;
  if (isBallotMeasure && result.label !== "unknown") {
    const missingCoreAxis =
      result.representation_level === "unknown" ||
      (result.decisiveness_level === "unknown" && !skipDecisiveness);
    let noBoostLabel = labelFromKnownAxis({
      representationLevel: result.representation_level,
      decisivenessLevel: result.decisiveness_level,
    });
    if (missingCoreAxis) {
      noBoostLabel = capLabel(noBoostLabel, "high");
    }
    boostApplied = result.label !== noBoostLabel;
  }
  if (isBallotMeasure) {
    parts.push(ballotMeasurePart(boostApplied));
  }

  return {
    how: HOW_CALCULATED,
    parts,
    result: explanationResultFor(result, boostApplied, skipDecisiveness),
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
