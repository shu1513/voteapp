export type CandidateRecordQualityClass =
  | "substantive"
  | "neutral_context"
  | "disallowed_thin";

export type CandidateRecordQualityReason =
  | "actual_record_action"
  | "fallback_context"
  | "pure_candidacy"
  | "future_promise"
  | "unclassified_context";

export type CandidateRecordQualityInput = {
  description: string;
  sourceUrl?: string | null;
};

export type CandidateRecordQualityResult = {
  classification: CandidateRecordQualityClass;
  reason: CandidateRecordQualityReason;
};

const PURE_CANDIDACY_PATTERNS = [
  /\b(?:is|was|are|were)\s+(?:a\s+)?candidate\s+for\b/i,
  /\b(?:running|ran)\s+for\b/i,
  /\bfiled\s+(?:paperwork\s+)?(?:to\s+run|as\s+a\s+candidate|for\s+office)\b/i,
  /\bqualified\s+for\s+(?:the\s+)?ballot\b/i,
  /\bappears?\s+on\s+(?:the\s+)?ballot\b/i,
  /\blist(?:s|ed)\s+(?:by|on|as)?\b.*\b(?:candidate|ballot|roster)\b/i,
  /\b(?:candidate|campaign)\s+(?:announcement|launch|filing)\b/i,
  /\bdeclared\s+(?:a\s+)?candidacy\b/i,
] as const;

const FUTURE_PROMISE_PATTERNS = [
  /\b(?:campaign|platform|website)\b.*\b(?:promises?|pledges?|vows?|plans?|proposes?)\b/i,
  /\b(?:promises?|pledges?|vows?)\s+to\b/i,
  // Past-tense promissory verbs are still promises, in any position:
  // "Promised as a judicial candidate to uphold ..." slipped past the
  // adjacent present-tense pattern above and became a canonical record.
  // Substantive completed-action verbs are matched first, so a description
  // that pairs a real action with its promise is still kept.
  /\b(?:promised|pledged|vowed)\b/i,
  /\b(?:says|said)\s+(?:he|she|they)\s+(?:will|would)\b/i,
  /\b(?:will|would)\s+(?:fight|work|cut|raise|support|oppose|create|expand|reduce|protect)\b/i,
] as const;

const SUBSTANTIVE_ACTION_PATTERNS = [
  /\b(?:voted|signed|vetoed|sponsored|co-sponsored|introduced|authored|passed|enacted)\b/i,
  /\b(?:issued|ordered|appointed|oversaw|implemented|managed|directed|founded|led|chaired)\b/i,
  /\b(?:served|serves|serving)\s+as\b/i,
  /\b(?:held|holds)\s+(?:public\s+)?office\b/i,
  /\b(?:was|were|is|are)\s+elected\s+to\b/i,
  /\b(?:ruled|sentenced|prosecuted|defended|settled|sued)\b/i,
  /\b(?:endorsed|received\s+an?\s+endorsement)\b/i,
  /\b(?:published|released)\s+(?:a\s+)?(?:report|study|audit|decision|opinion)\b/i,
] as const;

const FALLBACK_CONTEXT_PATTERNS = [
  /\b(?:biography|bio|profile)\b/i,
  /\b(?:graduated|earned\s+(?:a\s+)?degree|attended)\b/i,
  /\b(?:worked|works)\s+as\b/i,
  /\b(?:occupation|profession|professional\s+background)\b/i,
] as const;

function normalizeDescription(value: string): string {
  return value.trim().replace(/\s+/g, " ");
}

function matchesAny(value: string, patterns: readonly RegExp[]): boolean {
  return patterns.some((pattern) => pattern.test(value));
}

export function classifyCandidateRecordQuality(
  input: CandidateRecordQualityInput
): CandidateRecordQualityResult {
  const description = normalizeDescription(input.description);
  if (description.length === 0) {
    return { classification: "disallowed_thin", reason: "unclassified_context" };
  }

  if (matchesAny(description, SUBSTANTIVE_ACTION_PATTERNS)) {
    return { classification: "substantive", reason: "actual_record_action" };
  }

  if (matchesAny(description, PURE_CANDIDACY_PATTERNS)) {
    return { classification: "disallowed_thin", reason: "pure_candidacy" };
  }

  if (matchesAny(description, FUTURE_PROMISE_PATTERNS)) {
    return { classification: "disallowed_thin", reason: "future_promise" };
  }

  if (matchesAny(description, FALLBACK_CONTEXT_PATTERNS)) {
    return { classification: "neutral_context", reason: "fallback_context" };
  }

  return { classification: "neutral_context", reason: "unclassified_context" };
}

export function isDisallowedThinCandidateRecord(input: CandidateRecordQualityInput): boolean {
  return classifyCandidateRecordQuality(input).classification === "disallowed_thin";
}
