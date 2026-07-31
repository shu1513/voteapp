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
  // Routine candidacy-machinery filings are candidacy facts, not governance
  // records: a periodic campaign-finance report, the ballot-qualifying fee,
  // and qualifying as a candidate are steps every filer takes to run — 38
  // Orange County FL candidates were made to look researched by "filed a P2
  // campaign-finance report" rows. The filing pattern is article-anchored
  // ("filed a/the/his ... report") so a participle noun phrase like
  // "A filed campaign-finance statement ... reported a contribution from a
  // sitting judge" — a real integrity record — does not match. Substantive
  // verbs are checked first, so legislation ABOUT fees, reports, or
  // "qualifying" programs (health plans, veterans) stays substantive, and
  // finance-filing MISCONDUCT ("concealed", "illegally reimbursed",
  // "fined") is rescued by the past-tense misconduct verbs in
  // SUBSTANTIVE_ACTION_PATTERNS. The qualifying-fee pattern is deliberately
  // verb-unanchored: fee waivers, payments, and disputes are candidacy
  // machinery unless an enforcement/misconduct verb makes them a record
  // (verified against the full live corpus — zero legitimate rows match).
  /\bfiled\s+(?:a|an|the|his|her|its|their)\s+(?:[\w-]+\s+){0,3}?campaign[-\s]finance\s+(?:report|statement|disclosure)s?\b/i,
  /\bqualifying\s+fees?\b/i,
  /\bqualified\s+as\s+an?\s+(?:[\w-]+\s+){0,3}?candidate\b/i,
] as const;

// Words that mark the FOLLOWING "promises/pledges/vows" as the noun rather
// than the verb, shared by the promise pattern and the masking pattern below
// so the two cannot drift apart. Determiners and possessives cover "his
// promises"; the verb and adjective entries cover the live corpus hits
// "should not MAKE pledges about cases" and "opposition to ... UNFUNDED
// promises", where the noun follows a verb or a modifier instead.
const PROMISSORY_NOUN_CONTEXT =
  "a|an|the|his|her|its|their|our|any|no|every|that|this|these|those|campaign|election|make|makes|made|making|break|breaks|broke|broken|keep|keeps|kept|unfunded|empty|vague|broken|written|verbal|public|such|more|fewer|other|similar|specific";

const PROMISSORY_VERB_LOOKBEHIND = `(?<!\\b(?:${PROMISSORY_NOUN_CONTEXT})\\s)`;

const FUTURE_PROMISE_PATTERNS = [
  /\b(?:campaign|platform|website)\b.*\b(?:promises?|pledges?|vows?|plans?|proposes?)\b/i,
  /\b(?:promises?|pledges?|vows?)\s+to\b/i,
  // A promise can take a NOUN OBJECT instead of an infinitive: "pledges a
  // people-powered campaign, rejection of special-interest influence, and
  // transparent policy agendas" is entirely prospective, yet it escaped every
  // pattern here — the adjacent rule needs "pledges TO", the rule above needs
  // campaign/platform/website to appear BEFORE the verb (here it trails it),
  // and the past-tense rule below does not match "pledges". It then matched
  // "profile" in FALLBACK_CONTEXT_PATTERNS and landed in the writable
  // neutral_context bucket (live: Heather-Marie Wilson).
  //
  // The object is left unconstrained ("pledges lower taxes", "vows reform",
  // "promises $1 million for schools") — an earlier determiner-anchored
  // version covered only "pledges A/THE/HIS ..." and let every bare noun
  // phrase through. Two guards replace that anchor:
  //   - Only the -s forms, which are the third-person VERB forms records use.
  //     Singular "a promise"/"the pledge" is the noun sense and is skipped.
  //   - A lookbehind for determiners, possessives and "campaign"/"election",
  //     so plural NOUN uses ("his promises", "campaign pledges") do not match.
  //     "The campaign promises transparency" is still caught by the
  //     campaign/platform rule above.
  // Completed outcomes ("kept a promise", "broke a pledge") are rescued ahead
  // of this by PROMISE_OUTCOME_PATTERNS.
  new RegExp(`${PROMISSORY_VERB_LOOKBEHIND}\\b(?:promises|pledges|vows)\\b`, "i"),
  // Past-tense promissory verbs are still promises, in any position:
  // "Promised as a judicial candidate to uphold ..." slipped past the
  // adjacent present-tense pattern above and became a canonical record.
  // Substantive completed-action verbs are matched first, so a description
  // that pairs a real action with its promise is still kept.
  /\b(?:promised|pledged|vowed)\b/i,
  /\b(?:says|said)\s+(?:he|she|they)\s+(?:will|would)\b/i,
  /\b(?:will|would)\s+(?:fight|work|cut|raise|support|oppose|create|expand|reduce|protect)\b/i,
] as const;

// Keeping, breaking or abandoning a promise is a COMPLETED action and one of
// the more voter-relevant records there is — "broke a pledge a month later" is
// an integrity record, not a campaign promise. These are matched against the
// RAW description before any promissory masking, because the masking below
// deliberately blanks noun-object promise clauses and would otherwise eat the
// very object that identifies this as an outcome.
// Object-anchored on purpose: bare "kept"/"broke" are far too common
// ("broke ground", "kept the seat") to treat as promise outcomes.
const PROMISE_OUTCOME_PATTERNS = [
  /\b(?:kept|broke|fulfilled|honored|honoured|violated|abandoned|reversed)\s+(?:(?:a|an|the|his|her|its|their|our|multiple|several|\d+)\s+)?(?:campaign\s+|signed\s+)?(?:promises?|pledges?|vows?|commitments?)\b/i,
  /\breneged\s+on\s+(?:(?:a|an|the|his|her|its|their|our)\s+)?(?:campaign\s+)?(?:promises?|pledges?|vows?|commitments?)\b/i,
] as const;

const SUBSTANTIVE_ACTION_PATTERNS = [
  /\b(?:voted|signed|vetoed|sponsored|co-sponsored|introduced|authored|passed|enacted)\b/i,
  /\b(?:issued|ordered|appointed|oversaw|implemented|managed|directed|founded|led|chaired)\b/i,
  /\b(?:served|serves|serving)\s+as\b/i,
  /\b(?:held|holds)\s+(?:public\s+)?office\b/i,
  /\b(?:was|were|is|are)\s+elected\s+to\b/i,
  /\b(?:ruled|sentenced|prosecuted|defended|settled|sued)\b/i,
  // Misconduct/enforcement verbs: campaign-finance wrongdoing is a real
  // integrity record even when its sentence also matches a candidacy-
  // machinery pattern ("filed a false campaign-finance report that
  // CONCEALED a contribution", "illegally REIMBURSED the qualifying fee").
  // Past-tense forms only — a tenseless adjective ("illegal", "false")
  // would rescue future promises like "will fight illegal dumping" out of
  // future_promise, which the past-tense anchoring of this whole list
  // deliberately prevents. "reimbursed" is additionally adverb-anchored:
  // bare "reimbursed" is routine campaign bookkeeping and must not rescue
  // machinery rows like "the campaign reimbursed the qualifying fee".
  /\b(?:fined|charged|indicted|convicted|sanctioned|censured|investigated|audited|falsified|concealed|misreported|omitted)\b/i,
  /\b(?:illegally|improperly|unlawfully)\s+reimbursed\b/i,
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

// A substantive verb inside a promissory infinitive complement is not a
// completed action: in "promised to veto any tax increase passed by the
// legislature", both "veto" and "passed" describe the promise's content.
// Blank the complement through the end of its clause (period/semicolon/comma)
// before the substantive check; the promise patterns still see the original
// text. A clause that mixes a promise with a real action in the same
// unpunctuated breath loses the action and rejects — the operator then leads
// with the completed action, which is the phrasing the contract wants anyway.
const PROMISSORY_INFINITIVE_PATTERN =
  /\b(?:promis(?:es?|ed|ing)|pledg(?:es?|ed|ing)|vow(?:s|ed|ing)?)\s+(?:\w+\s+){0,3}?to\s+[^.;,]*/gi;

// The same reasoning applies to a promise with a NOUN object: in "pledges a
// ban on contractors convicted of fraud" the contractors were convicted, not
// the candidate, and in "promises a commission led by an independent chair"
// nobody has led anything yet. Both classified as substantive purely because a
// completed-action verb appeared inside the promised thing.
//
// Only the -s verb forms and unambiguous -ed/-ing forms are masked, behind the
// same determiner lookbehind the promise patterns use, so the NOUN sense is
// left alone: "signed the taxpayer protection pledge" must keep its "signed".
const PROMISSORY_NOUN_OBJECT_PATTERN = new RegExp(
  `${PROMISSORY_VERB_LOOKBEHIND}\\b(?:promis(?:es|ed|ing)|pledg(?:es|ed|ing)|vow(?:s|ed|ing))\\s+(?!to\\b)[^.;,]*`,
  "gi"
);

function withoutPromissoryComplements(value: string): string {
  return value
    .replace(PROMISSORY_INFINITIVE_PATTERN, " ")
    .replace(PROMISSORY_NOUN_OBJECT_PATTERN, " ");
}

export function classifyCandidateRecordQuality(
  input: CandidateRecordQualityInput
): CandidateRecordQualityResult {
  const description = normalizeDescription(input.description);
  if (description.length === 0) {
    return { classification: "disallowed_thin", reason: "unclassified_context" };
  }

  // Checked against the RAW description, and ahead of the masking below: the
  // noun-object mask blanks the promise clause, which would swallow the very
  // "promise"/"pledge" object that makes this an outcome rather than a promise.
  if (matchesAny(description, PROMISE_OUTCOME_PATTERNS)) {
    return { classification: "substantive", reason: "actual_record_action" };
  }

  if (matchesAny(withoutPromissoryComplements(description), SUBSTANTIVE_ACTION_PATTERNS)) {
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
