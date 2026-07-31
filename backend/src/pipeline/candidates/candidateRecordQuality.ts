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

// "promises/pledges/vows" is a verb in "She pledges lower taxes" and a noun in
// "unfunded promises", "Wilson's promises", "a series of pledges", "should not
// make pledges". Earlier versions tried to enumerate the words that precede
// the NOUN sense; that list can never be complete, and every gap in it
// silently REJECTED a real record — the worst direction for this gate to fail.
//
// Inverted here: a promissory word counts as a verb only when a plausible
// SUBJECT precedes it — sentence start, a pronoun, one of the nouns that
// actually head these sentences, or a capitalized name. Anything unrecognized
// is treated as the noun sense, so an unfamiliar construction leaves the
// record alone instead of suppressing it.
//
// The capitalized-name branch deliberately excludes possessives by omitting
// the apostrophe from the character class: "Wilson's promises" cannot match,
// because a match would have to begin at the lowercase "s". Capitalized
// determiners and conjunctions are excluded explicitly, so "The promises of
// reform" stays a noun.
const SUBJECT_BEFORE_PROMISSORY =
  /(?:^|[.;:]\s+|\b(?:[Hh]e|[Ss]he|[Tt]hey|[Ii]t|[Ww]ho|[Ww]hich|profile|[Cc]andidate|platform|website|page|biography|statement|incumbent|challenger|nominee|[Gg]overnor|[Mm]ayor|[Ss]enator|[Rr]epresentative)\s+|\b(?!The\b|A\b|An\b|His\b|Her\b|Its\b|Their\b|Our\b|This\b|That\b|These\b|Those\b|Of\b|In\b|On\b|For\b|From\b|About\b|And\b|But\b|As\b|No\b|Any\b)[A-Z][a-zA-Z.-]*[a-zA-Z]\s+)$/;

// Case-insensitive so a sentence-initial "Promises a full audit ..." is seen;
// SUBJECT_BEFORE_PROMISSORY above stays case-SENSITIVE on purpose, since
// capitalization is the signal that distinguishes a name from a common noun.
const PROMISSORY_WORD_SCAN = /\b(?:promis(?:es|ed|ing)|pledg(?:es|ed|ing)|vow(?:s|ed|ing))\b/gi;

function isPromissoryVerbUse(text: string, matchIndex: number): boolean {
  return SUBJECT_BEFORE_PROMISSORY.test(text.slice(0, matchIndex));
}

function containsPromissoryVerbUse(value: string): boolean {
  PROMISSORY_WORD_SCAN.lastIndex = 0;
  let match: RegExpExecArray | null;
  while ((match = PROMISSORY_WORD_SCAN.exec(value)) !== null) {
    if (isPromissoryVerbUse(value, match.index)) {
      return true;
    }
  }
  return false;
}

const FUTURE_PROMISE_PATTERNS = [
  /\b(?:campaign|platform|website)\b.*\b(?:promises?|pledges?|vows?|plans?|proposes?)\b/i,
  // Past-tense promissory verbs are still promises, in any position:
  // "Promised as a judicial candidate to uphold ..." slipped past the
  // present-tense handling and became a canonical record. Substantive
  // completed-action verbs are matched first, so a description that pairs a
  // real action with its promise is still kept.
  /\b(?:promised|pledged|vowed)\b/i,
  /\b(?:says|said)\s+(?:he|she|they)\s+(?:will|would)\b/i,
  /\b(?:will|would)\s+(?:fight|work|cut|raise|support|oppose|create|expand|reduce|protect)\b/i,
] as const;

// Keeping, breaking or abandoning a promise is a COMPLETED action and one of
// the more voter-relevant records there is — "broke a pledge a month later" is
// an integrity record, not a campaign promise.
//
// Matched against the MASKED text, alongside the other substantive patterns.
// An earlier version ran these against the raw description first, which let a
// promise ABOUT someone else's broken pledge pass as a completed action:
// "promised to investigate whether the governor broke his campaign pledge"
// scored substantive on the governor's conduct. Masking removes the promise
// clause before these are consulted, so only an outcome the record itself
// asserts survives. The masking leaves these intact because "broke A pledge"
// is the noun sense and is never treated as a promissory verb use.
//
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
// The promised thing is frequently a LIST — "promises a ban on convicted
// contractors, a commission led by independent experts" — and stopping at the
// first comma left later items exposed, so "led" in the second item scored the
// whole prospective sentence as substantive.
//
// Continuation across a comma is allowed only while the next item still looks
// like part of the list (an optional and/or, then a determiner or a number).
// A new clause starts with a subject instead, which is what keeps the action
// in "As promised during the campaign, she sponsored the bill" intact — an
// earlier version consumed commas unconditionally and swallowed "sponsored".
const PROMISED_OBJECT_TAIL = /^[^.;,]*(?:,\s*(?:and\s+|or\s+)?(?:an?|the|\d)\b[^.;,]*)*/;

function withoutPromissoryComplements(value: string): string {
  const withoutInfinitives = value.replace(PROMISSORY_INFINITIVE_PATTERN, " ");

  let result = "";
  let cursor = 0;
  PROMISSORY_WORD_SCAN.lastIndex = 0;
  let match: RegExpExecArray | null;
  while ((match = PROMISSORY_WORD_SCAN.exec(withoutInfinitives)) !== null) {
    if (match.index < cursor || !isPromissoryVerbUse(withoutInfinitives, match.index)) {
      continue;
    }
    const afterWord = match.index + match[0].length;
    const rest = withoutInfinitives.slice(afterWord);
    if (/^\s+to\b/.test(rest)) {
      continue; // infinitive complements are handled above
    }
    const objectLength = rest.match(PROMISED_OBJECT_TAIL)?.[0].length ?? 0;
    result += `${withoutInfinitives.slice(cursor, match.index)} `;
    cursor = afterWord + objectLength;
    PROMISSORY_WORD_SCAN.lastIndex = cursor;
  }
  return result + withoutInfinitives.slice(cursor);
}

export function classifyCandidateRecordQuality(
  input: CandidateRecordQualityInput
): CandidateRecordQualityResult {
  const description = normalizeDescription(input.description);
  if (description.length === 0) {
    return { classification: "disallowed_thin", reason: "unclassified_context" };
  }

  // Both completed-action families are judged on the masked text, so a verb
  // sitting inside a promised thing cannot vouch for the record.
  const withoutPromises = withoutPromissoryComplements(description);
  if (
    matchesAny(withoutPromises, PROMISE_OUTCOME_PATTERNS) ||
    matchesAny(withoutPromises, SUBSTANTIVE_ACTION_PATTERNS)
  ) {
    return { classification: "substantive", reason: "actual_record_action" };
  }

  if (matchesAny(description, PURE_CANDIDACY_PATTERNS)) {
    return { classification: "disallowed_thin", reason: "pure_candidacy" };
  }

  // The verb-use scan replaces the old "pledges TO" and noun-object patterns,
  // which is what stops "should not make pledges to decide pending cases" —
  // a noun, and the opposite of a promise — from being rejected as one.
  if (matchesAny(description, FUTURE_PROMISE_PATTERNS) || containsPromissoryVerbUse(description)) {
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
