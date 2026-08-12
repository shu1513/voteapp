// Question-log redaction (BEHAVIOR.md rule 11): emails, phone numbers,
// street-address patterns, and long digit runs are stripped BEFORE the
// question ever reaches an INSERT. Pure functions, unit-tested.

const EMAIL_RE = /[\w.+-]+@[\w-]+\.[\w.-]+/g;
// 7+ digit runs with optional separators/parens catch US phone formats
// without eating ordinary small numbers ("District 10", "Prop 39").
const PHONE_RE = /(?:[+(]{0,2}\d[\s().-]*){7,}\d/g;
// "123 Main St(reet)/Ave/Rd/Blvd/Dr/Ln/Way/Ct…" — the leading house number is
// what makes it an address instead of a place name.
const STREET_ADDRESS_RE =
  /\b\d{1,6}\s+(?:[A-Za-z0-9'.-]+\s+){0,4}(?:st(?:reet)?|ave(?:nue)?|r(?:oa)?d|blvd|boulevard|dr(?:ive)?|l(?:ane|n)|way|ct|court|pl(?:ace)?|ter(?:race)?|hwy|highway|pkwy|parkway|cir(?:cle)?)\b\.?/gi;
const LONG_DIGITS_RE = /\d{5,}/g;

export const REDACTED_TOKEN = "[redacted]";

export function redactQuestion(question: string): string {
  return question
    .replace(EMAIL_RE, REDACTED_TOKEN)
    .replace(STREET_ADDRESS_RE, REDACTED_TOKEN)
    .replace(PHONE_RE, REDACTED_TOKEN)
    .replace(LONG_DIGITS_RE, REDACTED_TOKEN);
}

/** Redacted + normalized form stored as chatbot.questions.question_norm and
 * aggregated by the report script: lowercase, collapsed whitespace, trailing
 * punctuation dropped so trivial variants count as one question. The 500-unit
 * cap is also a storage bound: question_norm is part of question_stats'
 * btree primary key, and 500 UTF-16 units is at most 2000 UTF-8 bytes —
 * safely under the ~2704-byte btree tuple limit. */
export function normalizeQuestion(question: string): string {
  return redactQuestion(question)
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[\s?!.]+$/g, "")
    .trim()
    .slice(0, 500);
}

/** Normalized form for the ANSWER-CACHE key: same trivial-variant collapsing,
 * but NO redaction and NO truncation — redaction would give "ballot in
 * 30303" and "ballot in 30305" the same key (one user would receive the
 * other question's cached answer), and the 500-unit cap would collide long
 * follow-up texts on their shared prefix. Safe because the key is a sha256
 * digest (limits.ts): the text itself is never stored. */
export function normalizeQuestionForCacheKey(question: string): string {
  return question
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[\s?!.]+$/g, "")
    .trim();
}
