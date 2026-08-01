/**
 * Shared plain-language style rules for prompts that produce user-facing prose
 * (candidate summaries, ballot measure summaries, candidate record
 * descriptions). Every voter is the audience, so generated text must read at
 * roughly a 6th-grade level instead of the wire-service/legal register the
 * model defaults to.
 *
 * Earlier versions of these rules still produced text real readers found hard:
 * multi-clause sentences, and legal terms kept with a bolted-on definition
 * ("quo warranto — a court case that..."). The rules below target those two
 * failure modes directly: a hard sentence cap, plain-meaning-first ordering,
 * and a read-aloud final check.
 *
 * Exported as prompt lines (not a paragraph) to match the line-array idiom the
 * prompt builders use.
 */
/**
 * Content rule for candidate-record descriptions, shared by the discovery and
 * source-repair prompts (the repair model may rewrite descriptions too, so
 * both paths must demand the same substance). Worded direction-neutral —
 * "role or action", not "what the candidate did" — because valid records
 * include actions taken toward the candidate (discipline, reversals,
 * endorsements received) where the candidate did nothing.
 */
export const RECORD_DESCRIPTION_SUBSTANCE_RULE =
  "- Keep descriptions neutral and factual, built on substance: say what happened, what it concerned, and the candidate's role or action — not just a procedural label (item/amendment numbers, vendor legal names) — and never add substance the source does not state. Keep vote tallies.";

export const PLAIN_LANGUAGE_STYLE_RULES: readonly string[] = [
  "- Write all reader-facing text for a 6th-grade reader: everyday words, one main idea per sentence, no sentence longer than about 20 words.",
  '- Say the plain meaning first, then the official term in parentheses only when the reader needs the exact term — it appears on their ballot, or it names a specific bill, case, or law they could look up ("a court case that tests whether an official can keep the office (called quo warranto)"; "voted for the state budget bill (HB 110)"); otherwise use the plain meaning alone and drop the term.',
  '- Plain must stay precise: never swap a specific fact for a vague one, keep who-does-what exact (THE STATE borrows money — never "you agree to borrow money"), and keep numbers concrete.',
  "- Final check: read each sentence as if explaining it to a neighbor who never follows politics; rewrite any sentence they would have to read twice.",
];
