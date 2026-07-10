/**
 * Shared plain-language style rules for prompts that produce user-facing prose
 * (candidate summaries, ballot measure summaries, candidate record
 * descriptions). Every voter is the audience, so generated text must read at
 * roughly a 6th-grade level instead of the wire-service/legal register the
 * model defaults to.
 *
 * Exported as prompt lines (not a paragraph) to match the line-array idiom the
 * prompt builders use.
 */
export const PLAIN_LANGUAGE_STYLE_RULES: readonly string[] = [
  "- Write all reader-facing text for a 6th-grade reader: short sentences, everyday words, concise — combine related facts, never chop into single-fact baby sentences.",
  '- Prefer the plain word when it means exactly the same thing ("refused", not "rebuffed"); when a technical term carries specific meaning, keep it and define it in plain words in the same sentence ("general obligation bonds — money the state borrows and pays back over time").',
  '- Plain must stay precise: never swap a specific term for a vague one ("officials whose job is to check on the sheriff\'s department", never "officials who watch over things").',
  '- Keep who-does-what exact: never move a government action onto the reader (THE STATE borrows money — not "you agree to borrow money").',
  "- Keep numbers concrete; do not round away meaning.",
];
