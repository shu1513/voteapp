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
  "- Write all reader-facing text for a 6th-grade reader: short sentences, everyday words.",
  '- Prefer the plain phrase over the technical one (say "borrow money", not "issue general obligation bonds"; say "refused", not "rebuffed").',
  '- When a technical term is unavoidable, define it in plain words in the same sentence (for example: "bonds — money the state borrows and pays back over time").',
  "- Keep numbers concrete; do not round away meaning.",
];
