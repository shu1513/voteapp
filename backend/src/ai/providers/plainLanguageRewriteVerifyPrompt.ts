import type { PlainLanguageRewriteKind } from "./plainLanguageRewritePrompt.js";

/**
 * Independent fact-consistency check for the Phase 2 plain-language backfill
 * (plan-content-wording.md). The verifier is never the rewriter judging
 * itself: it receives only the original and the rewrite, in a separate call,
 * and answers whether they state the same facts. Any doubt is a mismatch —
 * for civic data, a silently changed fact is worse than ugly wording, so a
 * mismatch keeps the original row and lands in a manual-review queue.
 */
export type PlainLanguageRewriteVerifyPromptInput = {
  kind: PlainLanguageRewriteKind;
  originalText: string;
  rewrittenText: string;
};

export function buildPlainLanguageRewriteVerifyPrompt(
  input: PlainLanguageRewriteVerifyPromptInput
): string {
  return [
    "You are verifying that a plain-language rewrite of election research text preserved the facts.",
    "You did not write the rewrite. Judge it strictly.",
    "Return strict JSON only.",
    "",
    "Original text:",
    JSON.stringify(input.originalText),
    "",
    "Rewritten text:",
    JSON.stringify(input.rewrittenText),
    "",
    "Return JSON with this exact shape:",
    "{",
    '  "verdict": "same_facts" or "mismatch",',
    '  "reason": "required when verdict is mismatch: the specific factual difference"',
    "}",
    "",
    "Rules:",
    "- Compare claim by claim: same people and organizations, same actions, same amounts and quantities, same dates, same outcomes, same direction (for/against, won/lost, yes/no), same attribution of who did what.",
    '- A lost negation, a flipped stance, a changed number, a changed name, or a new or dropped factual claim is a "mismatch".',
    ...(input.kind === "candidate_summary"
      ? [
          "- Exception: the rewrite is ALLOWED — and expected — to drop everything about the contest the candidate is currently seeking: the office sought (even when it is the seat they already hold), election year and date, stage (primary, runoff, general), candidacy or nominee status, unopposed status, vote percentages, primary results, and opponents. Dropping all of that, even when only a party or profession remains, is same_facts, not a mismatch.",
          '- "The current X" and "the incumbent X" state the same fact.',
          "- Judge only what remains in scope after that exception: current and past offices held, career history, and qualifications must be preserved.",
        ]
      : ["- The rewrite may not drop any claim; simplified wording is fine, missing content is not."]),
    '- Wording changes that keep the meaning (simpler words, shorter sentences, defined terms) are "same_facts".',
    '- When in doubt, answer "mismatch".',
    "- return JSON only (no prose, no markdown).",
  ].join("\n");
}
