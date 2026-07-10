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
    ...(input.kind === "candidate_summary"
      ? [
          "",
          "IMPORTANT CONTEXT: this rewrite was deliberately instructed to DELETE everything about the contest the candidate is currently seeking — the office sought (even the seat they already hold), election dates, stage (primary, runoff, general), candidacy or nominee status, unopposed status, vote percentages, primary results, and opponents, including who they lost to or are challenging. Those deletions are correct by design; never report a missing contest detail as a mismatch, even when only a party or profession remains. Judge ONLY the claims that remain in scope: current and past offices held, career history, and qualifications.",
        ]
      : []),
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
          "- Apply the deletion context above: dropped contest content is never a mismatch. Preserved claims must stay exact.",
          '- "The current X" and "the incumbent X" state the same fact.',
        ]
      : ["- The rewrite may not drop any claim; simplified wording is fine, missing content is not."]),
    '- Wording changes that keep the meaning (simpler words, shorter sentences, defined terms) are "same_facts". A synonym for the same role or action is the same fact: "directs the clinic" -> "runs the clinic", "served as" -> "was". Only a verb that changes what the person actually did (for example "tried cases" -> "led cases") is a mismatch.',
    '- Replacing a technical term with its plain-language meaning is "same_facts", not a dropped fact: "general obligation bonds" -> "borrow money", "a century" -> "100 years", "subpoena" -> "a court order to appear". Flag it only when the plain phrase says something different or vaguer than the term.',
    '- Watch who-does-what: a rewrite that moves an action to a different actor is a "mismatch" — including moving a government action onto the reader ("the state borrows" -> "you agree to borrow money").',
    '- When in doubt, answer "mismatch".',
    "- return JSON only (no prose, no markdown).",
  ].join("\n");
}
