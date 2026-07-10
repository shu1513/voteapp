import { PLAIN_LANGUAGE_STYLE_RULES } from "./promptWritingStyle.js";

/**
 * One-off Phase 2 backfill (plan-content-wording.md): rewrite existing
 * user-facing text to the plain-language style the Phase 1 prompts now demand
 * at generation time. The input text is authoritative — the model rewrites
 * wording only, never facts. A separate verifier prompt
 * (plainLanguageRewriteVerifyPrompt.ts) checks fact consistency afterward.
 */
export type PlainLanguageRewriteKind =
  | "candidate_summary"
  | "measure_summary"
  | "measure_what_yes_means"
  | "measure_what_no_means"
  | "record_description";

export type PlainLanguageRewritePromptInput = {
  kind: PlainLanguageRewriteKind;
  text: string;
  /** For candidate_summary only: the contest the app shows next to the text. */
  contestContext?: {
    officialBallotTitle: string;
    districtName: string;
    electionDate: string;
  };
};

const KIND_DESCRIPTIONS: Record<PlainLanguageRewriteKind, string> = {
  candidate_summary: "a short neutral bio summary of an election candidate",
  measure_summary: "a neutral summary of a ballot measure's real-world policy impact if enacted",
  measure_what_yes_means: "a neutral description of what a YES vote on a ballot measure means",
  measure_what_no_means: "a neutral description of what a NO vote on a ballot measure means",
  record_description: "a neutral factual description of one public record about an election candidate",
};

export function buildPlainLanguageRewritePrompt(input: PlainLanguageRewritePromptInput): string {
  return [
    `You are rewriting ${KIND_DESCRIPTIONS[input.kind]} into plain language.`,
    "The text was researched from reliable sources and is authoritative: rewrite the wording only, never the facts.",
    "Return strict JSON only.",
    "",
    ...(input.contestContext
      ? [
          "Contest the app displays directly next to this text:",
          `- official_ballot_title: ${JSON.stringify(input.contestContext.officialBallotTitle)}`,
          `- district_name: ${JSON.stringify(input.contestContext.districtName)}`,
          `- election_date: ${JSON.stringify(input.contestContext.electionDate)}`,
          "",
        ]
      : []),
    "Original text:",
    JSON.stringify(input.text),
    "",
    "Return JSON with this exact shape:",
    "{",
    '  "rewritten_text": "the rewritten text"',
    "}",
    "",
    "Rules:",
    "- Do not add, drop, soften, or strengthen any factual claim; keep every name, amount, date, and outcome exactly as stated.",
    "- Do not state anything the original does not state.",
    ...(input.kind === "candidate_summary"
      ? [
          "- Remove clauses that name the contest shown above (the office, election, election date, or stage the candidate is running for); the app already displays them next to this text.",
          '- Remove campaign-status and horse-race content: vote percentages, primary results, opponents, "running for...", "seeking re-election", "faces X in the runoff". Removing these clauses is the ONLY permitted content removal.',
          "- Do not mention the candidacy, the primary, the runoff, or the general election at all; describe only who the person is.",
          "- Offices the person currently holds or previously held are facts to keep — including the exact office name, place, and district, and that they are the incumbent. Only the contest being sought is removable, even when it is the same seat.",
          '- If nothing substantive remains after those removals, state the little that does remain (party, profession, current or past office) in one short sentence — for example "Jane Doe is a lawyer." Never re-add contest content to fill space.',
        ]
      : ["- Keep every sentence's content; only the wording changes."]),
    ...(input.kind === "measure_what_yes_means" || input.kind === "measure_what_no_means"
      ? [
          '- A vote approves or rejects a government action; the reader never performs it. Write "Voting yes approves the state borrowing money for...", never "you agree to borrow money".',
        ]
      : []),
    ...PLAIN_LANGUAGE_STYLE_RULES,
    "- return JSON only (no prose, no markdown).",
  ].join("\n");
}
