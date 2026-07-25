import { isUsSenateOfficeTitle } from "../../utils/senateOffice.js";
import { PLAIN_LANGUAGE_STYLE_RULES } from "./promptWritingStyle.js";

export type CandidateRecordSourceRepairPromptBadRecord = {
  badIndex: number;
  description: string;
  sourceUrl: string;
  eventDate: string;
  failureReason: string;
};

export type CandidateRecordSourceRepairPromptInput = {
  candidateDisplayName: string;
  districtName: string;
  districtType: string;
  state: string;
  electionDate: string;
  officialBallotTitle: string;
  electionStage?: string | null;
  senateClass?: string | null;
  termEndYear?: string | null;
  blockedUrls: readonly string[];
  badRecords: readonly CandidateRecordSourceRepairPromptBadRecord[];
  reviewFeedbackLines?: readonly string[];
};

function shouldIncludeState(input: CandidateRecordSourceRepairPromptInput): boolean {
  return !(input.districtType === "presidential" && input.state.trim().toUpperCase() === "US");
}

export function buildCandidateRecordSourceRepairPrompt(
  input: CandidateRecordSourceRepairPromptInput
): string {
  const includeSenateContext = isUsSenateOfficeTitle(input.officialBallotTitle);
  const reviewFeedbackLines = input.reviewFeedbackLines ?? [];

  return [
    "You are repairing bad candidate-record rows (schema issues and/or bad citation URLs).",
    "Return strict JSON only.",
    "",
    "Candidate + election context:",
    `- candidate_display_name: \"${input.candidateDisplayName}\"`,
    `- district_name: \"${input.districtName}\"`,
    `- district_type: \"${input.districtType}\"`,
    ...(shouldIncludeState(input) ? [`- state: \"${input.state}\"`] : []),
    `- election_date: \"${input.electionDate}\"`,
    `- official_ballot_title: \"${input.officialBallotTitle}\"`,
    ...(input.electionStage ? [`- election_stage: \"${input.electionStage}\"`] : []),
    ...(includeSenateContext && input.senateClass ? [`- senate_class: \"${input.senateClass}\"`] : []),
    ...(includeSenateContext && input.termEndYear ? [`- term_end_year: \"${input.termEndYear}\"`] : []),
    "",
    "Blocked URLs (must never be reused):",
    ...(input.blockedUrls.length > 0
      ? input.blockedUrls.map((url) => `- ${JSON.stringify(url)}`)
      : ["- []"]),
    "",
    "Bad records to repair:",
    ...input.badRecords.flatMap((record) => [
      `- bad_index: ${record.badIndex}`,
      `  description: ${JSON.stringify(record.description)}`,
      `  source_url: ${JSON.stringify(record.sourceUrl)}`,
      `  event_date: ${JSON.stringify(record.eventDate)}`,
      `  failure_reason: ${JSON.stringify(record.failureReason)}`,
    ]),
    "",
    "Return JSON with this exact shape:",
    "{",
    '  "repairs": [',
    "    {",
    '      "bad_index": 0,',
    '      "description": "neutral factual description of the record",',
    '      "source_url": "https://...",',
    '      "event_date": "YYYY-MM-DD"',
    "    },",
    "    {",
    '      "bad_index": 1,',
    '      "no_replacement": true,',
    '      "reason": "short reason no reliable replacement was found"',
    "    }",
    "  ]",
    "}",
    "",
    "Rules:",
    "- Return corrected full rows for bad_index values you can confidently repair.",
    "- You may fix description, source_url, and event_date when needed.",
    "- You may return fewer than all bad_index values; unresolved items can be omitted or returned with no_replacement=true.",
    "- Never reuse any URL listed in blocked URLs.",
    "- source_url must be a valid public http(s) URL.",
    "- source_url must not be a social/UGC platform (Reddit, X/Twitter, Facebook, YouTube, Medium, Substack, personal blogs); for damaging claims cite an official/legal source or reputable news outlet.",
    "- event_date must be YYYY-MM-DD.",
    "- Do not invent sources; if no reliable source exists, return no_replacement=true.",
    ...PLAIN_LANGUAGE_STYLE_RULES,
    "- return JSON only (no prose, no markdown).",
    ...(reviewFeedbackLines.length > 0
      ? [
          "",
          "Previous feedback to fix:",
          ...reviewFeedbackLines.map((line, index) => `${index + 1}. ${line}`),
          "Fix only the issues above and keep valid content.",
        ]
      : []),
  ].join("\n");
}
