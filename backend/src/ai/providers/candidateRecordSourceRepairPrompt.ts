import { isUsSenateOfficeTitle } from "../../utils/senateOffice.js";

export type CandidateRecordSourceRepairPromptBadRecord = {
  badIndex: number;
  title: string;
  description: string;
  sourceUrl: string;
  sourceName: string;
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

export function buildCandidateRecordSourceRepairPrompt(
  input: CandidateRecordSourceRepairPromptInput
): string {
  const includeSenateContext = isUsSenateOfficeTitle(input.officialBallotTitle);
  const reviewFeedbackLines = input.reviewFeedbackLines ?? [];

  return [
    "You are repairing candidate-record source citations for records with failed URLs.",
    "Return strict JSON only.",
    "",
    "Candidate + election context:",
    `- candidate_display_name: \"${input.candidateDisplayName}\"`,
    `- district_name: \"${input.districtName}\"`,
    `- district_type: \"${input.districtType}\"`,
    `- state: \"${input.state}\"`,
    `- election_date: \"${input.electionDate}\"`,
    `- official_ballot_title: \"${input.officialBallotTitle}\"`,
    ...(includeSenateContext && input.electionStage ? [`- election_stage: \"${input.electionStage}\"`] : []),
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
      `  title: ${JSON.stringify(record.title)}`,
      `  description: ${JSON.stringify(record.description)}`,
      `  source_name: ${JSON.stringify(record.sourceName)}`,
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
    '      "source_url": "https://...",',
    '      "source_name": "publisher/source name"',
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
    "- Do not change claim content: title, description, and event_date are immutable and must stay as-is.",
    "- You may only replace citation fields (source_url and source_name).",
    "- You may return fewer than all bad_index values; unresolved items can be omitted or returned with no_replacement=true.",
    "- Never reuse any URL listed in blocked URLs.",
    "- source_url must be a valid public http(s) URL.",
    "- Do not invent sources; if no reliable source exists, return no_replacement=true.",
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
