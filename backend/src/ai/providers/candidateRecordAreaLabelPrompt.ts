import { isUsSenateOfficeTitle } from "../../utils/senateOffice.js";

export type CandidateRecordAreaLabelPromptRecord = {
  description: string;
  sourceUrl: string;
  eventDate: string;
};

export type CandidateRecordAreaLabelPromptInput = {
  candidateDisplayName: string;
  districtName: string;
  districtType: string;
  state: string;
  electionDate: string;
  officialBallotTitle: string;
  electionStage?: string | null;
  senateClass?: string | null;
  termEndYear?: string | null;
  allowedResearchAreaSlugs: readonly string[];
  records: readonly CandidateRecordAreaLabelPromptRecord[];
  reviewFeedbackLines?: readonly string[];
};

export function buildCandidateRecordAreaLabelPrompt(input: CandidateRecordAreaLabelPromptInput): string {
  const includeSenateContext = isUsSenateOfficeTitle(input.officialBallotTitle);
  const reviewFeedbackLines = input.reviewFeedbackLines ?? [];

  return [
    "You are classifying candidate records into allowed research areas.",
    "Return strict JSON only.",
    "",
    "Candidate + election context:",
    `- candidate_display_name: "${input.candidateDisplayName}"`,
    `- district_name: "${input.districtName}"`,
    `- district_type: "${input.districtType}"`,
    `- state: "${input.state}"`,
    `- election_date: "${input.electionDate}"`,
    `- official_ballot_title: "${input.officialBallotTitle}"`,
    ...(input.electionStage ? [`- election_stage: "${input.electionStage}"`] : []),
    ...(includeSenateContext && input.senateClass ? [`- senate_class: "${input.senateClass}"`] : []),
    ...(includeSenateContext && input.termEndYear ? [`- term_end_year: "${input.termEndYear}"`] : []),
    "",
    `Allowed research area slugs for this candidate/election context (use only these): ${JSON.stringify(input.allowedResearchAreaSlugs)}`,
    "Special non-stance areas: use research_area_slug='general' when no specific allowed area applies; use research_area_slug='integrity_and_ethics' for documented criminal convictions, official ethics findings, sanctions, disciplinary actions, court judgments, enforcement actions, or verified public accountability records.",
    "",
    "Records to classify (record_index is required in output):",
    ...input.records.flatMap((record, index) => [
      `- record_index: ${index}`,
      `  description: ${JSON.stringify(record.description)}`,
      `  source_url: ${JSON.stringify(record.sourceUrl)}`,
      `  event_date: ${JSON.stringify(record.eventDate)}`,
    ]),
    "",
    "Return JSON with this exact shape:",
    "{",
    '  "labels": [',
    "    {",
    '      "record_index": 0,',
    '      "research_area_slug": "one allowed slug",',
    '      "stance": "for | against"',
    "    }",
    "  ]",
    "}",
    "",
    "Rules:",
    "- Every record_index in the input must appear in at least one output label row.",
    "- You may assign multiple area labels to the same record_index when relevant.",
    "- Use only slugs from the allowed list.",
    "- If no specific allowed area applies, use research_area_slug='general'.",
    "- When research_area_slug is 'general' or 'integrity_and_ethics', omit stance.",
    "- For all other research_area_slug values, stance is required and must be for|against.",
    "- Do not repeat the same (record_index, research_area_slug) pair.",
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
