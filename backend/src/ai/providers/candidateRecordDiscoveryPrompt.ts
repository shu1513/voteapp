import { isUsSenateOfficeTitle } from "../../utils/senateOffice.js";

export type CandidateRecordDiscoveryPromptInput = {
  candidateDisplayName: string;
  districtName: string;
  districtType: string;
  state: string;
  electionDate: string;
  officialBallotTitle: string;
  electionStage?: string | null;
  senateClass?: string | null;
  termEndYear?: string | null;
  sinceDate?: string | null;
  seedUrls?: readonly string[];
  reviewFeedbackLines?: readonly string[];
};

export function buildCandidateRecordDiscoveryPrompt(input: CandidateRecordDiscoveryPromptInput): string {
  const includeSenateContext = isUsSenateOfficeTitle(input.officialBallotTitle);
  const seedUrls = input.seedUrls ?? [];
  const reviewFeedbackLines = input.reviewFeedbackLines ?? [];

  return [
    "You are researching substantive public records about one election candidate.",
    "Focus on records that evaluate fitness/competence for this office and the candidate's background relevant to office duties.",
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
    ...(input.sinceDate ? [`- since_date: "${input.sinceDate}"`] : []),
    "",
    "Return JSON with this exact shape:",
    "{",
    '  "records": [',
    "    {",
    '      "title": "short record title",',
    '      "description": "neutral factual description of the record",',
    '      "source_url": "https://...",',
    '      "event_date": "YYYY-MM-DD"',
    "    }",
    "  ]",
    "}",
    "",
    "Rules:",
    "- Research reliable public records about this exact candidate that show concrete actions or accountability relevant to this office, such as votes, sponsored legislation, official decisions, public policy statements, budgets managed, committee work, finance records, legal/ethics scrutiny, prior government service, professional achievements or failures, and documented positions on key issues.",
    "- Include documented criminal convictions, official ethics findings, sanctions, disciplinary actions, court judgments, enforcement actions, or verified public accountability records when they exist. Do not include rumors or unverified accusations.",
    ...(input.sinceDate ? ['- Include only records with event_date >= since_date.'] : []),
    "- records may be an empty array if no reliable records are found.",
    "- Hard rule: no pure candidacy announcement/profile rows. Do not return records whose only substance is that the person is running, filed to run, launched a campaign, appears on a ballot, or is listed in a voter guide.",
    "- Each record must include source_url and event_date.",
    "- event_date must be YYYY-MM-DD.",
    "- Prefer the date the action/event occurred.",
    "- If the action/event date is unknown, use the source publication date.",
    "- If neither action/event date nor publication date is available, omit that record.",
    "- Use one row per concrete record; do not duplicate the same source/event.",
    "- Keep descriptions neutral and factual.",
    "- return JSON only (no prose, no markdown).",
    ...(seedUrls.length > 0
      ? [
          "",
          "Starting reference URLs (use these first, then expand research as needed):",
          ...seedUrls.map((url) => `- ${JSON.stringify(url)}`),
        ]
      : []),
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
