import { isUsSenateOfficeTitle } from "../../utils/senateOffice.js";
import type { ElectionContestFamily } from "../../types/election.js";

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
  discoveryContestFamily?: ElectionContestFamily | null;
  sinceDate?: string | null;
  existingRecordsToAvoid?: readonly CandidateRecordDiscoveryExistingRecord[];
  reviewFeedbackLines?: readonly string[];
};

export type CandidateRecordDiscoveryExistingRecord = {
  description: string;
  sourceUrl: string;
  eventDate: string;
};

function formatExistingRecordsToAvoid(
  records: readonly CandidateRecordDiscoveryExistingRecord[]
): string[] {
  if (records.length === 0) {
    return [];
  }

  return [
    "",
    "Existing candidate records already stored; do not return substantively duplicate records:",
    ...records.map((record, index) =>
      [
        `${index + 1}.`,
        `description: ${JSON.stringify(record.description)}`,
        `source_url: ${JSON.stringify(record.sourceUrl)}`,
        `event_date: ${JSON.stringify(record.eventDate)}`,
      ].join(" ")
    ),
  ];
}

export function buildCandidateRecordDiscoveryPrompt(input: CandidateRecordDiscoveryPromptInput): string {
  const includeSenateContext = isUsSenateOfficeTitle(input.officialBallotTitle);
  const useJudicialRecordObjective = input.discoveryContestFamily === "judicial_office";
  const reviewFeedbackLines = input.reviewFeedbackLines ?? [];
  const existingRecordsToAvoid = input.existingRecordsToAvoid ?? [];
  const objectiveRule = useJudicialRecordObjective
    ? "- Research the web for publicly available reliable records about this exact judicial candidate, focusing on evidence for evaluating the candidate's legal competence, integrity and ethics, impartiality, and professional record. Relevant sources may include notable cases, rulings, prior prosecutorial, defense, or judicial service, published legal work, disciplinary records, ethics complaints, controversies, and other verified public records."
    : "- Research reliable public records about this exact candidate that show concrete actions or accountability such as votes, sponsored legislation, official decisions, public policy statements, budgets managed, committee work, finance records, legal/ethics scrutiny/documented criminal convictions, prior government service, professional achievements or failures, and documented positions on key issues.";

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
    ...(input.discoveryContestFamily ? [`- discovery_contest_family: "${input.discoveryContestFamily}"`] : []),
    ...(input.electionStage ? [`- election_stage: "${input.electionStage}"`] : []),
    ...(includeSenateContext && input.senateClass ? [`- senate_class: "${input.senateClass}"`] : []),
    ...(includeSenateContext && input.termEndYear ? [`- term_end_year: "${input.termEndYear}"`] : []),
    ...(input.sinceDate ? [`- since_date: "${input.sinceDate}"`] : []),
    ...formatExistingRecordsToAvoid(existingRecordsToAvoid),
    "",
    "Return JSON with this exact shape:",
    "{",
    '  "records": [',
    "    {",
    '      "description": "neutral factual description of the record",',
    '      "source_url": "https://...",',
    '      "event_date": "YYYY-MM-DD"',
    "    }",
    "  ]",
    "}",
    "",
    "Rules:",
    objectiveRule,
    ...(useJudicialRecordObjective
      ? [
          "- Describe what the candidate actually did in the case and its effects/impacts.",
        ]
      : []),
    ...(input.sinceDate ? ['- Include only records with event_date >= since_date.'] : []),
    "- records may be an empty array if no reliable records are found.",
    "- Do not include pure candidacy announcements, such as records whose only substance is that the person is running, filed to run, launched a campaign, appears on a ballot, or is listed in a voter guide.",
    ...(existingRecordsToAvoid.length > 0
      ? [
          "- Do not return records that describe the same substantive action/event as any Existing candidate record, even if a different article words it differently.",
        ]
      : []),
    "- Each record must include source_url and event_date.",
    "- event_date must be YYYY-MM-DD; use the action/event date when known, otherwise use the source publication date.",
    "- If neither action/event date nor publication date is available, omit that record.",
    "- Use one row per concrete record; do not duplicate the same source/event.",
    "- Keep descriptions neutral and factual.",
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
