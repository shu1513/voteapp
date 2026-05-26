import { isUsSenateOfficeTitle } from "../../utils/senateOffice.js";
import type { CandidateResearchMode } from "../candidateResearchMode.js";

export type CandidateDuplicateOption = {
  roster_index: number;
  party?: string;
  is_incumbent?: boolean;
  sources: string[];
};

export type CandidateRosterDisambiguationPromptInput = {
  districtName: string;
  districtType: string;
  state: string;
  electionDate: string;
  officialBallotTitle: string;
  electionStage?: string | null;
  senateClass?: string | null;
  termEndYear?: string | null;
  researchMode: CandidateResearchMode;
  electionIsPartisan?: boolean | null;
  duplicateDisplayName: string;
  options: CandidateDuplicateOption[];
  seedUrls?: readonly string[];
  reviewFeedbackLines?: readonly string[];
};

export function buildCandidateRosterDisambiguationPrompt(
  input: CandidateRosterDisambiguationPromptInput
): string {
  const includeSenateContext = isUsSenateOfficeTitle(input.officialBallotTitle);
  const includeFecIds = input.researchMode !== "state_level";
  const seedUrls = input.seedUrls ?? [];
  const reviewFeedbackLines = input.reviewFeedbackLines ?? [];

  return [
    "You are resolving duplicate same-name candidates for one election roster.",
    "Return strict JSON only.",
    "",
    "Election context:",
    `- district_name: ${JSON.stringify(input.districtName)}`,
    `- district_type: ${JSON.stringify(input.districtType)}`,
    `- state: ${JSON.stringify(input.state)}`,
    `- election_date: ${JSON.stringify(input.electionDate)}`,
    `- official_ballot_title: ${JSON.stringify(input.officialBallotTitle)}`,
    `- research_mode: ${JSON.stringify(input.researchMode)}`,
    ...(includeSenateContext && input.electionStage
      ? [`- election_stage: ${JSON.stringify(input.electionStage)}`]
      : []),
    ...(includeSenateContext && input.senateClass
      ? [`- senate_class: ${JSON.stringify(input.senateClass)}`]
      : []),
    ...(includeSenateContext && input.termEndYear
      ? [`- term_end_year: ${JSON.stringify(input.termEndYear)}`]
      : []),
    `- election_is_partisan: ${input.electionIsPartisan === true ? "true" : input.electionIsPartisan === false ? "false" : "unknown"}`,
    `- duplicate_display_name: ${JSON.stringify(input.duplicateDisplayName)}`,
    "",
    "Candidate options (same display_name, each with roster_index):",
    ...input.options.flatMap((option) => [
      `- roster_index: ${option.roster_index}`,
      ...(option.party ? [`  party_hint: ${JSON.stringify(option.party)}`] : []),
      ...(option.is_incumbent !== undefined
        ? [`  is_incumbent_hint: ${option.is_incumbent ? "true" : "false"}`]
        : []),
      ...(option.sources.length > 0 ? [`  sources: ${JSON.stringify(option.sources)}`] : []),
    ]),
    "",
    "Return JSON with this exact shape:",
    "{",
    '  "people": [',
    "    {",
    '      "roster_index": 0,',
    '      "status": "clear | ambiguous | same_as_other",',
    '      "disambiguation_hint": "short hint that distinguishes this person in this election (required when status=clear)",',
    '      "same_as_roster_index": 0,',
    ...(includeFecIds ? ['      "fec_ids": ["required FEC candidate ID(s)"],'] : []),
    '      "sources": ["https://..."]',
    "    }",
    "  ]",
    "}",
    "",
    "Rules:",
    "- Return one people row per provided option (same roster_index set as input).",
    "- Set status=clear when this row is a distinct person you can confidently identify in this contest.",
    "- Set status=same_as_other when this row refers to the same person as another row in this response.",
    "- Set status=ambiguous when evidence is insufficient for that specific row.",
    "- If status=same_as_other, same_as_roster_index is required and must point to a row marked clear.",
    "- If status=same_as_other, do not include disambiguation_hint.",
    ...(includeFecIds
      ? ["- For this federal contest, fec_ids is required on every people row and must include one or more FEC candidate IDs."]
      : ["- Do not include fec_ids for this state-level contest."]),
    "- Every returned people row must include at least one supporting source URL.",
    "- disambiguation_hint is required when status=clear and should be concise and specific to this election context.",
    "- disambiguation_hint must be omitted when status=ambiguous or status=same_as_other.",
    "- WRONG: two rows marked same_as_other pointing to each other (no clear target).",
    "- WRONG: same_as_other pointing to a row marked ambiguous.",
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
