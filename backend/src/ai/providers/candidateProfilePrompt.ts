import { isUsSenateOfficeTitle } from "../../utils/senateOffice.js";
import type { CandidateResearchMode } from "../candidateResearchMode.js";
import { PLAIN_LANGUAGE_STYLE_RULES } from "./promptWritingStyle.js";

export type CandidateProfilePromptInput = {
  candidateDisplayName: string;
  districtName: string;
  districtType: string;
  state: string;
  electionDate?: string | null;
  officialBallotTitle: string;
  electionStage?: string | null;
  senateClass?: string | null;
  termEndYear?: string | null;
  researchMode: CandidateResearchMode;
  rosterParty?: string;
  rosterIncumbent?: boolean;
  rosterFecIds?: readonly string[];
  rosterStateFilingIds?: readonly string[];
  disambiguationHint?: string;
  seedUrls?: readonly string[];
  reviewFeedbackLines?: readonly string[];
};

export function buildCandidateProfilePrompt(input: CandidateProfilePromptInput): string {
  const includeSenateContext = isUsSenateOfficeTitle(input.officialBallotTitle);
  const includeFecIds = input.researchMode !== "state_level";
  const seedUrls = input.seedUrls ?? [];
  const reviewFeedbackLines = input.reviewFeedbackLines ?? [];

  return [
    "You are researching one election candidate profile for identity matching and basic display fields.",
    "Return strict JSON only.",
    "",
    "Candidate + election context:",
    `- candidate_display_name: "${input.candidateDisplayName}"`,
    `- district_name: "${input.districtName}"`,
    `- district_type: "${input.districtType}"`,
    `- state: "${input.state}"`,
    ...(input.electionDate ? [`- election_date: "${input.electionDate}"`] : []),
    `- official_ballot_title: "${input.officialBallotTitle}"`,
    `- research_mode: "${input.researchMode}"`,
    ...(includeSenateContext && input.electionStage ? [`- election_stage: "${input.electionStage}"`] : []),
    ...(includeSenateContext && input.senateClass ? [`- senate_class: "${input.senateClass}"`] : []),
    ...(includeSenateContext && input.termEndYear ? [`- term_end_year: "${input.termEndYear}"`] : []),
    ...(input.rosterIncumbent !== undefined
      ? [`- is_incumbent: ${input.rosterIncumbent ? "true" : "false"}`]
      : []),
    ...(input.disambiguationHint
      ? [`- roster_disambiguation_hint: ${JSON.stringify(input.disambiguationHint)}`]
      : []),
    ...(includeFecIds && (input.rosterFecIds?.length ?? 0) > 0
      ? [`- candidate_fec_ids: ${JSON.stringify(input.rosterFecIds)}`]
      : []),
    ...(!includeFecIds && (input.rosterStateFilingIds?.length ?? 0) > 0
      ? [`- candidate_state_filing_ids: ${JSON.stringify(input.rosterStateFilingIds)}`]
      : []),
    "",
    "Return JSON with this exact shape:",
    "{",
    '  "display_name": "name for display",',
    '  "first_name": "first name",',
    '  "last_name": "last name",',
    ...(!includeFecIds ? ['  "date_of_birth": "YYYY-MM-DD (optional)",'] : []),
    '  "twitter_handle": "handle without URL (optional)",',
    '  "linkedin_url": "https://... (optional)",',
    '  "official_website_url": "https://... (optional)",',
    '  "current_office": "current elected/appointed/public office, if any (optional)",',
    '  "has_held_public_office": true|false,',
    '  "summary": "short neutral bio summary (optional)",',
    '  "sources": ["https://..."]',
    "}",
    "",
    "Rules:",
    "- Research this exact person running in this exact election context; avoid same-name mismatches.",
    ...(input.disambiguationHint
      ? ["- Use roster_disambiguation_hint to target this person only. If evidence conflicts, do not guess."]
      : []),
    ...(input.disambiguationHint
      ? ["- When identity is uncertain, prefer null/omission for identity fields over guessing another person's identifiers."]
      : []),
    ...(includeFecIds
      ? ["- For this federal contest, do not include date_of_birth; backend stores it as null."]
      : ["- date_of_birth, twitter_handle (without URL), linkedin_url, official_website_url, current_office are optional."]),
    ...(includeFecIds
      ? ["- twitter_handle (without URL), linkedin_url, official_website_url, current_office are optional."]
      : []),
    "- Use current_office only for a current elected, appointed, or public office, such as Governor, State Treasurer, or Secretary of State.",
    "- Do not put occupation, employer, campaign status, or past office in current_office.",
    "- Use null/omission for current_office if no reliable source-backed current office is found.",
    "- has_held_public_office is required: true if this person has EVER held elected or appointed public office (current or former, including judicial office), false if never.",
    "- Use null/omission for unknown optional fields; do not invent.",
    "- summary is who the person is and what they have done: current role, career, qualifications.",
    "- The app always shows the summary next to the contest, so in summary do not name the office, election, election date, or stage the candidate is running for.",
    '- No campaign-status or horse-race content in summary: no vote percentages, no primary results, no opponents, no "running for...", "seeking re-election", or "faces X in the runoff".',
    "- Include sources used for this profile and identity evidence.",
    ...PLAIN_LANGUAGE_STYLE_RULES,
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
