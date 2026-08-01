import { isUsSenateOfficeTitle } from "../../utils/senateOffice.js";
import type { CandidateResearchMode } from "../candidateResearchMode.js";

export type CandidateRosterPromptInput = {
  districtName: string;
  districtType: string;
  state: string;
  electionDate: string;
  officialBallotTitle: string;
  electionStage?: string | null;
  senateClass?: string | null;
  termEndYear?: string | null;
  researchMode: CandidateResearchMode;
  includeParty?: boolean;
  seedUrls?: readonly string[];
  reviewFeedbackLines?: readonly string[];
};

export function buildCandidateRosterPrompt(input: CandidateRosterPromptInput): string {
  const includeParty = input.includeParty !== false;
  const includeSenateContext = isUsSenateOfficeTitle(input.officialBallotTitle);
  const includeFecIds = input.researchMode !== "state_level";
  const seedUrls = input.seedUrls ?? [];
  const reviewFeedbackLines = input.reviewFeedbackLines ?? [];

  return [
    "You are researching one office election contest and must return only the candidate roster.",
    "Return strict JSON only.",
    "",
    "Election context:",
    `- district_name: "${input.districtName}"`,
    `- district_type: "${input.districtType}"`,
    `- state: "${input.state}"`,
    `- election_date: "${input.electionDate}"`,
    `- official_ballot_title: "${input.officialBallotTitle}"`,
    `- research_mode: "${input.researchMode}"`,
    ...(includeSenateContext && input.electionStage ? [`- election_stage: "${input.electionStage}"`] : []),
    ...(includeSenateContext && input.senateClass ? [`- senate_class: "${input.senateClass}"`] : []),
    ...(includeSenateContext && input.termEndYear ? [`- term_end_year: "${input.termEndYear}"`] : []),
    "",
    "Return JSON with this exact shape:",
    "{",
    '  "candidates": [',
    "    {",
    '      "display_name": "candidate name appears exactly as listed on the official ballot",',
    ...(includeParty ? ['      "party": "party label when clearly known (optional)",'] : []),
    '      "is_incumbent": true,',
    ...(includeFecIds ? ['      "fec_ids": ["required FEC candidate ID(s)"],'] : []),
    ...(!includeFecIds ? ['      "state_filing_ids": ["state filing ID(s) (optional)"],'] : []),
    '      "running_mate": { "display_name": "running mate ballot name", "party": "optional", "sources": ["https://..."] },',
    '      "sources": ["https://..."]',
    "    }",
    "  ]",
    "}",
    "",
    "Rules:",
    "- Actively search the public web for this exact contest.",
    "- Return only candidates running in this exact contest (no other districts or offices).",
    "- Build the roster from the state or local election authority's certified/qualified candidate list for this exact stage (a primary certification is not a general-election roster); aggregators, news roundups, and a candidate's own site are leads to verify, never the roster.",
    "- Exclude withdrawn, disqualified, non-qualified, and merely-declared filers; if the authority has not yet certified or qualified candidates for this stage, return an empty candidates array.",
    "- A person who is only mentioned, endorsed, or speculated about on a page is not a candidate; every row needs the authority's list or an official filing placing them in this contest.",
    "- sources must not be social/UGC platforms, generated candidate directories, or personal blogs; the importer rejects known platform domains.",
    "- candidates can be an empty array if no roster is found.",
    "- display_name must match the ballot-listed candidate name exactly when available (do not substitute legal/full names).",
    "- Do not deduplicate by display_name; include each ballot-listed candidate row, even for same-name candidates.",
    "- When this office elects a joint ticket (for example Governor / Lieutenant Governor), return one candidate row per ticket: display_name is the ticket lead's ballot name only, and running_mate carries the second person's ballot name and sources. Never merge two people's names into one display_name.",
    "- running_mate is only for offices where two people are elected together on one ballot line; omit it everywhere else.",
    ...(includeFecIds
      ? [
          "- For this federal contest, fec_ids is required for each candidate and must include one or more current-cycle FEC candidate IDs.",
          "- Omit candidates who have no current-cycle FEC candidate ID: candidates who never registered with the FEC are not treated as serious contenders. Do not reuse FEC IDs from older election cycles.",
        ]
      : []),
    "- Each candidate must include at least one supporting source URL.",
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
