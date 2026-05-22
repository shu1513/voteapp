export type CandidateProfilePromptInput = {
  candidateDisplayName: string;
  districtName: string;
  districtType: string;
  state: string;
  electionDate: string;
  officialBallotTitle: string;
  rosterParty?: string;
  rosterIncumbent?: boolean;
  seedUrls?: readonly string[];
  reviewFeedbackLines?: readonly string[];
};

export function buildCandidateProfilePrompt(input: CandidateProfilePromptInput): string {
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
    `- election_date: "${input.electionDate}"`,
    `- official_ballot_title: "${input.officialBallotTitle}"`,
    ...(input.rosterParty ? [`- roster_party_hint: "${input.rosterParty}"`] : []),
    ...(input.rosterIncumbent !== undefined
      ? [`- roster_is_incumbent_hint: ${input.rosterIncumbent ? "true" : "false"}`]
      : []),
    "",
    "Return JSON with this exact shape:",
    "{",
    '  "display_name": "name for display",',
    '  "first_name": "first name",',
    '  "last_name": "last name",',
    '  "party": "party label (optional)",',
    '  "date_of_birth": "YYYY-MM-DD (optional)",',
    '  "twitter_handle": "handle without URL, optional",',
    '  "linkedin_url": "https://... (optional)",',
    '  "official_website_url": "https://... (optional)",',
    '  "fec_ids": ["..."],',
    '  "state_filing_ids": ["..."],',
    '  "summary": "short neutral bio summary (optional)",',
    '  "sources": ["https://..."]',
    "}",
    "",
    "Rules:",
    "- Research this exact person running in this exact election context; avoid same-name mismatches.",
    "- official_website_url is optional.",
    "- date_of_birth, twitter_handle, linkedin_url, fec_ids, state_filing_ids are optional.",
    "- Use null/omission for unknown optional fields; do not invent.",
    "- Include sources used for this profile and identity evidence.",
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
