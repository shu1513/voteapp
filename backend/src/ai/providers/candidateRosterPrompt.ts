export type CandidateRosterPromptInput = {
  districtName: string;
  districtType: string;
  state: string;
  electionDate: string;
  officialBallotTitle: string;
  seedUrls?: readonly string[];
  reviewFeedbackLines?: readonly string[];
};

export function buildCandidateRosterPrompt(input: CandidateRosterPromptInput): string {
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
    "",
    "Return JSON with this exact shape:",
    "{",
    '  "candidates": [',
    "    {",
    '      "display_name": "candidate name exactly as listed",',
    '      "party": "party label when clearly known (optional)",',
    '      "is_incumbent": true,',
    '      "sources": ["https://..."]',
    "    }",
    "  ]",
    "}",
    "",
    "Rules:",
    "- Actively search the public web for this exact contest.",
    "- Return only people running in this exact contest (no other districts or offices).",
    "- candidates can be an empty array if no roster is found.",
    "- Do not include candidate profile details in this call.",
    "- Deduplicate candidates by display_name.",
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
