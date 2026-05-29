export type BallotMeasurePromptInput = {
  districtName: string;
  districtType: string;
  state: string;
  electionDate: string;
  officialBallotTitle: string;
  seedUrls: readonly string[];
  reviewFeedbackLines?: readonly string[];
};

function escapeJson(value: string): string {
  return JSON.stringify(value);
}

export function buildBallotMeasuresPrompt(input: BallotMeasurePromptInput): string {
  const retrySection =
    input.reviewFeedbackLines && input.reviewFeedbackLines.length > 0
      ? [
          "",
          "Previous feedback to address:",
          ...input.reviewFeedbackLines.map((line, index) => `${index + 1}. ${line}`),
          "Fix only the issues above and keep valid content.",
        ]
      : [];

  return [
    "You are researching one ballot measure contest.",
    "Return strict JSON only.",
    "",
    "Contest context:",
    `- district_name: ${escapeJson(input.districtName)}`,
    `- district_type: ${escapeJson(input.districtType)}`,
    `- state: ${escapeJson(input.state)}`,
    `- election_date: ${escapeJson(input.electionDate)}`,
    `- official_ballot_title: ${escapeJson(input.officialBallotTitle)}`,
    "",
    "Return JSON with this exact shape:",
    "{",
    '  "official_measure_url": "https://...",',
    '  "summary": "neutral plain-language summary of the measure’s real-world policy impact if enacted",',
    '  "what_yes_means": "plain-language voter impact if YES",',
    '  "what_no_means": "plain-language voter impact if NO",',
    '  "sources": ["https://..."]',
    "}",
    "",
    "Rules:",
    "- Actively search the public web for this measure.",
    "- official_measure_url must point to the source where a reader can view the full official measure text in its entirety (for example, the election authority's official measure page or official PDF text).",
    "- summary must be a neutral, concise plain-language summary of the measure’s real-world policy impact if enacted.",
    "- what_yes_means and what_no_means must be concrete and neutral.",
    "- sources must include all URLs you used for this research.",
    "- return JSON only (no prose, no markdown).",
    ...retrySection,
    ...(input.seedUrls.length > 0
      ? [
          "",
          "Starting reference URLs (use these first, then expand research as needed):",
          ...input.seedUrls.map((url) => `- ${escapeJson(url)}`),
        ]
      : []),
  ].join("\n");
}
