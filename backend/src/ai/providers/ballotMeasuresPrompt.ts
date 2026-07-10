import { PLAIN_LANGUAGE_STYLE_RULES } from "./promptWritingStyle.js";

export type BallotMeasurePromptInput = {
  districtName: string;
  districtType: string;
  state: string;
  electionDate: string;
  officialBallotTitle: string;
  seedUrls: readonly string[];
  allowedResearchAreaSlugs: readonly string[];
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
    '  "research_area_tags": [',
    '    { "research_area_slug": "housing_affordability", "stance": "for" }',
    "  ],",
    '  "sources": ["https://..."]',
    "}",
    "",
    "Rules:",
    "- Actively search the public web for this measure.",
    "- official_measure_url must point to the source where a reader can view the full official measure text in its entirety (for example, the election authority's official measure page or official PDF text).",
    "- summary must be a neutral, concise plain-language summary of the measure’s real-world policy impact if enacted.",
    "- what_yes_means and what_no_means must be concrete and neutral.",
    "- research_area_tags describes the likely policy effect if YES wins / the measure passes.",
    '- For research_area_tags, "for" means the YES outcome advances that research area’s goal.',
    '- For research_area_tags, "against" means the YES outcome cuts against that research area’s goal.',
    "- Do not tag an area if the effect is mixed, indirect, unclear, or not meaningfully directional.",
    "- Do not use stance to describe which campaign side supports the measure.",
    "- research_area_tags must use only allowed research_area_slug values.",
    "- Return an empty research_area_tags array only if no allowed area clearly applies.",
    "- sources must include the official/full-text URL and the best supporting URLs used for this research, up to 20 unique URLs.",
    ...PLAIN_LANGUAGE_STYLE_RULES,
    "- return JSON only (no prose, no markdown).",
    "",
    "Allowed research_area_slug values:",
    ...input.allowedResearchAreaSlugs.map((slug) => `- ${escapeJson(slug)}`),
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
