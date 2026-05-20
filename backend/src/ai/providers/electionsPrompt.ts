import type { ElectionDraftPayload } from "../../types/election.js";

export type ElectionContestFamily =
  | "all"
  | "non_judicial_office"
  | "judicial_office"
  | "ballot_measure";

function escapeJson(value: string): string {
  return JSON.stringify(value);
}

export function buildElectionsPrompt(args: {
  draft: ElectionDraftPayload;
  softRetryCount: number;
  reviewFeedbackLines: string[];
  contestFamily?: ElectionContestFamily;
  seedUrls?: readonly string[];
}): string {
  const { draft, softRetryCount, reviewFeedbackLines, contestFamily = "all", seedUrls = [] } = args;
  const includeElectionStageInOutput = contestFamily !== "ballot_measure";
  const isBallotFamily = contestFamily === "ballot_measure";
  const isOfficeOnlyFamily =
    contestFamily === "non_judicial_office" || contestFamily === "judicial_office";
  const includeRaceTypeInOutput = contestFamily === "all";
  const retrySection =
    reviewFeedbackLines.length > 0
      ? [
          "",
          "Previous validator feedback to address:",
          ...reviewFeedbackLines.map((line, index) => `${index + 1}. ${line}`),
          "Fix only the issues above and keep valid content.",
        ].join("\n")
      : "";
  const familySection =
    contestFamily === "all"
      ? []
      : [
          "",
          `Contest family for this call: ${contestFamily}`,
          contestFamily === "non_judicial_office"
            ? "- Return only non-judicial office contests for this district scope (executive/legislative/administrative local offices)."
            : contestFamily === "judicial_office"
              ? "- Return only judicial office contests for this district scope (judge/justice/retention)."
              : "- Return only ballot measure contests for this district scope.",
          contestFamily === "non_judicial_office"
            ? "- Exclude all ballot measures and all judicial contests."
            : contestFamily === "judicial_office"
              ? "- Exclude all ballot measures and all non-judicial offices."
              : "- Exclude all office contests.",
          contestFamily === "non_judicial_office"
            ? '- Non-judicial office examples: Governor, Lieutenant Governor, Secretary of State, Treasurer, Controller, Attorney General, Superintendent of Public Instruction, Board of Supervisors, Sheriff, Assessor, County Clerk, Mayor, City Council.'
            : contestFamily === "judicial_office"
              ? '- Judicial examples: "Judge of the Superior Court, Office No. X", "Justice of the Supreme Court (Retention)", "Court of Appeal Justice (Retention)".'
              : '- Ballot measure examples: Proposition, Measure, Bond, Charter Amendment, Referendum, Initiative, Advisory Question.',
        ];
  const includeReviewFieldsInShape = softRetryCount > 0;
  const scopePriorityLine =
    draft.district_type === "statewide"
      ? "- Source priority for this scope: prefer official state secretary-of-state/state election office pages first; if unavailable or unclear, use reliable third-party sources; when both support the same entry, prefer official URLs."
      : draft.district_type === "county"
        ? "- Source priority for this scope: prefer official county registrar/county elections office pages first (including official list-of-offices pages); if unavailable or unclear, use reliable third-party sources; when both support the same entry, prefer official URLs."
        : draft.district_type === "place"
          ? "- Source priority for this scope: prefer official city/town clerk or local elections office pages first; if unavailable or unclear, use reliable third-party sources; when both support the same entry, prefer official URLs."
          : "";
  const includeOfficialSourcePriorityLine = contestFamily !== "ballot_measure";
  const ballotOfficialPreferenceLine =
    "- Prefer official election sources first for this scope; when both official and third-party sources support the same entry, prefer official URLs.";
  const entryShapeLines = [
    '      "official_ballot_title": "exact title shown on ballot",',
    '      "election_date": "YYYY-MM-DD",',
    ...(includeElectionStageInOutput
      ? ['      "election_stage": "primary | general | runoff | special",']
      : []),
    isBallotFamily
      ? '      "impact": "what this measure changes",'
      : isOfficeOnlyFamily
        ? '      "impact": "what this office does",'
        : '      "impact": "what this office does OR what this measure changes",',
    ...(includeRaceTypeInOutput ? ['      "race_type": "office or ballot_measure",'] : []),
    '      "sources": ["https://..."]',
  ].join("\n");
  const outputShape = includeReviewFieldsInShape
    ? `{
  "entries": [
    {
${entryShapeLines}
    }
  ],
  "review_decision": "approve or reject",
  "review_reason": "short reason"
}`
    : `{
  "entries": [
    {
${entryShapeLines}
    }
  ]
}`;

  return [
    "You are extracting upcoming election entries for exactly one district scope.",
    "Do not include parent or sub-district contests.",
    "",
    "District context:",
    `- district_name: ${escapeJson(draft.district_name)}`,
    `- district_type: ${escapeJson(draft.district_type)}`,
    `- state: ${escapeJson(draft.state)}`,
    "",
    "Return strict JSON with this exact shape:",
    outputShape,
    "",
    "Rules:",
    "- Actively search the public web for this task.",
    "- Use web access to verify upcoming contests for this exact district scope.",
    "- If one source is insufficient, continue to additional sources until you can confirm or rule out contests.",
    "- entries may be an empty array when no upcoming contest is found.",
    ...(includeRaceTypeInOutput ? ["- race_type must be one of: office, ballot_measure."] : []),
    ...(includeElectionStageInOutput
      ? [
          includeRaceTypeInOutput
            ? "- election_stage is optional and only applies when race_type is office; include it only when clearly known from source. When included, set one of: primary, general, runoff, special."
            : "- election_stage is optional; include it only when clearly known from source. When included, set one of: primary, general, runoff, special.",
        ]
      : []),
    isBallotFamily
      ? "- impact: Explain what this measure would actually change if passed, in concrete, no fluff real-world terms."
      : isOfficeOnlyFamily
        ? "- impact: Explain what this office does, in concrete, no fluff real-world terms."
        : "- impact: Explain what this office does (if race_type=office) or what this measure would actually change if passed (if race_type=ballot_measure), in concrete, no fluff real-world terms.",
    "- Example impact for office: \"Leads the county sheriff's department, oversees patrol and jail operations, and sets local law-enforcement priorities.\"",
    "- Example impact for ballot_measure: \"Increases county sales tax by 0.5% for five years to fund county hospital and clinic services.\"",
    "- Focus on upcoming elections only; do not include past elections.",
    "- Use only contests in this exact district scope (no parent or child scope contests).",
    "- Copy official_ballot_title exactly as shown on the ballot when available; do not paraphrase.",
    "- For ballot measures, official_ballot_title must be the actual official measure label/title from the election authority (for example: Measure ER or Proposition 4), not the full ballot question sentence.",
    includeOfficialSourcePriorityLine ? scopePriorityLine : ballotOfficialPreferenceLine,
    ...familySection,
    "- If scope is uncertain, exclude the entry instead of guessing.",
    "- sources must list the URLs you used for each entry, and each entry should include at least one directly supporting source URL.",
    "- return JSON only (no prose, no markdown).",
    softRetryCount > 0
      ? "- This is a review pass after validator soft-fail. Include review_decision (approve/reject) and review_reason."
      : "",
    retrySection,
    ...(seedUrls.length > 0
      ? [
          "",
          "Starting reference URLs (use these first, then expand research as needed):",
          ...seedUrls.map((url) => `- ${url}`),
        ]
      : []),
    "",
    "Draft input:",
    JSON.stringify(draft),
  ].join("\n");
}
