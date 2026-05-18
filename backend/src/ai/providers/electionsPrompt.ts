import type { ElectionDraftPayload } from "../../types/election.js";

function escapeJson(value: string): string {
  return JSON.stringify(value);
}

export function buildElectionsPrompt(args: {
  draft: ElectionDraftPayload;
  softRetryCount: number;
  reviewFeedbackLines: string[];
}): string {
  const { draft, softRetryCount, reviewFeedbackLines } = args;
  const retrySection =
    reviewFeedbackLines.length > 0
      ? [
          "",
          "Previous validator feedback to address:",
          ...reviewFeedbackLines.map((line, index) => `${index + 1}. ${line}`),
          "Fix only the issues above and keep valid content.",
        ].join("\n")
      : "";

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
    `{
  "district_id": ${escapeJson(draft.district_id)},
  "district_name": ${escapeJson(draft.district_name)},
  "district_type": ${escapeJson(draft.district_type)},
  "state": ${escapeJson(draft.state)},
  "entries": [
    {
      "official_ballot_title": "exact title shown on ballot",
      "election_date": "YYYY-MM-DD",
      "description": "brief factual description",
      "race_type": "office or ballot_measure",
      "sources": ["https://..."]
    }
  ],
  "review_decision": "approve or reject",
  "review_reason": "short reason"
}`,
    "",
    "Rules:",
    "- Actively search the public web for this task.",
    "- Use web access to verify upcoming contests for this exact district scope.",
    "- If one source is insufficient, continue to additional sources until you can confirm or rule out contests.",
    "- entries may be an empty array when no upcoming contest is found.",
    "- race_type must be one of: office, ballot_measure.",
    "- Focus on upcoming elections only; do not include past elections.",
    "- Use only contests in this exact district scope (no parent or child scope contests).",
    "- Prefer official election sources first (state/county/city/school election offices).",
    "- If official sources are unavailable or unclear, use reliable third-party sources and keep citations.",
    "- Prefer official (.gov / election office) URLs over third-party URLs when both support the same entry.",
    "- Use the official contest label/title from the election authority when available.",
    "- If scope is uncertain, exclude the entry instead of guessing.",
    "- sources must list the URLs you used for each entry, and each entry should include at least one directly supporting source URL.",
    "- return JSON only (no prose, no markdown).",
    softRetryCount > 0
      ? "- This is a review pass after validator soft-fail. Decide approve/reject in review_decision."
      : "- review_decision should be approve when your output is within district scope; otherwise reject.",
    retrySection,
    "",
    "Draft input:",
    JSON.stringify(draft),
  ].join("\n");
}
