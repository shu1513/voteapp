import {
  FINANCE_INDUSTRY_SLUGS,
  type FinanceLabelType,
} from "../../pipeline/finance/financeLabelClassifier.js";

export type FinanceIndustryClassificationPromptLabel = {
  rawLabel: string;
  labelType: Extract<FinanceLabelType, "employer" | "donor">;
  normalizedLabel: string;
};

export function buildFinanceIndustryClassificationPrompt(input: {
  labels: readonly FinanceIndustryClassificationPromptLabel[];
}): string {
  const labelsJson = JSON.stringify(
    input.labels.map((label, index) => ({
      id: String(index + 1),
      label: label.rawLabel,
    })),
    null,
    2
  );

  return [
    "Classify campaign-finance employer, donor, or organization labels into one industry slug.",
    "",
    "Rules:",
    "- Use only the allowed industry_slug values below, or unknown.",
    "- If the label is a political committee, campaign committee, PAC, candidate committee, party committee, or ideological group, return unknown.",
    "- If the employer/organization is ambiguous, generic, self-employed, retired, unavailable, or not enough information, return unknown.",
    "- Do not invent industries. Prefer unknown over guessing.",
    "- Return exactly one classification for each input id.",
    "",
    `Allowed industry_slug values: ${FINANCE_INDUSTRY_SLUGS.join(", ")}, unknown`,
    "",
    "Labels to classify:",
    labelsJson,
    "",
    "Return strict JSON only:",
    JSON.stringify(
      {
        classifications: [
          {
            id: "1",
            industry_slug: "technology",
            confidence: "high",
          },
        ],
      },
      null,
      2
    ),
  ].join("\n");
}
