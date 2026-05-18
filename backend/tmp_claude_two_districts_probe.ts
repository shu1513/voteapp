import { getPipelineEnv } from "./src/config/env";
import { enrichElections } from "./src/ai/enrichElections";

(async () => {
  const env = getPipelineEnv();
  const drafts = [
    {
      district_id: "958b9eb8-2f80-4295-a14b-b8f8375f43f6",
      district_name: "California's 31st congressional district",
      district_type: "us_house" as const,
      state: "CA",
    },
    {
      district_id: "6c3696d6-c5eb-4f4f-bf87-8ab41cf5d0fb",
      district_name: "Baldwin Park city",
      district_type: "place" as const,
      state: "CA",
    },
  ];

  const out: unknown[] = [];
  for (const draft of drafts) {
    const result = await enrichElections(
      {
        ingestKey: `claude_probe:${draft.district_type}:${Date.now()}`,
        draft,
        promptVersion: "v3",
        softRetryCount: 0,
        reviewFeedback: [],
      },
      {
        timeoutMs: 90000,
        anthropicWebSearchMaxUses: env.ANTHROPIC_WEB_SEARCH_MAX_USES,
        openAiUseResponsesWebSearch: env.OPENAI_ELECTIONS_USE_RESPONSES_WEB_SEARCH,
        openAiApiKey: env.OPENAI_API_KEY,
        anthropicApiKey: env.ANTHROPIC_API_KEY,
        geminiApiKey: env.GEMINI_API_KEY,
      },
      [{ provider: "claude", model: "claude-sonnet-4-6" }]
    );

    out.push(
      result.ok
        ? {
            district: draft.district_name,
            ok: true,
            review_decision: result.payload.review_decision,
            review_reason: result.payload.review_reason,
            entries: result.payload.entries,
          }
        : {
            district: draft.district_name,
            ok: false,
            errorCode: result.errorCode,
            reason: result.reason,
          }
    );
  }

  console.log(JSON.stringify(out, null, 2));
})();
