import { getPipelineEnv } from "./src/config/env";
import { enrichElections } from "./src/ai/enrichElections";

(async () => {
  const env = getPipelineEnv();

  const drafts = [
    { district_id: "f9cf788f-9ec6-42b2-9f2b-c6758f44a61d", district_name: "California", district_type: "statewide", state: "CA" },
    { district_id: "958b9eb8-2f80-4295-a14b-b8f8375f43f6", district_name: "California's 31st congressional district", district_type: "us_house", state: "CA" },
    { district_id: "8f81ae0f-0698-4f2b-8423-831f0fcd2f32", district_name: "Los Angeles County", district_type: "county", state: "CA" },
    { district_id: "6c3696d6-c5eb-4f4f-bf87-8ab41cf5d0fb", district_name: "Baldwin Park city", district_type: "place", state: "CA" }
  ];

  const out: any[] = [];
  for (const d of drafts) {
    const started = Date.now();
    const result = await enrichElections(
      {
        ingestKey: `live_probe:${d.district_type}:${d.state}:${Date.now()}`,
        draft: d,
        promptVersion: "v3",
        softRetryCount: 0,
        reviewFeedback: [],
      },
      {
        openAiApiKey: env.OPENAI_API_KEY,
        anthropicApiKey: env.ANTHROPIC_API_KEY,
        geminiApiKey: env.GEMINI_API_KEY,
        timeoutMs: 90000,
        anthropicWebSearchMaxUses: env.ANTHROPIC_WEB_SEARCH_MAX_USES,
        openAiUseResponsesWebSearch: true,
      },
      [{ provider: "openai", model: "gpt-5.4-mini" }]
    );

    out.push({
      district: `${d.district_name} (${d.district_type})`,
      ok: result.ok,
      elapsedMs: Date.now() - started,
      entriesCount: result.ok ? result.payload.entries.length : null,
      reviewDecision: result.ok ? result.payload.review_decision : null,
      reviewReason: result.ok ? result.payload.review_reason : null,
      apiMode: result.ok ? result.aiRawDebug?.openai_api_mode ?? null : null,
      webSourcesCount: result.ok ? result.aiRawDebug?.openai_web_search_sources_count ?? null : null,
      providerError: result.ok ? null : { code: result.errorCode, reason: result.reason },
    });
  }

  console.log(JSON.stringify(out, null, 2));
})();
