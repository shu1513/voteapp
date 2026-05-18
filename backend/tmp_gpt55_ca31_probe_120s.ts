import { getPipelineEnv } from "./src/config/env";
import { enrichElections } from "./src/ai/enrichElections";

(async () => {
  const env = getPipelineEnv();
  const draft = {
    district_id: "958b9eb8-2f80-4295-a14b-b8f8375f43f6",
    district_name: "California's 31st congressional district",
    district_type: "us_house" as const,
    state: "CA",
  };

  const started = Date.now();
  const result = await enrichElections(
    {
      ingestKey: `gpt55_probe_120s:us_house:${Date.now()}`,
      draft,
      promptVersion: "v3",
      softRetryCount: 0,
      reviewFeedback: [],
    },
    {
      timeoutMs: 120000,
      anthropicWebSearchMaxUses: env.ANTHROPIC_WEB_SEARCH_MAX_USES,
      openAiApiKey: env.OPENAI_API_KEY,
      anthropicApiKey: env.ANTHROPIC_API_KEY,
      geminiApiKey: env.GEMINI_API_KEY,
    },
    [{ provider: "openai", model: "gpt-5.5" }]
  );

  console.log(JSON.stringify({ model: "openai:gpt-5.5", timeoutMs: 120000, elapsedMs: Date.now() - started, ok: result.ok, result }, null, 2));
})();
