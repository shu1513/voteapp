import { getPipelineEnv } from "./src/config/env.js";
import { openAiProvider } from "./src/ai/providers/openaiProvider.js";

async function main(): Promise<void> {
  const env = getPipelineEnv();
  const result = await openAiProvider(
    {
      ingestKey: `live:state_resources:${Date.now()}`,
      promptVersion: env.PROMPT_VERSION,
      fieldGroup: "online_registration",
      draft: {
        state_fips: "06",
        state_abbreviation: "CA",
        state_name: "California",
        population_estimate: null,
        census_source_url: "https://api.census.gov/data/2024/acs/acs5?get=NAME,B01001_001E&for=state:*",
        state_abbreviation_reference_url: "https://pe.usps.com/text/pub28/28apb.htm",
        seed_sources: ["https://vote.gov/register/california"],
      },
      evidence: [
        {
          url: "https://vote.gov/register/california",
          title: "Vote.gov California",
        },
      ],
    },
    {
      provider: "openai",
      model: "gpt-5.4-mini",
      timeoutMs: env.AI_TIMEOUT_MS,
      openAiApiKey: env.OPENAI_API_KEY,
    }
  );

  if (!result.ok) {
    console.log(JSON.stringify({ type: "state_resources_live", ok: false, result }, null, 2));
    process.exitCode = 1;
    return;
  }

  console.log(
    JSON.stringify(
      {
        type: "state_resources_live",
        ok: true,
        openai_api_mode: result.debugMeta?.openai_api_mode ?? null,
        web_search_sources_count: result.debugMeta?.web_search_sources_count ?? null,
        rawPayload: result.rawPayload,
      },
      null,
      2
    )
  );
}

main().catch((error) => {
  const reason = error instanceof Error ? error.message : String(error);
  console.error(JSON.stringify({ ok: false, reason }, null, 2));
  process.exit(1);
});
