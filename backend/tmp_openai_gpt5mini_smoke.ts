import { getPipelineEnv } from "./src/config/env.js";
import { openAiProvider } from "./src/ai/providers/openaiProvider.js";

async function main(): Promise<void> {
  const env = getPipelineEnv();

  const result = await openAiProvider(
    {
      ingestKey: "smoke:gpt5mini",
      promptVersion: env.PROMPT_VERSION,
      draft: {
        state_fips: "06",
        state_abbreviation: "CA",
        state_name: "California",
        population_estimate: null,
        census_source_url: "https://api.census.gov/data/2024/acs/acs5?get=NAME,B01001_001E&for=state:*",
        state_abbreviation_reference_url: "https://pe.usps.com/text/pub28/28apb.htm",
        seed_sources: ["https://vote.gov/register"],
      },
      evidence: [
        {
          url: "https://vote.gov/register/california",
          title: "Vote.gov California",
          snippet: "Register to vote online in California and check deadlines.",
        },
        {
          url: "https://www.sos.ca.gov/elections/polling-place",
          title: "California polling place",
          snippet: "Find your polling place in California.",
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
    console.error(
      JSON.stringify(
        {
          ok: false,
          errorCode: result.errorCode,
          retryable: result.retryable,
          reason: result.reason,
        },
        null,
        2
      )
    );
    process.exitCode = 1;
    return;
  }

  console.log(
    JSON.stringify(
      {
        ok: true,
        hasRawPayload: typeof result.rawPayload === "object" && result.rawPayload !== null,
        hasDebugMeta: !!result.debugMeta,
      },
      null,
      2
    )
  );
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
