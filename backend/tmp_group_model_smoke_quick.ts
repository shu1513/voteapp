import { loadProjectEnv, getPipelineEnv } from "./src/config/env.ts";
import { AI_CANDIDATES } from "./src/ai/aiCandidates.ts";
import { enrichStateResourcesGroup } from "./src/ai/enrichStateResources.ts";
import type { EnrichStateResourcesConfig, EnrichStateResourcesInput } from "./src/ai/types.ts";

async function run(): Promise<void> {
  loadProjectEnv();
  const env = getPipelineEnv();
  const group = "online_registration" as const;

  const input: EnrichStateResourcesInput & { fieldGroup: "online_registration" } = {
    ingestKey: "state_resources:06:2026:group_smoke_quick",
    fieldGroup: group,
    draft: {
      state_fips: "06",
      state_abbreviation: "CA",
      state_name: "California",
    },
    promptVersion: env.PROMPT_VERSION,
    evidence: [
      {
        url: "https://vote.gov/register/california",
        title: "Vote.gov California registration",
        snippet: "California voter registration and online registration deadline information.",
      },
    ],
  };

  const results: Array<{
    provider: string;
    model: string;
    ok: boolean;
    errorCode?: string;
    reason?: string;
  }> = [];

  for (const candidate of AI_CANDIDATES) {
    const config: EnrichStateResourcesConfig = {
      provider: candidate.provider,
      model: candidate.model,
      timeoutMs: env.AI_TIMEOUT_MS,
      openAiApiKey: env.OPENAI_API_KEY,
      anthropicApiKey: env.ANTHROPIC_API_KEY,
      geminiApiKey: env.GEMINI_API_KEY,
    };
    const result = await enrichStateResourcesGroup(input, config);
    if (result.ok) {
      results.push({
        provider: candidate.provider,
        model: candidate.model,
        ok: true,
      });
    } else {
      results.push({
        provider: candidate.provider,
        model: candidate.model,
        ok: false,
        errorCode: result.errorCode,
        reason: result.reason,
      });
    }
  }

  const invalidJson = results.filter((r) => r.errorCode === "INVALID_JSON").length;
  const missingRequired = results.filter((r) => r.errorCode === "MISSING_REQUIRED_FIELDS").length;

  console.log(
    JSON.stringify(
      {
        type: "group_model_smoke_quick",
        group,
        total: results.length,
        passed: results.filter((r) => r.ok).length,
        invalidJson,
        missingRequired,
        results,
      },
      null,
      2
    )
  );
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

