import { loadProjectEnv, getPipelineEnv } from "./src/config/env.ts";
import { STATE_RESOURCES_AI_CANDIDATES } from "./src/ai/aiCandidates.ts";
import { enrichStateResourcesGroup } from "./src/ai/enrichStateResources.ts";
import { STATE_RESOURCE_FIELD_GROUP_ORDER } from "./src/ai/stateResourceFieldGroups.ts";
import type { EnrichStateResourcesConfig, EnrichStateResourcesInput } from "./src/ai/types.ts";

type SmokeResult = {
  provider: string;
  model: string;
  fieldGroup: string;
  ok: boolean;
  errorCode?: string;
  reason?: string;
};

async function run(): Promise<void> {
  loadProjectEnv();
  const env = getPipelineEnv();

  const baseInput: Omit<EnrichStateResourcesInput, "fieldGroup"> = {
    ingestKey: "state_resources:06:2026:group_smoke",
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
        snippet: "California voter registration and mail voting information.",
      },
      {
        url: "https://www.ncsl.org/elections-and-campaigns/early-in-person-voting",
        title: "NCSL early voting",
        snippet: "State early in-person voting rules and windows.",
      },
      {
        url: "https://www.ncsl.org/elections-and-campaigns/polling-places",
        title: "NCSL polling places",
        snippet: "Polling place and polling hours references by state.",
      },
      {
        url: "https://www.vote.org/polling-place-locator/",
        title: "Vote.org polling place locator",
        snippet: "Find your polling place.",
      },
      {
        url: "https://www.ncsl.org/elections-and-campaigns/same-day-voter-registration",
        title: "NCSL same-day registration",
        snippet: "States with same-day voter registration policies.",
      },
      {
        url: "https://www.ncsl.org/elections-and-campaigns/voter-id",
        title: "NCSL voter ID",
        snippet: "State voter ID requirements.",
      },
    ],
  };

  const results: SmokeResult[] = [];

  for (const candidate of STATE_RESOURCES_AI_CANDIDATES) {
    const config: EnrichStateResourcesConfig = {
      provider: candidate.provider,
      model: candidate.model,
      timeoutMs: env.AI_TIMEOUT_MS,
      openAiApiKey: env.OPENAI_API_KEY,
      anthropicApiKey: env.ANTHROPIC_API_KEY,
      geminiApiKey: env.GEMINI_API_KEY,
    };

    for (const group of STATE_RESOURCE_FIELD_GROUP_ORDER) {
      const result = await enrichStateResourcesGroup(
        {
          ...baseInput,
          fieldGroup: group,
        },
        config
      );

      if (result.ok) {
        results.push({
          provider: candidate.provider,
          model: candidate.model,
          fieldGroup: group,
          ok: true,
        });
      } else {
        results.push({
          provider: candidate.provider,
          model: candidate.model,
          fieldGroup: group,
          ok: false,
          errorCode: result.errorCode,
          reason: result.reason,
        });
      }
    }
  }

  const failed = results.filter((r) => !r.ok);
  const invalidJson = failed.filter((r) => r.errorCode === "INVALID_JSON");
  const missingFields = failed.filter((r) => r.errorCode === "MISSING_REQUIRED_FIELDS");

  console.log(
    JSON.stringify(
      {
        type: "group_model_smoke_summary",
        total: results.length,
        passed: results.length - failed.length,
        failed: failed.length,
        invalidJsonCount: invalidJson.length,
        missingRequiredFieldsCount: missingFields.length,
        failures: failed,
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

