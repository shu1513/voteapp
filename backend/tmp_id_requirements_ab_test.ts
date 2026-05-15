import { loadProjectEnv, getPipelineEnv } from "./src/config/env.ts";
import { enrichStateResourcesGroup } from "./src/ai/enrichStateResources.ts";
import type { EnrichStateResourcesConfig, EnrichStateResourcesInput } from "./src/ai/types.ts";

const EXPECTED = "Strict non-photo ID";
const ATTEMPTS_PER_MODEL = 4;

type ModelCase = { provider: "openai"; model: string };
const CASES: readonly ModelCase[] = [
  { provider: "openai", model: "gpt-5.4-mini" },
  { provider: "openai", model: "gpt-5.2" },
] as const;

async function run(): Promise<void> {
  loadProjectEnv();
  const env = getPipelineEnv();

  const input: EnrichStateResourcesInput & { fieldGroup: "id_requirements" } = {
    ingestKey: "state_resources:04:2026:id_req_ab",
    fieldGroup: "id_requirements",
    draft: {
      state_fips: "04",
      state_abbreviation: "AZ",
      state_name: "Arizona",
    },
    promptVersion: env.PROMPT_VERSION,
    evidence: [
      {
        url: "https://www.ncsl.org/elections-and-campaigns/voter-id",
        title: "NCSL voter ID",
      },
    ],
  };

  const results: Array<{
    provider: string;
    model: string;
    attempt: number;
    ok: boolean;
    idRequirements: string | null;
    expectedMatch: boolean;
    errorCode?: string;
    reason?: string;
  }> = [];

  for (const testCase of CASES) {
    const config: EnrichStateResourcesConfig = {
      provider: testCase.provider,
      model: testCase.model,
      timeoutMs: env.AI_TIMEOUT_MS,
      openAiApiKey: env.OPENAI_API_KEY,
      anthropicApiKey: env.ANTHROPIC_API_KEY,
      geminiApiKey: env.GEMINI_API_KEY,
    };

    for (let attempt = 1; attempt <= ATTEMPTS_PER_MODEL; attempt += 1) {
      const result = await enrichStateResourcesGroup(input, config);
      if (!result.ok) {
        results.push({
          provider: testCase.provider,
          model: testCase.model,
          attempt,
          ok: false,
          idRequirements: null,
          expectedMatch: false,
          errorCode: result.errorCode,
          reason: result.reason,
        });
        continue;
      }

      const value = result.payload.id_requirements;
      results.push({
        provider: testCase.provider,
        model: testCase.model,
        attempt,
        ok: true,
        idRequirements: value,
        expectedMatch: value === EXPECTED,
      });
    }
  }

  const summary = CASES.map((testCase) => {
    const rows = results.filter((r) => r.model === testCase.model);
    const okCount = rows.filter((r) => r.ok).length;
    const expectedCount = rows.filter((r) => r.expectedMatch).length;
    return {
      provider: testCase.provider,
      model: testCase.model,
      attempts: rows.length,
      okCount,
      expectedCount,
      expectedRate: Number((expectedCount / Math.max(rows.length, 1)).toFixed(2)),
      failures: rows.filter((r) => !r.ok).map((r) => ({ attempt: r.attempt, errorCode: r.errorCode, reason: r.reason })),
      outputs: rows.filter((r) => r.ok).map((r) => ({ attempt: r.attempt, idRequirements: r.idRequirements, expectedMatch: r.expectedMatch })),
    };
  });

  console.log(
    JSON.stringify(
      {
        type: "id_requirements_ab_test",
        state: "AZ",
        expected: EXPECTED,
        attemptsPerModel: ATTEMPTS_PER_MODEL,
        summary,
      },
      null,
      2,
    ),
  );
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
