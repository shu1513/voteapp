import { performance } from "node:perf_hooks";

import { loadProjectEnv } from "./src/config/env.js";
import { openAiProvider } from "./src/ai/providers/openaiProvider.js";
import { claudeProvider } from "./src/ai/providers/claudeProvider.js";
import { geminiProvider } from "./src/ai/providers/geminiProvider.js";
import type {
  AiProvider,
  EnrichStateResourcesConfig,
  EnrichStateResourcesInput,
} from "./src/ai/types.js";

type Candidate = {
  provider: AiProvider;
  model: string;
};

const CANDIDATES: Candidate[] = [
  { provider: "gemini", model: "gemini-2.5-flash-lite" },
  { provider: "gemini", model: "gemini-3.1-flash-lite" },
  { provider: "openai", model: "gpt-5.4-mini" },
  { provider: "claude", model: "claude-sonnet-4-6" },
  { provider: "gemini", model: "gemini-2.5-pro" },
  { provider: "claude", model: "claude-opus-4-7" },
];

const INPUT: EnrichStateResourcesInput = {
  ingestKey: "state_resources:06:2026",
  promptVersion: "state_resources_v1",
  draft: {
    state_fips: "06",
    state_abbreviation: "CA",
    state_name: "California",
  },
  evidence: [
    {
      url: "https://vote.gov/register/california",
      title: "Vote.gov California Registration",
      snippet: "Register to vote in California.",
    },
    {
      url: "https://www.vote.org/polling-place-locator/",
      title: "Vote.org Polling Place Locator",
      snippet: "Find your polling place.",
    },
  ],
};

const providerByName = {
  openai: openAiProvider,
  claude: claudeProvider,
  gemini: geminiProvider,
} as const;

function isModelNotFoundFailure(reason: string): boolean {
  const lowered = reason.toLowerCase();
  return (
    lowered.includes("model") &&
    (lowered.includes("not found") ||
      lowered.includes("unknown") ||
      lowered.includes("unsupported") ||
      lowered.includes("does not exist"))
  );
}

async function run(): Promise<void> {
  loadProjectEnv();

  const baseConfig: Omit<EnrichStateResourcesConfig, "provider" | "model"> = {
    timeoutMs: 90_000,
    openAiApiKey: process.env.OPENAI_API_KEY,
    anthropicApiKey: process.env.ANTHROPIC_API_KEY,
    geminiApiKey: process.env.GEMINI_API_KEY,
  };

  const summary: Array<{
    provider: string;
    model: string;
    ok: boolean;
    callable: boolean;
    retryable?: boolean;
    errorCode?: string;
    reason?: string;
    elapsedMs: number;
  }> = [];

  for (const candidate of CANDIDATES) {
    const startedAt = performance.now();
    const provider = providerByName[candidate.provider];
    const result = await provider(INPUT, {
      ...baseConfig,
      provider: candidate.provider,
      model: candidate.model,
    });
    const elapsedMs = Math.round(performance.now() - startedAt);

    if (result.ok) {
      summary.push({
        provider: candidate.provider,
        model: candidate.model,
        ok: true,
        callable: true,
        elapsedMs,
      });
      continue;
    }

    const modelNotFound = isModelNotFoundFailure(result.reason);
    summary.push({
      provider: candidate.provider,
      model: candidate.model,
      ok: false,
      callable: !modelNotFound,
      retryable: result.retryable,
      errorCode: result.errorCode,
      reason: result.reason,
      elapsedMs,
    });
  }

  console.log(JSON.stringify({ type: "model_smoke_summary", summary }, null, 2));
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

