import { afterEach, describe, expect, it, vi } from "vitest";

import { AI_CALLS_BLOCKED_REASON, isAiApiCallAllowed } from "../../src/ai/aiCallGuard.ts";
import { callResearchProvider } from "../../src/ai/researchProviderClient.ts";
import { claudeProvider } from "../../src/ai/providers/claudeProvider.ts";
import { openAiProvider } from "../../src/ai/providers/openaiProvider.ts";
import { geminiProvider } from "../../src/ai/providers/geminiProvider.ts";

const originalFetch = globalThis.fetch;

afterEach(() => {
  globalThis.fetch = originalFetch;
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

function buildStateResourcesInput() {
  return {
    ingestKey: "state_resources:06:2026",
    draft: {
      state_fips: "06",
      state_abbreviation: "CA",
      state_name: "California",
      population_estimate: 39_538_223,
      census_source_url: "https://api.census.gov/data/2024/acs/acs5?get=NAME,B01001_001E&for=state:*",
      state_abbreviation_reference_url: "https://pe.usps.com/text/pub28/28apb.htm",
      seed_sources: ["https://www.vote.org/polling-place-locator/"],
    },
    evidence: [{ url: "https://www.vote.org/polling-place-locator/", title: "Vote.org", snippet: "CA polling place" }],
    promptVersion: "state_resources_v1",
    promptVariant: "default" as const,
  };
}

const stateResourcesConfig = {
  timeoutMs: 1000,
  model: "test-model",
  openAiApiKey: "test-openai-key",
  anthropicApiKey: "test-anthropic-key",
  geminiApiKey: "test-gemini-key",
} as never;

describe("isAiApiCallAllowed", () => {
  it("is false unless AI_API_CALLS_ALLOWED is exactly 'true'", () => {
    vi.stubEnv("AI_API_CALLS_ALLOWED", "");
    expect(isAiApiCallAllowed()).toBe(false);

    vi.stubEnv("AI_API_CALLS_ALLOWED", "1");
    expect(isAiApiCallAllowed()).toBe(false);

    vi.stubEnv("AI_API_CALLS_ALLOWED", "TRUE");
    expect(isAiApiCallAllowed()).toBe(false);

    vi.stubEnv("AI_API_CALLS_ALLOWED", "true");
    expect(isAiApiCallAllowed()).toBe(true);
  });
});

describe("callResearchProvider guard", () => {
  it("blocks the call without touching fetch when AI calls are not allowed", async () => {
    vi.stubEnv("AI_API_CALLS_ALLOWED", "");
    const fetchSpy = vi.fn();
    globalThis.fetch = fetchSpy as never;

    const result = await callResearchProvider(
      { provider: "openai", model: "gpt-5.5" },
      "test prompt",
      { timeoutMs: 1000, openAiApiKey: "test-openai-key" }
    );

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.retryable).toBe(false);
      expect(result.errorCode).toBe("CONFIGURATION_ERROR");
      expect(result.reason).toBe(AI_CALLS_BLOCKED_REASON);
    }
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("proceeds to the provider call when AI calls are allowed", async () => {
    const fetchSpy = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          output: [
            {
              type: "message",
              content: [{ type: "output_text", text: JSON.stringify({ ok: true }) }],
            },
          ],
        }),
        { status: 200 }
      )
    );
    globalThis.fetch = fetchSpy as never;

    const result = await callResearchProvider(
      { provider: "openai", model: "gpt-5.5" },
      "test prompt",
      { timeoutMs: 1000, openAiApiKey: "test-openai-key" }
    );

    expect(fetchSpy).toHaveBeenCalledTimes(1);
    expect(result.ok).toBe(true);
  });
});

describe("state resources provider guards", () => {
  it.each([
    ["claudeProvider", claudeProvider],
    ["openAiProvider", openAiProvider],
    ["geminiProvider", geminiProvider],
  ] as const)("%s blocks the call without touching fetch when AI calls are not allowed", async (_name, provider) => {
    vi.stubEnv("AI_API_CALLS_ALLOWED", "");
    const fetchSpy = vi.fn();
    globalThis.fetch = fetchSpy as never;

    const result = await provider(buildStateResourcesInput() as never, stateResourcesConfig);

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.retryable).toBe(false);
      expect(result.errorCode).toBe("CONFIGURATION_ERROR");
      expect(result.reason).toBe(AI_CALLS_BLOCKED_REASON);
    }
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it.each([
    ["claudeProvider", claudeProvider],
    ["openAiProvider", openAiProvider],
    ["geminiProvider", geminiProvider],
  ] as const)("%s reaches fetch when AI calls are allowed", async (_name, provider) => {
    // Baseline AI_API_CALLS_ALLOWED=true comes from vitest.config.ts. Asserting
    // fetch is reached guards against the guard check drifting below fetch in a
    // future refactor, which the blocked-path test alone would not catch.
    const fetchSpy = vi.fn().mockResolvedValue(new Response(JSON.stringify({}), { status: 200 }));
    globalThis.fetch = fetchSpy as never;

    await provider(buildStateResourcesInput() as never, stateResourcesConfig);

    expect(fetchSpy).toHaveBeenCalledTimes(1);
  });
});
