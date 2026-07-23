import { afterEach, describe, expect, it, vi } from "vitest";

import { FRONTIER_AI_CANDIDATES } from "../../src/ai/aiCandidates.ts";
import { claudeProvider } from "../../src/ai/providers/claudeProvider.ts";
import { geminiProvider } from "../../src/ai/providers/geminiProvider.ts";
import type { EnrichStateResourcesInput } from "../../src/ai/types.ts";

const originalFetch = globalThis.fetch;

const input: EnrichStateResourcesInput = {
  ingestKey: "state_resources:06:2026",
  draft: {
    state_fips: "06",
    state_abbreviation: "CA",
    state_name: "California",
    population_estimate: 39_538_223,
    census_source_url: "https://api.census.gov/data/2024/acs/acs5?get=NAME&for=state:*",
    state_abbreviation_reference_url: "https://pe.usps.com/text/pub28/28apb.htm",
    seed_sources: [],
  },
  evidence: [],
  promptVersion: "state_resources_v1",
};

afterEach(() => {
  globalThis.fetch = originalFetch;
  vi.restoreAllMocks();
});

describe("frontier provider request compatibility", () => {
  it("omits sampling parameters rejected by frontier Claude models", async () => {
    let requestBodyText = "";
    globalThis.fetch = vi.fn(async (_url: RequestInfo | URL, init?: RequestInit) => {
      requestBodyText = typeof init?.body === "string" ? init.body : "";
      return new Response(
        JSON.stringify({ content: [{ type: "text", text: "{}" }] }),
        { status: 200, headers: { "content-type": "application/json" } }
      );
    }) as unknown as typeof fetch;

    const candidate = FRONTIER_AI_CANDIDATES[0];
    const result = await claudeProvider(input, {
      ...candidate,
      timeoutMs: 1_000,
      anthropicApiKey: "test-key",
    });

    expect(result.ok).toBe(true);
    const body = JSON.parse(requestBodyText) as Record<string, unknown>;
    expect(body.model).toBe(candidate.model);
    expect(body).not.toHaveProperty("temperature");
    expect(body).not.toHaveProperty("top_p");
    expect(body).not.toHaveProperty("top_k");
  });

  it("omits sampling parameters that moving Gemini aliases may reject", async () => {
    let requestBodyText = "";
    globalThis.fetch = vi.fn(async (_url: RequestInfo | URL, init?: RequestInit) => {
      requestBodyText = typeof init?.body === "string" ? init.body : "";
      return new Response(
        JSON.stringify({ candidates: [{ content: { parts: [{ text: "{}" }] } }] }),
        { status: 200, headers: { "content-type": "application/json" } }
      );
    }) as unknown as typeof fetch;

    const candidate = FRONTIER_AI_CANDIDATES[2];
    const result = await geminiProvider(input, {
      ...candidate,
      timeoutMs: 1_000,
      geminiApiKey: "test-key",
    });

    expect(result.ok).toBe(true);
    const body = JSON.parse(requestBodyText) as Record<string, unknown>;
    expect(body).not.toHaveProperty("generationConfig");
  });
});
