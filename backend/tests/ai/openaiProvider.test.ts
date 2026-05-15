import { afterEach, describe, expect, it, vi } from "vitest";

import { openAiProvider } from "../../src/ai/providers/openaiProvider.ts";

const originalFetch = globalThis.fetch;

afterEach(() => {
  globalThis.fetch = originalFetch;
  vi.restoreAllMocks();
});

function buildInput(withRetryFeedback: boolean) {
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
      allow_open_web_research: true,
    },
    evidence: [{ url: "https://www.vote.org/polling-place-locator/", title: "Vote.org", snippet: "CA polling place" }],
    promptVersion: "state_resources_v1",
    promptVariant: "default" as const,
    ...(withRetryFeedback
      ? {
          retryFeedback: {
            previousFailureReason: "sources.polling_hours citation URL could not be verified",
            failedCitationUrls: ["https://example.gov/bad-hours-url"],
            failedCitationDetails: [
              { url: "https://example.gov/bad-hours-url", reason: "citation fetch returned status 403" },
            ],
            retryCount: 2,
            failedAt: "2026-03-27T23:00:00.000Z",
          },
        }
      : {}),
  };
}

describe("openAiProvider prompt retry feedback", () => {
  it("injects retry feedback into prompt when provided", async () => {
    let capturedBody = "";
    globalThis.fetch = vi.fn(async (_url: RequestInfo | URL, init?: RequestInit) => {
      capturedBody = typeof init?.body === "string" ? init.body : "";
      return new Response(
        JSON.stringify({
          choices: [{ message: { content: "{}" } }],
        }),
        { status: 200, headers: { "content-type": "application/json" } }
      );
    }) as unknown as typeof fetch;

    const result = await openAiProvider(buildInput(true), {
      provider: "openai",
      model: "gpt-4o-mini",
      timeoutMs: 1000,
      openAiApiKey: "test-key",
    });

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.debugMeta?.provider_prompt_has_retry_feedback).toBe(true);
      expect(String(result.debugMeta?.provider_prompt_retry_feedback_snapshot ?? "")).toContain(
        "https://example.gov/bad-hours-url"
      );
    }
    expect(capturedBody).toContain("Previous attempt feedback (retry context):");
    expect(capturedBody).toContain("https://example.gov/bad-hours-url");
    expect(capturedBody).toContain("failed_citation_details");
    expect(capturedBody).toContain("citation fetch returned status 403");
    expect(capturedBody).toContain("Do not reuse any URL listed in failed_citation_urls.");
  });

  it("uses citation-repair prompt variant on second-pass retries", async () => {
    let capturedBody = "";
    globalThis.fetch = vi.fn(async (_url: RequestInfo | URL, init?: RequestInit) => {
      capturedBody = typeof init?.body === "string" ? init.body : "";
      return new Response(
        JSON.stringify({
          choices: [{ message: { content: "{}" } }],
        }),
        { status: 200, headers: { "content-type": "application/json" } }
      );
    }) as unknown as typeof fetch;

    const input = buildInput(true);
    input.promptVariant = "citation_repair";
    const result = await openAiProvider(input, {
      provider: "openai",
      model: "gpt-4o-mini",
      timeoutMs: 1000,
      openAiApiKey: "test-key",
    });

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.debugMeta?.provider_prompt_variant).toBe("citation_repair");
    }
    expect(capturedBody).toContain("Citation-repair mode:");
    expect(capturedBody).toContain("replace broken citations only");
  });

  it("does not inject retry feedback section when absent", async () => {
    let capturedBody = "";
    globalThis.fetch = vi.fn(async (_url: RequestInfo | URL, init?: RequestInit) => {
      capturedBody = typeof init?.body === "string" ? init.body : "";
      return new Response(
        JSON.stringify({
          choices: [{ message: { content: "{}" } }],
        }),
        { status: 200, headers: { "content-type": "application/json" } }
      );
    }) as unknown as typeof fetch;

    const result = await openAiProvider(buildInput(false), {
      provider: "openai",
      model: "gpt-4o-mini",
      timeoutMs: 1000,
      openAiApiKey: "test-key",
    });

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.debugMeta?.provider_prompt_has_retry_feedback).toBe(false);
    }
    expect(capturedBody).not.toContain("Previous attempt feedback (retry context):");
  });
});

describe("openAiProvider temperature behavior", () => {
  it("omits explicit temperature for gpt-5 models", async () => {
    let capturedBody = "";
    globalThis.fetch = vi.fn(async (_url: RequestInfo | URL, init?: RequestInit) => {
      capturedBody = typeof init?.body === "string" ? init.body : "";
      return new Response(JSON.stringify({ choices: [{ message: { content: "{}" } }] }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }) as unknown as typeof fetch;

    const result = await openAiProvider(buildInput(false), {
      provider: "openai",
      model: "gpt-5.5",
      timeoutMs: 1000,
      openAiApiKey: "test-key",
    });

    expect(result.ok).toBe(true);
    const body = JSON.parse(capturedBody) as Record<string, unknown>;
    expect(body).not.toHaveProperty("temperature");
  });

  it("keeps explicit temperature for non-gpt-5 models", async () => {
    let capturedBody = "";
    globalThis.fetch = vi.fn(async (_url: RequestInfo | URL, init?: RequestInit) => {
      capturedBody = typeof init?.body === "string" ? init.body : "";
      return new Response(JSON.stringify({ choices: [{ message: { content: "{}" } }] }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }) as unknown as typeof fetch;

    const result = await openAiProvider(buildInput(false), {
      provider: "openai",
      model: "gpt-4o-mini",
      timeoutMs: 1000,
      openAiApiKey: "test-key",
    });

    expect(result.ok).toBe(true);
    const body = JSON.parse(capturedBody) as Record<string, unknown>;
    expect(body).toHaveProperty("temperature", 0);
  });
});

describe("openAiProvider scoped prompt formatting", () => {
  it("includes strict JSON-only instruction and a valid JSON example for grouped prompts", async () => {
    let capturedBody = "";
    globalThis.fetch = vi.fn(async (_url: RequestInfo | URL, init?: RequestInit) => {
      capturedBody = typeof init?.body === "string" ? init.body : "";
      return new Response(JSON.stringify({ choices: [{ message: { content: "{}" } }] }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }) as unknown as typeof fetch;

    const input = buildInput(false);
    input.fieldGroup = "online_registration";
    const result = await openAiProvider(input, {
      provider: "openai",
      model: "gpt-4o-mini",
      timeoutMs: 1000,
      openAiApiKey: "test-key",
    });

    expect(result.ok).toBe(true);
    const requestBody = JSON.parse(capturedBody) as {
      messages: Array<{ role: string; content: string }>;
    };
    const userPrompt = requestBody.messages.find((m) => m.role === "user")?.content ?? "";
    expect(userPrompt).toContain("Output must be raw JSON only (single object). No prose, no markdown, no code fences.");
    expect(userPrompt).toContain("Valid JSON example for this group:");
    expect(userPrompt).toContain("\"online_registration_available\":true");
    expect(userPrompt).toContain("\"online_registration_deadline_rule\":");
  });
});

describe("openAiProvider JSON parsing hardening", () => {
  it("accepts duplicated back-to-back identical JSON objects", async () => {
    const objectText =
      '{"online_registration_available":true,"online_registration_deadline_rule":"Online registration closes 15 days before Election Day.","sources":{"online_registration_available":["https://vote.gov/register/california"],"online_registration_deadline_rule":["https://vote.gov/register/california"]}}';

    globalThis.fetch = vi.fn(async () => {
      return new Response(
        JSON.stringify({
          choices: [{ message: { content: `${objectText}\n${objectText}` } }],
        }),
        { status: 200, headers: { "content-type": "application/json" } }
      );
    }) as unknown as typeof fetch;

    const input = buildInput(false);
    input.fieldGroup = "online_registration";
    const result = await openAiProvider(input, {
      provider: "openai",
      model: "gpt-4o-mini",
      timeoutMs: 1000,
      openAiApiKey: "test-key",
    });

    expect(result.ok).toBe(true);
  });

  it("rejects non-duplicate trailing content after the first JSON object", async () => {
    const objectText =
      '{"online_registration_available":true,"online_registration_deadline_rule":"Online registration closes 15 days before Election Day.","sources":{"online_registration_available":["https://vote.gov/register/california"],"online_registration_deadline_rule":["https://vote.gov/register/california"]}}';

    globalThis.fetch = vi.fn(async () => {
      return new Response(
        JSON.stringify({
          choices: [{ message: { content: `${objectText}\nThis is extra prose.` } }],
        }),
        { status: 200, headers: { "content-type": "application/json" } }
      );
    }) as unknown as typeof fetch;

    const input = buildInput(false);
    input.fieldGroup = "online_registration";
    const result = await openAiProvider(input, {
      provider: "openai",
      model: "gpt-4o-mini",
      timeoutMs: 1000,
      openAiApiKey: "test-key",
    });

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.errorCode).toBe("INVALID_JSON");
      expect(result.reason).toContain("trailing output");
    }
  });
});
