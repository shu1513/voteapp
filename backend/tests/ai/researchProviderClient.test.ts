import { afterEach, describe, expect, it, vi } from "vitest";

import { callResearchProvider, hasProviderApiKey } from "../../src/ai/researchProviderClient.ts";
import { FRONTIER_AI_CANDIDATES, type AiCandidate } from "../../src/ai/aiCandidates.ts";

const originalFetch = globalThis.fetch;

afterEach(() => {
  globalThis.fetch = originalFetch;
  vi.restoreAllMocks();
});

function jsonResponse(body: unknown, init?: ResponseInit): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "content-type": "application/json" },
    ...(init ?? {}),
  });
}

function frontierCandidate(provider: AiCandidate["provider"]): AiCandidate {
  const candidate = FRONTIER_AI_CANDIDATES.find((entry) => entry.provider === provider);
  if (!candidate) {
    throw new Error(`Missing frontier AI candidate for ${provider}`);
  }
  return candidate;
}

describe("researchProviderClient", () => {
  it("builds OpenAI Responses web-search request shape", async () => {
    let requestUrl = "";
    let requestBodyText = "";
    globalThis.fetch = vi.fn(async (url: RequestInfo | URL, init?: RequestInit) => {
      requestUrl = String(url);
      requestBodyText = typeof init?.body === "string" ? init.body : "";
      return jsonResponse({
        output_text: "{\"ok\":true}",
        output: [
          {
            type: "web_search_call",
            action: {
              sources: [
                { url: "https://example.com/source-1", title: "S1" },
                { url: "https://example.com/source-2", title: "S2" },
              ],
            },
          },
        ],
      });
    }) as unknown as typeof fetch;

    const candidate = frontierCandidate("openai");
    const result = await callResearchProvider(candidate, "test prompt", {
      timeoutMs: 1_000,
      openAiApiKey: "test-openai-key",
    });

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.parsed).toEqual({ ok: true });
      expect(result.debugMeta?.openai_api_mode).toBe("responses_web_search");
      expect(result.debugMeta?.web_search_urls_count).toBe(2);
      expect(result.debugMeta?.web_search_sources_count).toBe(2);
    }
    expect(requestUrl).toBe("https://api.openai.com/v1/responses");
    const body = JSON.parse(requestBodyText) as Record<string, unknown>;
    expect(body.tools).toEqual([{ type: "web_search" }]);
    expect(body.tool_choice).toBe("auto");
    expect(body.include).toEqual(["web_search_call.action.sources"]);
    // gpt-5 models should omit explicit temperature.
    expect(body).not.toHaveProperty("temperature");
  });

  it("builds Claude web-search request shape", async () => {
    let requestUrl = "";
    let requestBodyText = "";
    let requestHeaders: Headers | undefined;
    globalThis.fetch = vi.fn(async (url: RequestInfo | URL, init?: RequestInit) => {
      requestUrl = String(url);
      requestBodyText = typeof init?.body === "string" ? init.body : "";
      requestHeaders = new Headers(init?.headers);
      return jsonResponse({
        content: [
          {
            type: "text",
            text: "{\"ok\":true}",
          },
          {
            type: "web_search_tool_result",
            content: [{ url: "https://example.org/claude-source" }],
          },
        ],
      });
    }) as unknown as typeof fetch;

    const candidate = frontierCandidate("claude");
    const result = await callResearchProvider(candidate, "claude prompt", {
      timeoutMs: 1_000,
      anthropicApiKey: "test-anthropic-key",
      anthropicWebSearchMaxUses: 5,
      claudeInterCallDelayMs: 0,
    });

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.parsed).toEqual({ ok: true });
      expect(result.debugMeta?.web_search_urls_count).toBe(1);
    }
    expect(requestUrl).toBe("https://api.anthropic.com/v1/messages");
    const body = JSON.parse(requestBodyText) as Record<string, unknown>;
    expect(body.tools).toEqual([
      {
        type: "web_search_20250305",
        name: "web_search",
        max_uses: 5,
      },
    ]);
    expect(requestHeaders?.get("anthropic-beta")).toBe("web-search-2025-03-05");
    expect(body).not.toHaveProperty("temperature");
  });

  it("builds Gemini request shape with v1beta + json mime when configured", async () => {
    let requestUrl = "";
    let requestBodyText = "";
    globalThis.fetch = vi.fn(async (url: RequestInfo | URL, init?: RequestInit) => {
      requestUrl = String(url);
      requestBodyText = typeof init?.body === "string" ? init.body : "";
      return jsonResponse({
        candidates: [{ content: { parts: [{ text: "{\"ok\":true}" }] } }],
      });
    }) as unknown as typeof fetch;

    const candidate = frontierCandidate("gemini");
    const result = await callResearchProvider(candidate, "gemini prompt", {
      timeoutMs: 1_000,
      geminiApiKey: "test-gemini-key",
      geminiApiVersion: "v1beta",
      geminiResponseMimeTypeJson: true,
    });

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.parsed).toEqual({ ok: true });
    }
    expect(requestUrl).toContain(`/v1beta/models/${candidate.model}:generateContent?key=`);
    const body = JSON.parse(requestBodyText) as Record<string, unknown>;
    expect(body.contents).toEqual([{ role: "user", parts: [{ text: "gemini prompt" }] }]);
    expect(body.generationConfig).toEqual({
      responseMimeType: "application/json",
    });
  });

  it("returns CONFIGURATION_ERROR when provider API key is missing", async () => {
    const candidate = frontierCandidate("openai");
    const result = await callResearchProvider(candidate, "prompt", {
      timeoutMs: 1_000,
    });

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.retryable).toBe(false);
      expect(result.errorCode).toBe("CONFIGURATION_ERROR");
    }
  });

  it("classifies AbortError as TIMEOUT", async () => {
    const abortError = new Error("request aborted");
    (abortError as Error & { name: string }).name = "AbortError";
    globalThis.fetch = vi.fn(async () => {
      throw abortError;
    }) as unknown as typeof fetch;

    const candidate = frontierCandidate("openai");
    const result = await callResearchProvider(candidate, "prompt", {
      timeoutMs: 1_000,
      openAiApiKey: "test-openai-key",
    });

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.retryable).toBe(true);
      expect(result.errorCode).toBe("TIMEOUT");
      expect(result.reason).toContain("timed out");
    }
  });

  it("reports provider api-key presence accurately", () => {
    expect(
      hasProviderApiKey("openai", { timeoutMs: 1_000, openAiApiKey: "x" })
    ).toBe(true);
    expect(
      hasProviderApiKey("claude", { timeoutMs: 1_000, anthropicApiKey: "" })
    ).toBe(false);
    expect(
      hasProviderApiKey("gemini", { timeoutMs: 1_000, geminiApiKey: "x" })
    ).toBe(true);
  });
});
