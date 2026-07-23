import {
  FRONTIER_AI_CANDIDATES,
  type AiCandidate,
} from "./aiCandidates.js";
import { callResearchProvider, trimDebugText } from "./researchProviderClient.js";
import type { AiProvider } from "./types.js";
import {
  buildPlainLanguageRewritePrompt,
  type PlainLanguageRewritePromptInput,
} from "./providers/plainLanguageRewritePrompt.js";
import {
  buildPlainLanguageRewriteVerifyPrompt,
  type PlainLanguageRewriteVerifyPromptInput,
} from "./providers/plainLanguageRewriteVerifyPrompt.js";

export type PlainLanguageAiConfig = {
  timeoutMs: number;
  openAiApiKey?: string;
  anthropicApiKey?: string;
  geminiApiKey?: string;
};

export type PlainLanguageRewriteResult =
  | { ok: true; provider: AiProvider; model: string; rewrittenText: string }
  | { ok: false; reason: string };

export type PlainLanguageVerifyResult =
  | { ok: true; provider: AiProvider; model: string; verdict: "same_facts" | "mismatch"; reason: string | null }
  | { ok: false; reason: string };

export function parseRewritePayload(payload: unknown): string {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    throw new Error("Expected JSON object");
  }
  const rewrittenText = (payload as { rewritten_text?: unknown }).rewritten_text;
  if (typeof rewrittenText !== "string" || rewrittenText.trim().length === 0) {
    throw new Error("Expected non-empty string field: rewritten_text");
  }
  return rewrittenText.trim();
}

export function parseVerifyPayload(payload: unknown): { verdict: "same_facts" | "mismatch"; reason: string | null } {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    throw new Error("Expected JSON object");
  }
  const record = payload as { verdict?: unknown; reason?: unknown };
  const verdict = typeof record.verdict === "string" ? record.verdict.trim().toLowerCase() : "";
  if (verdict !== "same_facts" && verdict !== "mismatch") {
    throw new Error(`Expected verdict of same_facts or mismatch, got: ${verdict || "(missing)"}`);
  }
  const reason = typeof record.reason === "string" && record.reason.trim().length > 0 ? record.reason.trim() : null;
  if (verdict === "mismatch" && reason === null) {
    throw new Error("mismatch verdict requires a reason");
  }
  return { verdict, reason };
}

async function callFirstWorkingProvider<T>(
  candidates: readonly AiCandidate[],
  prompt: string,
  config: PlainLanguageAiConfig,
  parse: (payload: unknown) => T
): Promise<{ ok: true; provider: AiProvider; model: string; value: T } | { ok: false; reason: string }> {
  let lastReason = "No providers were configured";
  for (const candidate of candidates) {
    const providerResult = await callResearchProvider(candidate, prompt, {
      timeoutMs: config.timeoutMs,
      openAiApiKey: config.openAiApiKey,
      anthropicApiKey: config.anthropicApiKey,
      geminiApiKey: config.geminiApiKey,
      geminiApiVersion: "v1",
    });
    if (!providerResult.ok) {
      // Provider-local failures (missing key, rate limit) fall through to the
      // next candidate, matching the other non-research AI callers.
      lastReason = providerResult.reason;
      continue;
    }
    try {
      return { ok: true, provider: candidate.provider, model: candidate.model, value: parse(providerResult.parsed) };
    } catch (error) {
      lastReason = `${candidate.provider}/${candidate.model} schema mismatch: ${
        error instanceof Error ? error.message : String(error)
      } raw=${trimDebugText(providerResult.rawText)}`;
    }
  }
  return { ok: false, reason: lastReason };
}

export async function rewriteToPlainLanguage(
  input: PlainLanguageRewritePromptInput,
  config: PlainLanguageAiConfig,
  candidates: readonly AiCandidate[] = FRONTIER_AI_CANDIDATES
): Promise<PlainLanguageRewriteResult> {
  const result = await callFirstWorkingProvider(
    candidates,
    buildPlainLanguageRewritePrompt(input),
    config,
    parseRewritePayload
  );
  return result.ok
    ? { ok: true, provider: result.provider, model: result.model, rewrittenText: result.value }
    : result;
}

export async function verifyPlainLanguageRewrite(
  input: PlainLanguageRewriteVerifyPromptInput,
  config: PlainLanguageAiConfig,
  rewriterProvider: AiProvider,
  candidates: readonly AiCandidate[] = FRONTIER_AI_CANDIDATES
): Promise<PlainLanguageVerifyResult> {
  // Rewriter and verifier share the global frontier chain. Enforce
  // independence here: never verify with the rewriter's provider, and fail
  // closed (abort, retry on resume) when no other provider is configured.
  const independentCandidates = candidates.filter((candidate) => candidate.provider !== rewriterProvider);
  if (independentCandidates.length === 0) {
    return {
      ok: false,
      reason: `no verifier provider independent of rewriter provider ${rewriterProvider} is configured`,
    };
  }
  const result = await callFirstWorkingProvider(
    independentCandidates,
    buildPlainLanguageRewriteVerifyPrompt(input),
    config,
    parseVerifyPayload
  );
  return result.ok
    ? { ok: true, provider: result.provider, model: result.model, verdict: result.value.verdict, reason: result.value.reason }
    : result;
}
