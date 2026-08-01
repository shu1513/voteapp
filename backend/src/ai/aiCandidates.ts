import type { AiProvider } from "./types.js";

export type AiCandidate = {
  provider: AiProvider;
  model: string;
};

/**
 * Quality-first provider fallback policy shared by every AI workflow.
 *
 * Anthropic does not offer an evergreen "best" alias, so its model ID must be
 * reviewed when Anthropic publishes a stronger generally available model.
 * OpenAI's family alias follows the flagship model within that family.
 *
 * Google's evergreen alias `gemini-pro-latest` is deliberately NOT used here:
 * it is published only on the v1beta surface, while every caller in this
 * codebase pins `geminiApiVersion: "v1"`. That mismatch made the Gemini rung
 * return 404 "not found for API version v1" in every workflow, so the fallback
 * was silently dead. Pinning a model that v1 actually serves keeps the rung
 * working; revisit when Google promotes a newer pro model to v1.
 */
export const FRONTIER_AI_CANDIDATES = [
  { provider: "claude", model: "claude-fable-5" },
  { provider: "openai", model: "gpt-5.6" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const satisfies readonly AiCandidate[];

/**
 * Quality-first environment default. Cost-sensitive deployments should set
 * both AI_PROVIDER and AI_MODEL explicitly.
 */
export const DEFAULT_AI_CANDIDATE: AiCandidate = FRONTIER_AI_CANDIDATES[0];
