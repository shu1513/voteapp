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
 * OpenAI's family alias follows the flagship model within that family, while
 * Google's latest alias is intentionally allowed to move between releases.
 */
export const FRONTIER_AI_CANDIDATES = [
  { provider: "claude", model: "claude-fable-5" },
  { provider: "openai", model: "gpt-5.6" },
  { provider: "gemini", model: "gemini-pro-latest" },
] as const satisfies readonly AiCandidate[];

export const DEFAULT_AI_CANDIDATE: AiCandidate = FRONTIER_AI_CANDIDATES[0];
