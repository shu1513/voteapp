import type { AiProvider } from "./types.js";

export type AiCandidate = {
  provider: AiProvider;
  model: string;
};

/**
 * Curated default AI candidates that have passed live smoke checks in this project.
 * Keep this list intentionally short and explicit.
 */
export const AI_CANDIDATES: readonly AiCandidate[] = [
  { provider: "gemini", model: "gemini-2.5-flash-lite" },
  { provider: "openai", model: "gpt-5.4-mini" },
  { provider: "claude", model: "claude-sonnet-4-6" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const;

export const DEFAULT_AI_CANDIDATE: AiCandidate = AI_CANDIDATES[0];
