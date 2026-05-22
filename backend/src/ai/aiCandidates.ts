import type { AiProvider } from "./types.js";

export type AiCandidate = {
  provider: AiProvider;
  model: string;
};

/**
 * Curated state-resources AI candidates that have passed live smoke checks in this project.
 * Keep this list intentionally short and explicit.
 */
export const STATE_RESOURCES_AI_CANDIDATES: readonly AiCandidate[] = [
  { provider: "gemini", model: "gemini-2.5-flash-lite" },
  { provider: "openai", model: "gpt-5.4-mini" },
  { provider: "claude", model: "claude-sonnet-4-6" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const;

/**
 * Curated elections AI candidates.
 * Kept separate from state-resources to allow independent tuning.
 */
export const ELECTIONS_AI_CANDIDATES: readonly AiCandidate[] = [
  { provider: "gemini", model: "gemini-2.5-flash-lite" },
  { provider: "claude", model: "claude-sonnet-4-6" },
  { provider: "openai", model: "gpt-5.5" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const;

/**
 * Curated ballot-measures AI candidates.
 * Kept separate from elections/state-resources to allow independent tuning.
 */
export const BALLOT_MEASURES_AI_CANDIDATES: readonly AiCandidate[] = [
  { provider: "gemini", model: "gemini-2.5-flash-lite" },
  { provider: "claude", model: "claude-sonnet-4-6" },
  { provider: "openai", model: "gpt-5.5" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const;

/**
 * Curated candidate-discovery/profile AI candidates.
 * Used by candidate roster/profile enrichers.
 */
export const CANDIDATES_AI_CANDIDATES: readonly AiCandidate[] = [
  { provider: "gemini", model: "gemini-2.5-flash-lite" },
  { provider: "claude", model: "claude-sonnet-4-6" },
  { provider: "openai", model: "gpt-5.4-mini" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const;

export const DEFAULT_AI_CANDIDATE: AiCandidate = STATE_RESOURCES_AI_CANDIDATES[0];
