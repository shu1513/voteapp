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
 * Curated election-result AI candidates.
 * Kept separate because result search has stricter source-authority rules than election discovery.
 */
export const ELECTION_RESULTS_AI_CANDIDATES: readonly AiCandidate[] = [
  { provider: "claude", model: "claude-sonnet-4-6" },
  { provider: "openai", model: "gpt-5.5" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const;

/**
 * Curated presidential primary-date AI candidates.
 * This is official-source web research, so keep the order aligned with election-result searches.
 */
export const PRESIDENTIAL_PRIMARY_DATE_AI_CANDIDATES: readonly AiCandidate[] = [
  { provider: "claude", model: "claude-sonnet-4-6" },
  { provider: "openai", model: "gpt-5.5" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const;

/**
 * Curated presidential candidate roster AI candidates.
 * This is national roster/status web research with FEC-sensitive matching, so keep it separate from local candidate flows.
 */
export const PRESIDENTIAL_ROSTER_AI_CANDIDATES: readonly AiCandidate[] = [
  { provider: "claude", model: "claude-opus-4-8" },
  { provider: "openai", model: "gpt-5.5" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const;

/**
 * Curated presidential nominee AI candidates.
 * Nominee detection has higher consequence than roster refreshes, so keep it separately tunable.
 */
export const PRESIDENTIAL_NOMINEE_AI_CANDIDATES: readonly AiCandidate[] = [
  { provider: "claude", model: "claude-opus-4-8" },
  { provider: "openai", model: "gpt-5.5" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const;

/**
 * Curated presidential candidate profile AI candidates.
 * Presidential profile work is national identity research, so keep it tuned separately from local profile enrichment.
 */
export const PRESIDENTIAL_PROFILE_AI_CANDIDATES: readonly AiCandidate[] = [
  { provider: "claude", model: "claude-sonnet-4-6" },
  { provider: "openai", model: "gpt-5.5" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const;

/**
 * Curated presidential candidate-record discovery AI candidates.
 * Kept separate so presidential/vice-presidential records can use stronger national-office tuning.
 */
export const PRESIDENTIAL_CANDIDATE_RECORD_DISCOVERY_AI_CANDIDATES: readonly AiCandidate[] = [
  { provider: "claude", model: "claude-opus-4-8" },
  { provider: "openai", model: "gpt-5.5" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const;

/**
 * Curated presidential candidate-record area-labeling AI candidates.
 * This is the second candidate-record AI pass, so keep Sonnet first for presidential/VP records.
 */
export const PRESIDENTIAL_CANDIDATE_RECORD_AREA_LABEL_AI_CANDIDATES: readonly AiCandidate[] = [
  { provider: "claude", model: "claude-sonnet-4-6" },
  { provider: "openai", model: "gpt-5.5" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const;

/**
 * Curated candidate-discovery/profile AI candidates.
 * Used by candidate roster/profile enrichers.
 */
export const CANDIDATES_AI_CANDIDATES: readonly AiCandidate[] = [
  { provider: "claude", model: "claude-sonnet-4-6" },
  { provider: "openai", model: "gpt-5.4-mini" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const;

/**
 * Curated candidate-record discovery AI candidates.
 * Kept separate from roster/profile to allow independent tuning.
 */
export const CANDIDATE_RECORD_DISCOVERY_AI_CANDIDATES: readonly AiCandidate[] = [
  { provider: "claude", model: "claude-sonnet-4-6" },
  { provider: "openai", model: "gpt-5.4-mini" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const;

/**
 * Curated candidate-record area-labeling AI candidates.
 * Kept separate from discovery to allow independent tuning.
 */
export const CANDIDATE_RECORD_AREA_LABEL_AI_CANDIDATES: readonly AiCandidate[] = [
  { provider: "gemini", model: "gemini-2.5-flash-lite" },
  { provider: "claude", model: "claude-sonnet-4-6" },
  { provider: "openai", model: "gpt-5.4-mini" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const;

/**
 * Curated finance industry classification AI candidates.
 * This is narrow taxonomy classification, not web research; keep it separate and easy to tune.
 */
export const FINANCE_INDUSTRY_CLASSIFICATION_AI_CANDIDATES: readonly AiCandidate[] = [
  { provider: "claude", model: "claude-sonnet-4-6" },
  { provider: "openai", model: "gpt-5.4-mini" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const;

/**
 * Plain-language rewrite (Phase 2 backfill) AI candidates. Text-to-text
 * rewriting with no web research. The rewriter leads with a mid-tier model on
 * purpose: live sampling showed gemini-2.5-flash-lite kept violating the
 * style constraints (reader-as-actor phrasing, vague simplifications, choppy
 * padding) that claude-sonnet-4-6 follows. The verifier chain deliberately
 * leads with a different provider, and verifyPlainLanguageRewrite excludes
 * the rewriter's provider at call time: the fact-consistency check is only
 * independent when a different model family judges the rewrite.
 */
export const PLAIN_LANGUAGE_REWRITE_AI_CANDIDATES: readonly AiCandidate[] = [
  { provider: "claude", model: "claude-sonnet-4-6" },
  { provider: "openai", model: "gpt-5.4-mini" },
  { provider: "gemini", model: "gemini-2.5-pro" },
] as const;

export const PLAIN_LANGUAGE_REWRITE_VERIFY_AI_CANDIDATES: readonly AiCandidate[] = [
  { provider: "openai", model: "gpt-5.4-mini" },
  { provider: "gemini", model: "gemini-2.5-pro" },
  { provider: "claude", model: "claude-sonnet-4-6" },
] as const;

export const DEFAULT_AI_CANDIDATE: AiCandidate = STATE_RESOURCES_AI_CANDIDATES[0];
