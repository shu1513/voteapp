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
 * it is published only on the v1beta surface, while most callers in this
 * codebase pin `geminiApiVersion: "v1"` (three — ballot measures, election
 * results, presidential primary dates — pin "v1beta" and were unaffected).
 * That mismatch made the Gemini rung return 404 "not found for API version
 * v1" in every v1 workflow, so the fallback was silently dead there. Pinning
 * a stable model id that both surfaces serve keeps every rung working.
 *
 * SCHEDULED SHUTDOWN: Google retires `gemini-2.5-pro` on 2026-10-16 (per
 * ai.google.dev/gemini-api/docs/deprecations); the named successor is
 * `gemini-3.1-pro-preview`. The pin is deliberately NOT swapped ahead of a
 * verification run: preview models are typically v1beta-only, and swapping
 * unverified would recreate the exact both-surfaces bug above for every v1
 * caller. Before the shutdown date, verify the successor (or the then-stable
 * 3.x pro id) answers on BOTH `v1` and `v1beta` with one approved live call
 * per surface (AI calls are default-deny in this codebase), then update this
 * pin. Until then, an expired pin fails loudly: provider fallback reports
 * every rung's error, so a dead Gemini rung cannot mask the others.
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
