/**
 * Global kill-switch for outbound AI provider HTTP calls (OpenAI, Anthropic,
 * Gemini). Calls are DENIED by default so no scheduler, worker, or pipeline
 * can spend provider credits unattended. An intentional manual run opts in by
 * setting the env var on the command line, e.g.:
 *
 *   AI_API_CALLS_ALLOWED=true npm run elections:enrich
 *
 * Do not put AI_API_CALLS_ALLOWED=true in backend/.env — that would re-enable
 * every automatic caller and defeat the guard.
 */
export function isAiApiCallAllowed(): boolean {
  return process.env.AI_API_CALLS_ALLOWED === "true";
}

export const AI_CALLS_BLOCKED_REASON =
  "AI API calls are disabled by default. Set AI_API_CALLS_ALLOWED=true on the command line of an intentional manual run (never in .env).";
