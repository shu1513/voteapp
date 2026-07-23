const CLAUDE_DEFAULT_ONLY_SAMPLING_MODEL_PATTERNS = [
  /^claude-(?:fable|mythos)-\d+(?:-|$)/,
  /^claude-opus-4-(?:[7-9]|\d{2,})(?:-|$)/,
  /^claude-opus-(?:[5-9]|\d{2,})(?:-|$)/,
  /^claude-sonnet-(?:[5-9]|\d{2,})(?:-|$)/,
] as const;

/**
 * Newer Claude families reject non-default sampling values. Preserve the
 * deterministic setting for older model overrides that still support it.
 */
export function shouldSetExplicitClaudeTemperature(model: string): boolean {
  const normalizedModel = model.trim().toLowerCase();
  return !CLAUDE_DEFAULT_ONLY_SAMPLING_MODEL_PATTERNS.some((pattern) =>
    pattern.test(normalizedModel)
  );
}

/** GPT-5-family models use their default temperature behavior. */
export function shouldSetExplicitOpenAiTemperature(model: string): boolean {
  return !model.trim().toLowerCase().startsWith("gpt-5");
}
