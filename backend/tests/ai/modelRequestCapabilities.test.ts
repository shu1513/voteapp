import { describe, expect, it } from "vitest";

import {
  shouldSetExplicitClaudeTemperature,
  shouldSetExplicitOpenAiTemperature,
} from "../../src/ai/modelRequestCapabilities.ts";

describe("model request capabilities", () => {
  it.each([
    ["claude-fable-5", false],
    ["claude-fable-5-20260609", false],
    ["claude-mythos-5", false],
    ["claude-opus-4-7", false],
    ["claude-opus-4-8", false],
    ["claude-opus-5-0", false],
    ["claude-sonnet-5", false],
    ["claude-sonnet-4-6", true],
    ["claude-haiku-4-5", true],
    ["claude-3-5-sonnet-20241022", true],
  ])("sets Claude temperature for %s: %s", (model, expected) => {
    expect(shouldSetExplicitClaudeTemperature(model)).toBe(expected);
  });

  it.each([
    ["gpt-5.6", false],
    ["gpt-5.4-mini", false],
    ["gpt-4o-mini", true],
  ])("sets OpenAI temperature for %s: %s", (model, expected) => {
    expect(shouldSetExplicitOpenAiTemperature(model)).toBe(expected);
  });
});
