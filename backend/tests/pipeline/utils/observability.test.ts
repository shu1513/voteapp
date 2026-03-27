import { afterEach, describe, expect, it, vi } from "vitest";

import { createStageObserver } from "../../../src/pipeline/utils/observability.js";

describe("StageObserver record metadata fallback", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("preserves explicit null metadata instead of falling back to run context", () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    const observer = createStageObserver("validator", {
      run_id: "run_ctx",
      provider: "openai",
      model: "gpt-4o-mini",
      prompt_version: "v_ctx",
    });

    observer.record({
      outcome: "skipped",
      run_id: null,
      provider: null,
      model: null,
      prompt_version: null,
    });

    const event = JSON.parse(String(logSpy.mock.calls[0]?.[0])) as Record<string, unknown>;
    expect(event.type).toBe("pipeline_event");
    expect(event.run_id).toBeNull();
    expect(event.provider).toBeNull();
    expect(event.model).toBeNull();
    expect(event.prompt_version).toBeNull();
  });

  it("falls back to run context metadata when event metadata is undefined", () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    const observer = createStageObserver("writer", {
      run_id: "run_ctx",
      provider: "claude",
      model: "claude-haiku-4-5-20251001",
      prompt_version: "v_ctx",
    });

    observer.record({
      outcome: "written",
    });

    const event = JSON.parse(String(logSpy.mock.calls[0]?.[0])) as Record<string, unknown>;
    expect(event.type).toBe("pipeline_event");
    expect(event.run_id).toBe("run_ctx");
    expect(event.provider).toBe("claude");
    expect(event.model).toBe("claude-haiku-4-5-20251001");
    expect(event.prompt_version).toBe("v_ctx");
  });
});
