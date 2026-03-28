import { describe, expect, it } from "vitest";

import { buildRetryFeedbackPromptLines, normalizeRetryFeedback } from "../../src/ai/retryFeedback.ts";

describe("normalizeRetryFeedback", () => {
  it("returns null for unusable input", () => {
    expect(normalizeRetryFeedback(null)).toBeNull();
    expect(normalizeRetryFeedback("bad")).toBeNull();
    expect(normalizeRetryFeedback({})).toBeNull();
  });

  it("normalizes retry feedback with deduped URLs", () => {
    const normalized = normalizeRetryFeedback({
      previousFailureReason: "  citation url failed  ",
      failedCitationUrls: [
        "https://example.com/a",
        "https://example.com/a",
        " https://example.com/b ",
      ],
      retryCount: 2.8,
      failedAt: " 2026-03-27T23:00:00.000Z ",
    });

    expect(normalized).toEqual({
      previousFailureReason: "citation url failed",
      failedCitationUrls: ["https://example.com/a", "https://example.com/b"],
      retryCount: 2,
      failedAt: "2026-03-27T23:00:00.000Z",
    });
  });
});

describe("buildRetryFeedbackPromptLines", () => {
  it("returns empty lines when retry feedback is missing", () => {
    expect(buildRetryFeedbackPromptLines(null)).toEqual([]);
  });

  it("builds prompt lines with strict retry rules", () => {
    const lines = buildRetryFeedbackPromptLines({
      previousFailureReason: "sources.polling_hours citation URL could not be verified",
      failedCitationUrls: ["https://example.com/bad-link"],
      retryCount: 3,
      failedAt: "2026-03-27T23:00:00.000Z",
    });

    expect(lines.join("\n")).toContain("Previous attempt feedback (retry context):");
    expect(lines.join("\n")).toContain("https://example.com/bad-link");
    expect(lines.join("\n")).toContain("Do not reuse any URL listed in failed_citation_urls.");
  });
});
