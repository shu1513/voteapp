import { describe, expect, it } from "vitest";

import { electionCountdownAnswer } from "../../src/chatbot/askService.js";

// Pure pieces of the ask service; the full pipeline is exercised by
// `npm run chatbot:eval` against the live index.

describe("electionCountdownAnswer", () => {
  it("counts calendar days to November 3, 2026", () => {
    expect(electionCountdownAnswer(new Date(2026, 7, 12))).toBe(
      "The November 2026 general election is on Tuesday, November 3, 2026 — 83 days from today."
    );
  });

  it("says tomorrow/today at the boundary regardless of time of day", () => {
    expect(electionCountdownAnswer(new Date(2026, 10, 2, 23, 59))).toBe(
      "The November 2026 general election is tomorrow: Tuesday, November 3, 2026."
    );
    expect(electionCountdownAnswer(new Date(2026, 10, 3, 0, 1))).toBe(
      "The November 2026 general election is today, Tuesday, November 3, 2026!"
    );
  });

  it("uses the past tense after election day", () => {
    expect(electionCountdownAnswer(new Date(2026, 10, 4))).toBe(
      "The November 2026 general election was on Tuesday, November 3, 2026."
    );
  });
});
