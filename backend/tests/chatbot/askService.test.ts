import { describe, expect, it } from "vitest";

import { electionCountdownAnswer } from "../../src/chatbot/askService.js";

// Pure pieces of the ask service; the full pipeline is exercised by
// `npm run chatbot:eval` against the live index.
//
// All instants are explicit UTC (Date.UTC) and the function itself only uses
// UTC accessors, so these tests pass identically on any machine timezone.

describe("electionCountdownAnswer", () => {
  it("counts calendar days to November 3, 2026", () => {
    expect(electionCountdownAnswer(new Date(Date.UTC(2026, 7, 12, 12, 0)))).toBe(
      "The November 2026 general election is on Tuesday, November 3, 2026 — 83 days from today."
    );
  });

  it("stays on 'tomorrow' just after midnight UTC (US clocks are still on November 2)", () => {
    // 2026-11-03 00:30 UTC = Nov 2, 7:30pm Eastern — saying "today" here
    // would be wrong for every US timezone.
    expect(electionCountdownAnswer(new Date(Date.UTC(2026, 10, 3, 0, 30)))).toBe(
      "The November 2026 general election is tomorrow: Tuesday, November 3, 2026."
    );
    // 2026-11-02 05:00 UTC = Nov 2, midnight Eastern — the day before.
    expect(electionCountdownAnswer(new Date(Date.UTC(2026, 10, 2, 5, 0)))).toBe(
      "The November 2026 general election is tomorrow: Tuesday, November 3, 2026."
    );
  });

  it("flips to 'today' once the US Eastern date is November 3", () => {
    // 2026-11-03 06:00 UTC = 1:00am Eastern on election day.
    expect(electionCountdownAnswer(new Date(Date.UTC(2026, 10, 3, 6, 0)))).toBe(
      "The November 2026 general election is today, Tuesday, November 3, 2026!"
    );
    // Late election night: 11:00pm Eastern is still November 3.
    expect(electionCountdownAnswer(new Date(Date.UTC(2026, 10, 4, 4, 0)))).toBe(
      "The November 2026 general election is today, Tuesday, November 3, 2026!"
    );
  });

  it("uses the past tense after election day", () => {
    expect(electionCountdownAnswer(new Date(Date.UTC(2026, 10, 4, 12, 0)))).toBe(
      "The November 2026 general election was on Tuesday, November 3, 2026."
    );
  });
});
