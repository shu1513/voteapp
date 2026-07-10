import { describe, expect, it } from "vitest";

import {
  parseCandidateRecordDiscoveryPayload,
  parseCandidateRecordDiscoveryPayloadPartial,
} from "../../src/contracts/candidateRecordDiscoveryPayloadContract.js";

describe("parseCandidateRecordDiscoveryPayload", () => {
  it("parses valid payload rows with URL and date normalization", () => {
    const parsed = parseCandidateRecordDiscoveryPayload({
      records: [
        {
          title: "Sponsored Bill 101",
          description: "Sponsored a bill on public transit funding.",
          source_url: "HTTPS://example.org/news/story/",
          event_date: "2026-04-05T12:00:00.000Z",
        },
      ],
    });

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }
    expect(parsed.payload.records).toEqual([
      {
        description: "Sponsored a bill on public transit funding.",
        source_url: "https://example.org/news/story",
        event_date: "2026-04-05",
      },
    ]);
  });

  it("parses natural-language event_date into YYYY-MM-DD without UTC slicing", () => {
    const parsed = parseCandidateRecordDiscoveryPayload({
      records: [
        {
          description: "Candidate hosted a town hall.",
          source_url: "https://example.org/townhall",
          event_date: "April 5, 2026",
        },
      ],
    });

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }
    expect(parsed.payload.records[0]?.event_date).toBe("2026-04-05");
  });

  it("rejects malformed row fields", () => {
    const parsed = parseCandidateRecordDiscoveryPayload({
      records: [
        {
          description: "",
          source_url: "https://example.org",
          event_date: "2026-04-05",
        },
      ],
    });

    expect(parsed.ok).toBe(false);
  });

  it("dedupes duplicate rows by normalized source/date/description", () => {
    const parsed = parseCandidateRecordDiscoveryPayload({
      records: [
        {
          description: "Town hall on housing.",
          source_url: "https://example.org/a/",
          event_date: "2026-01-01",
        },
        {
          description: "town hall on housing.",
          source_url: "https://example.org/a",
          event_date: "2026-01-01",
        },
      ],
    });

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }
    expect(parsed.payload.records).toHaveLength(1);
  });

  // Dates computed relative to now so these tests never age into failures.
  function utcDatePlusDays(days: number): string {
    const date = new Date();
    date.setUTCDate(date.getUTCDate() + days);
    return date.toISOString().slice(0, 10);
  }

  it("rejects a future event_date and says why in the reason", () => {
    const parsed = parseCandidateRecordDiscoveryPayload({
      records: [
        {
          description: "Will host a town hall next week.",
          source_url: "https://example.org/upcoming",
          event_date: utcDatePlusDays(10),
        },
      ],
    });

    expect(parsed.ok).toBe(false);
    if (!parsed.ok) {
      expect(parsed.reason).toContain("payload.records contains invalid row");
      expect(parsed.reason).toContain("records[0]");
      expect(parsed.reason).toContain("is in the future");
    }
  });

  it("accepts event_date of today and the one-day timezone grace, rejects beyond it", () => {
    const today = parseCandidateRecordDiscoveryPayload({
      records: [
        {
          description: "Voted on final passage today.",
          source_url: "https://example.org/vote",
          event_date: utcDatePlusDays(0),
        },
      ],
    });
    expect(today.ok).toBe(true);

    const grace = parseCandidateRecordDiscoveryPayload({
      records: [
        {
          description: "Same-day action reported from a timezone ahead of UTC.",
          source_url: "https://example.org/vote-tz",
          event_date: utcDatePlusDays(1),
        },
      ],
    });
    expect(grace.ok).toBe(true);

    const beyond = parseCandidateRecordDiscoveryPayload({
      records: [
        {
          description: "Two days out is always a future event.",
          source_url: "https://example.org/vote-future",
          event_date: utcDatePlusDays(2),
        },
      ],
    });
    expect(beyond.ok).toBe(false);
  });

  // Timestamps name an instant; which calendar date it falls on depends on
  // timezone. The contract must return the date the string itself states —
  // never local server components, which turn "…T00:00:00Z" into the prior
  // day under TZ=America/Los_Angeles, and never the UTC date, which shifts
  // "…T23:30:00-07:00" to the next day.
  it("takes the stated date from timestamps regardless of server timezone", () => {
    const parsed = parseCandidateRecordDiscoveryPayload({
      records: [
        {
          description: "Vote recorded at UTC midnight.",
          source_url: "https://example.org/utc-midnight",
          event_date: "2026-04-05T00:00:00.000Z",
        },
        {
          description: "Late-evening action in a timezone behind UTC.",
          source_url: "https://example.org/late-evening",
          event_date: "2026-04-05T23:30:00-07:00",
        },
      ],
    });

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }
    expect(parsed.payload.records.map((record) => record.event_date)).toEqual([
      "2026-04-05",
      "2026-04-05",
    ]);
  });

  // Regression: year-only/year-month strings parse as UTC midnight in
  // new Date(), so the local-component fallback shifted "2025" to 2024-12-31
  // in timezones behind UTC (e.g. TZ=America/Los_Angeles), and other partial
  // spellings hit the legacy parser and silently invent day 01. All partial
  // dates must be rejected outright, whatever the spelling.
  it("rejects partial event_date spellings instead of shifting or inventing a day", () => {
    for (const eventDate of ["2025", "2025-04", "2025-4", "2025/04", "April 2025"]) {
      const parsed = parseCandidateRecordDiscoveryPayload({
        records: [
          {
            description: "Record reported with only a partial date.",
            source_url: "https://example.org/partial-date",
            event_date: eventDate,
          },
        ],
      });

      expect(parsed.ok).toBe(false);
      if (!parsed.ok) {
        expect(parsed.reason).toContain(`event_date "${eventDate}" is incomplete`);
        expect(parsed.reason).toContain("publication date");
      }
    }
  });

  it("rejects an impossible calendar date that matches the YYYY-MM-DD format", () => {
    const parsed = parseCandidateRecordDiscoveryPayload({
      records: [
        {
          description: "Signed the bill on a day that does not exist.",
          source_url: "https://example.org/feb31",
          event_date: "2026-02-31",
        },
      ],
    });

    expect(parsed.ok).toBe(false);
    if (!parsed.ok) {
      expect(parsed.reason).toContain("event_date 2026-02-31 is not a real calendar date");
    }
  });

  it("reports future event_date as an invalid row in the partial parser", () => {
    const parsed = parseCandidateRecordDiscoveryPayloadPartial({
      records: [
        {
          description: "Valid past action.",
          source_url: "https://example.org/past",
          event_date: "2026-01-15",
        },
        {
          description: "Invented future date.",
          source_url: "https://example.org/future",
          event_date: utcDatePlusDays(30),
        },
      ],
    });

    expect(parsed.ok).toBe(true);
    if (!parsed.ok) {
      return;
    }
    expect(parsed.payload.records).toHaveLength(1);
    expect(parsed.invalid_rows).toHaveLength(1);
    expect(parsed.invalid_rows[0]?.reason).toContain("is in the future");
  });
});
