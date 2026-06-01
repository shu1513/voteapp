import { describe, expect, it } from "vitest";

import { parseCandidateRecordDiscoveryPayload } from "../../src/contracts/candidateRecordDiscoveryPayloadContract.js";

describe("parseCandidateRecordDiscoveryPayload", () => {
  it("parses valid payload rows with URL and date normalization", () => {
    const parsed = parseCandidateRecordDiscoveryPayload({
      records: [
        {
          title: "Sponsored Bill 101",
          description: "Sponsored a bill on public transit funding.",
          source_url: "HTTPS://example.org/news/story/",
          source_name: "Example News",
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
        title: "Sponsored Bill 101",
        description: "Sponsored a bill on public transit funding.",
        source_url: "https://example.org/news/story",
        source_name: "Example News",
        event_date: "2026-04-05",
      },
    ]);
  });

  it("rejects malformed row fields", () => {
    const parsed = parseCandidateRecordDiscoveryPayload({
      records: [
        {
          title: "Record",
          description: "",
          source_url: "https://example.org",
          source_name: "Example",
          event_date: "2026-04-05",
        },
      ],
    });

    expect(parsed.ok).toBe(false);
  });

  it("dedupes duplicate rows by normalized source/date/title", () => {
    const parsed = parseCandidateRecordDiscoveryPayload({
      records: [
        {
          title: "Town hall on housing",
          description: "First copy",
          source_url: "https://example.org/a/",
          source_name: "Example One",
          event_date: "2026-01-01",
        },
        {
          title: "town hall on housing",
          description: "Second copy",
          source_url: "https://example.org/a",
          source_name: "Example Two",
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
});
