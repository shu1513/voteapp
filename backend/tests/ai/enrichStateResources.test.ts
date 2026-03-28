import { afterEach, describe, expect, it, vi } from "vitest";
import { enrichStateResources } from "../../src/ai/enrichStateResources.ts";

const originalFetch = globalThis.fetch;

afterEach(() => {
  globalThis.fetch = originalFetch;
  vi.restoreAllMocks();
});

function draft() {
  return {
    state_fips: "06",
    state_abbreviation: "CA",
    state_name: "California",
    population_estimate: 39_538_223,
    census_source_url: "https://api.census.gov/data/2024/acs/acs5?get=NAME,B01001_001E&for=state:*",
    state_abbreviation_reference_url: "https://pe.usps.com/text/pub28/28apb.htm",
    seed_sources: ["https://www.vote.org/polling-place-locator/"],
    allow_open_web_research: true,
  };
}

function floridaDraft() {
  return {
    state_fips: "12",
    state_abbreviation: "FL",
    state_name: "Florida",
    population_estimate: 22_634_867,
    census_source_url: "https://api.census.gov/data/2024/acs/acs5?get=NAME,B01001_001E&for=state:*",
    state_abbreviation_reference_url: "https://pe.usps.com/text/pub28/28apb.htm",
    seed_sources: ["https://www.vote.org/polling-place-locator/"],
    allow_open_web_research: true,
  };
}

function validPayload(overrides: Record<string, unknown> = {}) {
  return {
    state_fips: "06",
    state_abbreviation: "CA",
    state_name: "California",
    polling_place_url: "https://www.vote.org/polling-place-locator/",
    voter_registration_url: "https://www.usa.gov/register-to-vote",
    vote_by_mail_info:
      "California allows any registered voter to vote by mail, and ballots must be returned by election day under state deadline rules.",
    polling_hours:
      "California polling places are generally open from 7:00 a.m. to 8:00 p.m. on election day, with local guidance for special cases.",
    id_requirements:
      "California generally does not require voter ID at the polls, except limited first-time federal voter cases.",
    sources: {
      polling_place_url: [{ source_name: "Vote.org", source_url: "https://www.vote.org/polling-place-locator/" }],
      voter_registration_url: [{ source_name: "USA.gov", source_url: "https://www.usa.gov/register-to-vote" }],
      vote_by_mail_info: [{ source_name: "Vote.org", source_url: "https://www.vote.org/absentee-ballot/" }],
      polling_hours: [{ source_name: "NASS", source_url: "https://www.nass.org/can-i-vote" }],
      id_requirements: [{ source_name: "US Vote Foundation", source_url: "https://www.usvotefoundation.org/voter-id-laws" }],
    },
    ...overrides,
  };
}

function openAiResponse(payload: unknown): Response {
  return new Response(
    JSON.stringify({
      choices: [{ message: { content: JSON.stringify(payload) } }],
    }),
    { status: 200, headers: { "content-type": "application/json" } }
  );
}

describe("enrichStateResources", () => {
  it("fails fast on missing evidence before provider call", async () => {
    const fetchSpy = vi.fn();
    globalThis.fetch = fetchSpy as unknown as typeof fetch;

    const result = await enrichStateResources(
      {
        ingestKey: "k1",
        draft: draft(),
        evidence: [],
        promptVersion: "state_resources_v1",
      },
      {
        provider: "openai",
        model: "gpt-5-mini",
        timeoutMs: 1000,
      }
    );

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.errorCode).toBe("SCHEMA_MISMATCH");
      expect(result.reason).toContain("evidence snippets are required");
    }
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("accepts citation URLs that are valid even when not present in collected evidence", async () => {
    globalThis.fetch = vi.fn(async () => openAiResponse(validPayload())) as unknown as typeof fetch;

    const result = await enrichStateResources(
      {
        ingestKey: "k2",
        draft: draft(),
        evidence: [
          {
            url: "https://irrelevant.example.org/only-source",
            title: "Only source",
            snippet: "Evidence snippet",
          },
        ],
        promptVersion: "state_resources_v1",
      },
      {
        provider: "openai",
        model: "gpt-5-mini",
        timeoutMs: 1000,
        openAiApiKey: "test-key",
      }
    );

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.payload.sources.vote_by_mail_info[0]?.source_url).toBe("https://www.vote.org/absentee-ballot");
    }
    expect(globalThis.fetch).toHaveBeenCalled();
  });

  it("returns success when citations are grounded in evidence URLs", async () => {
    globalThis.fetch = vi.fn(async () => openAiResponse(validPayload())) as unknown as typeof fetch;

    const result = await enrichStateResources(
      {
        ingestKey: "k3",
        draft: draft(),
        evidence: [
          { url: "https://www.vote.org/polling-place-locator/", title: "Vote.org", snippet: "Polling place" },
          { url: "https://www.usa.gov/register-to-vote", title: "USA.gov", snippet: "Registration" },
          { url: "https://www.vote.org/absentee-ballot/", title: "Vote.org", snippet: "Mail vote" },
          { url: "https://www.nass.org/can-i-vote", title: "NASS", snippet: "Hours" },
          { url: "https://www.usvotefoundation.org/voter-id-laws", title: "USVF", snippet: "ID" },
        ],
        promptVersion: "state_resources_v1",
      },
      {
        provider: "openai",
        model: "gpt-5-mini",
        timeoutMs: 1000,
        openAiApiKey: "test-key",
      }
    );

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.payload.state_fips).toBe("06");
      expect(result.schemaVersion).toBe("state_resources_enrichment_v1");
    }
    expect(globalThis.fetch).toHaveBeenCalledTimes(1);
  });

  it("verifies and appends AI-cited URLs that are outside pre-collected evidence", async () => {
    const additionalCitationUrl = "https://www.sos.ca.gov/elections/voting-options/vote-mail";
    globalThis.fetch = vi.fn(async (input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url.includes("/v1/chat/completions")) {
        return openAiResponse(
          validPayload({
            sources: {
              polling_place_url: [{ source_name: "Vote.org", source_url: "https://www.vote.org/polling-place-locator/" }],
              voter_registration_url: [{ source_name: "USA.gov", source_url: "https://www.usa.gov/register-to-vote" }],
              vote_by_mail_info: [{ source_name: "California Secretary of State", source_url: additionalCitationUrl }],
              polling_hours: [{ source_name: "NASS", source_url: "https://www.nass.org/can-i-vote" }],
              id_requirements: [{ source_name: "US Vote Foundation", source_url: "https://www.usvotefoundation.org/voter-id-laws" }],
            },
          })
        );
      }

      if (url.startsWith(additionalCitationUrl)) {
        return new Response(
          "<html><head><title>California Secretary of State</title></head><body>Vote by mail deadlines and return rules.</body></html>",
          { status: 200, headers: { "content-type": "text/html; charset=utf-8" } }
        );
      }

      return new Response("not found", { status: 404, headers: { "content-type": "text/plain" } });
    }) as unknown as typeof fetch;

    const result = await enrichStateResources(
      {
        ingestKey: "k3b",
        draft: draft(),
        evidence: [
          { url: "https://www.vote.org/polling-place-locator/", title: "Vote.org", snippet: "Polling place" },
          { url: "https://www.usa.gov/register-to-vote", title: "USA.gov", snippet: "Registration" },
          { url: "https://www.vote.org/absentee-ballot/", title: "Vote.org", snippet: "Mail vote" },
          { url: "https://www.nass.org/can-i-vote", title: "NASS", snippet: "Hours" },
          { url: "https://www.usvotefoundation.org/voter-id-laws", title: "USVF", snippet: "ID" },
        ],
        promptVersion: "state_resources_v1",
      },
      {
        provider: "openai",
        model: "gpt-5-mini",
        timeoutMs: 1000,
        openAiApiKey: "test-key",
      }
    );

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.verifiedCitationEvidence.some((item) => item.url === additionalCitationUrl)).toBe(true);
    }
  });

  it("rejects when an AI-cited URL outside evidence cannot be verified", async () => {
    const additionalCitationUrl = "https://www.sos.ca.gov/elections/voting-options/vote-mail";
    globalThis.fetch = vi.fn(async (input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url.includes("/v1/chat/completions")) {
        return openAiResponse(
          validPayload({
            sources: {
              polling_place_url: [{ source_name: "Vote.org", source_url: "https://www.vote.org/polling-place-locator/" }],
              voter_registration_url: [{ source_name: "USA.gov", source_url: "https://www.usa.gov/register-to-vote" }],
              vote_by_mail_info: [{ source_name: "California Secretary of State", source_url: additionalCitationUrl }],
              polling_hours: [{ source_name: "NASS", source_url: "https://www.nass.org/can-i-vote" }],
              id_requirements: [{ source_name: "US Vote Foundation", source_url: "https://www.usvotefoundation.org/voter-id-laws" }],
            },
          })
        );
      }

      if (url.startsWith(additionalCitationUrl)) {
        return new Response("upstream unavailable", { status: 503, headers: { "content-type": "text/plain" } });
      }

      return new Response("not found", { status: 404, headers: { "content-type": "text/plain" } });
    }) as unknown as typeof fetch;

    const result = await enrichStateResources(
      {
        ingestKey: "k3c",
        draft: draft(),
        evidence: [
          { url: "https://www.vote.org/polling-place-locator/", title: "Vote.org", snippet: "Polling place" },
          { url: "https://www.usa.gov/register-to-vote", title: "USA.gov", snippet: "Registration" },
          { url: "https://www.vote.org/absentee-ballot/", title: "Vote.org", snippet: "Mail vote" },
          { url: "https://www.nass.org/can-i-vote", title: "NASS", snippet: "Hours" },
          { url: "https://www.usvotefoundation.org/voter-id-laws", title: "USVF", snippet: "ID" },
        ],
        promptVersion: "state_resources_v1",
      },
      {
        provider: "openai",
        model: "gpt-5-mini",
        timeoutMs: 1000,
        openAiApiKey: "test-key",
      }
    );

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.errorCode).toBe("SCHEMA_MISMATCH");
      expect(result.reason).toContain("could not be verified");
    }
  });

  it("captures all failed citation URLs from one enrichment attempt", async () => {
    const badMailUrl = "https://example.org/mail-info";
    const badHoursUrl = "https://example.org/polling-hours";

    globalThis.fetch = vi.fn(async (input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url.includes("/v1/chat/completions")) {
        return openAiResponse(
          validPayload({
            sources: {
              polling_place_url: [{ source_name: "Vote.org", source_url: "https://www.vote.org/polling-place-locator/" }],
              voter_registration_url: [{ source_name: "USA.gov", source_url: "https://www.usa.gov/register-to-vote" }],
              vote_by_mail_info: [{ source_name: "Mail rules", source_url: badMailUrl }],
              polling_hours: [{ source_name: "Hours rules", source_url: badHoursUrl }],
              id_requirements: [{ source_name: "US Vote Foundation", source_url: "https://www.usvotefoundation.org/voter-id-laws" }],
            },
          })
        );
      }

      if (url.startsWith(badMailUrl)) {
        return new Response("not found", { status: 404, headers: { "content-type": "text/plain" } });
      }

      if (url.startsWith(badHoursUrl)) {
        return new Response("forbidden", { status: 403, headers: { "content-type": "text/plain" } });
      }

      return new Response("not found", { status: 404, headers: { "content-type": "text/plain" } });
    }) as unknown as typeof fetch;

    const result = await enrichStateResources(
      {
        ingestKey: "k3d",
        draft: draft(),
        evidence: [
          { url: "https://www.vote.org/polling-place-locator/", title: "Vote.org", snippet: "Polling place" },
          { url: "https://www.usa.gov/register-to-vote", title: "USA.gov", snippet: "Registration" },
          { url: "https://www.vote.org/absentee-ballot/", title: "Vote.org", snippet: "Mail vote" },
          { url: "https://www.nass.org/can-i-vote", title: "NASS", snippet: "Hours" },
          { url: "https://www.usvotefoundation.org/voter-id-laws", title: "USVF", snippet: "ID" },
        ],
        promptVersion: "state_resources_v1",
      },
      {
        provider: "openai",
        model: "gpt-5-mini",
        timeoutMs: 1000,
        openAiApiKey: "test-key",
      }
    );

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.errorCode).toBe("SCHEMA_MISMATCH");
      const failureDebug = (result.failureDebug ?? {}) as Record<string, unknown>;
      const failedCitationUrls = Array.isArray(failureDebug.failed_citation_urls)
        ? (failureDebug.failed_citation_urls as unknown[])
        : [];
      expect(failedCitationUrls).toContain(badMailUrl);
      expect(failedCitationUrls).toContain(badHoursUrl);
    }
  });

  it("rejects URL-only text for vote_by_mail_info", async () => {
    globalThis.fetch = vi.fn(async () =>
      openAiResponse(
        validPayload({
          vote_by_mail_info: "https://www.vote.org/absentee-ballot/",
        })
      )
    ) as unknown as typeof fetch;

    const result = await enrichStateResources(
      {
        ingestKey: "k4",
        draft: draft(),
        evidence: [
          { url: "https://www.vote.org/polling-place-locator/", title: "Vote.org", snippet: "Polling place" },
          { url: "https://www.usa.gov/register-to-vote", title: "USA.gov", snippet: "Registration" },
          { url: "https://www.vote.org/absentee-ballot/", title: "Vote.org", snippet: "Mail vote" },
          { url: "https://www.nass.org/can-i-vote", title: "NASS", snippet: "Hours" },
          { url: "https://www.usvotefoundation.org/voter-id-laws", title: "USVF", snippet: "ID" },
        ],
        promptVersion: "state_resources_v1",
      },
      {
        provider: "openai",
        model: "gpt-5-mini",
        timeoutMs: 1000,
        openAiApiKey: "test-key",
      }
    );

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.errorCode).toBe("SCHEMA_MISMATCH");
      expect(result.reason).toContain("plain-language text");
    }
  });

  it("rejects boilerplate vote_by_mail_info text", async () => {
    globalThis.fetch = vi.fn(async () =>
      openAiResponse(
        validPayload({
          vote_by_mail_info:
            "California voters can request and return vote-by-mail ballots based on state deadlines and local election rules.",
        })
      )
    ) as unknown as typeof fetch;

    const result = await enrichStateResources(
      {
        ingestKey: "k8",
        draft: draft(),
        evidence: [
          { url: "https://www.vote.org/polling-place-locator/", title: "Vote.org", snippet: "Polling place" },
          { url: "https://www.usa.gov/register-to-vote", title: "USA.gov", snippet: "Registration" },
          { url: "https://www.vote.org/absentee-ballot/", title: "Vote.org", snippet: "Mail vote" },
          { url: "https://www.nass.org/can-i-vote", title: "NASS", snippet: "Hours" },
          { url: "https://www.usvotefoundation.org/voter-id-laws", title: "USVF", snippet: "ID" },
        ],
        promptVersion: "state_resources_v1",
      },
      {
        provider: "openai",
        model: "gpt-5-mini",
        timeoutMs: 1000,
        openAiApiKey: "test-key",
      }
    );

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.errorCode).toBe("SCHEMA_MISMATCH");
      expect(result.reason).toContain("generic boilerplate");
    }
  });

  it("rejects boilerplate id_requirements text with state-name prefix", async () => {
    globalThis.fetch = vi.fn(async () =>
      openAiResponse(
        validPayload({
          id_requirements: "California voter ID requirements depend on election type and local/state rules.",
        })
      )
    ) as unknown as typeof fetch;

    const result = await enrichStateResources(
      {
        ingestKey: "k9",
        draft: draft(),
        evidence: [
          { url: "https://www.vote.org/polling-place-locator/", title: "Vote.org", snippet: "Polling place" },
          { url: "https://www.usa.gov/register-to-vote", title: "USA.gov", snippet: "Registration" },
          { url: "https://www.vote.org/absentee-ballot/", title: "Vote.org", snippet: "Mail vote" },
          { url: "https://www.nass.org/can-i-vote", title: "NASS", snippet: "Hours" },
          { url: "https://www.usvotefoundation.org/voter-id-laws", title: "USVF", snippet: "ID" },
        ],
        promptVersion: "state_resources_v1",
      },
      {
        provider: "openai",
        model: "gpt-5-mini",
        timeoutMs: 1000,
        openAiApiKey: "test-key",
      }
    );

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.errorCode).toBe("SCHEMA_MISMATCH");
      expect(result.reason).toContain("generic boilerplate");
    }
  });

  it("prefers official polling_place_url from evidence over aggregator URL", async () => {
    const officialPollingUrl = "https://www.sos.ca.gov/elections/polling-place";
    globalThis.fetch = vi.fn(async () =>
      openAiResponse(
        validPayload({
          polling_place_url: "https://www.vote.org/polling-place-locator/",
          sources: {
            polling_place_url: [
              { source_name: "Vote.org", source_url: "https://www.vote.org/polling-place-locator/" },
            ],
            voter_registration_url: [{ source_name: "USA.gov", source_url: "https://www.usa.gov/register-to-vote" }],
            vote_by_mail_info: [{ source_name: "Vote.org", source_url: "https://www.vote.org/absentee-ballot/" }],
            polling_hours: [{ source_name: "NASS", source_url: "https://www.nass.org/can-i-vote" }],
            id_requirements: [{ source_name: "USVF", source_url: "https://www.usvotefoundation.org/voter-id-laws" }],
          },
        })
      )
    ) as unknown as typeof fetch;

    const result = await enrichStateResources(
      {
        ingestKey: "k5",
        draft: draft(),
        evidence: [
          { url: "https://www.vote.org/polling-place-locator/", title: "Vote.org", snippet: "Polling place" },
          {
            url: officialPollingUrl,
            title: "California Secretary of State",
            snippet: "Find your polling place and registration resources",
          },
          { url: "https://www.usa.gov/register-to-vote", title: "USA.gov", snippet: "Registration" },
          { url: "https://www.vote.org/absentee-ballot/", title: "Vote.org", snippet: "Mail vote" },
          { url: "https://www.nass.org/can-i-vote", title: "NASS", snippet: "Hours" },
          { url: "https://www.usvotefoundation.org/voter-id-laws", title: "USVF", snippet: "ID" },
        ],
        promptVersion: "state_resources_v1",
      },
      {
        provider: "openai",
        model: "gpt-5-mini",
        timeoutMs: 1000,
        openAiApiKey: "test-key",
      }
    );

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.payload.polling_place_url).toBe(officialPollingUrl);
      expect(result.payload.sources.polling_place_url[0].source_url).toBe(officialPollingUrl);
    }
  });

  it("replaces non-polling polling_place_url with best polling URL from evidence", async () => {
    const officialPollingUrl = "https://www.sos.ca.gov/elections/polling-place";
    globalThis.fetch = vi.fn(async () =>
      openAiResponse(
        validPayload({
          polling_place_url: "https://www.usa.gov/register-to-vote",
          sources: {
            polling_place_url: [
              { source_name: "USA.gov", source_url: "https://www.usa.gov/register-to-vote" },
            ],
            voter_registration_url: [{ source_name: "USA.gov", source_url: "https://www.usa.gov/register-to-vote" }],
            vote_by_mail_info: [{ source_name: "Vote.org", source_url: "https://www.vote.org/absentee-ballot/" }],
            polling_hours: [{ source_name: "NASS", source_url: "https://www.nass.org/can-i-vote" }],
            id_requirements: [{ source_name: "USVF", source_url: "https://www.usvotefoundation.org/voter-id-laws" }],
          },
        })
      )
    ) as unknown as typeof fetch;

    const result = await enrichStateResources(
      {
        ingestKey: "k6",
        draft: draft(),
        evidence: [
          { url: "https://www.usa.gov/register-to-vote", title: "USA.gov", snippet: "Registration info" },
          { url: officialPollingUrl, title: "California Secretary of State", snippet: "Find your polling place" },
          { url: "https://www.vote.org/absentee-ballot/", title: "Vote.org", snippet: "Mail vote" },
          { url: "https://www.nass.org/can-i-vote", title: "NASS", snippet: "Hours" },
          { url: "https://www.usvotefoundation.org/voter-id-laws", title: "USVF", snippet: "ID" },
        ],
        promptVersion: "state_resources_v1",
      },
      {
        provider: "openai",
        model: "gpt-5-mini",
        timeoutMs: 1000,
        openAiApiKey: "test-key",
      }
    );

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.payload.polling_place_url).toBe(officialPollingUrl);
    }
  });

  it("falls back to draft polling seed URL when evidence has no polling candidate", async () => {
    globalThis.fetch = vi.fn(async () =>
      openAiResponse(
        validPayload({
          polling_place_url: "https://www.usa.gov/register-to-vote",
          sources: {
            polling_place_url: [{ source_name: "USA.gov", source_url: "https://www.usa.gov/register-to-vote" }],
            voter_registration_url: [{ source_name: "USA.gov", source_url: "https://www.usa.gov/register-to-vote" }],
            vote_by_mail_info: [{ source_name: "Vote.org", source_url: "https://www.vote.org/absentee-ballot/" }],
            polling_hours: [{ source_name: "NASS", source_url: "https://www.nass.org/can-i-vote" }],
            id_requirements: [{ source_name: "USVF", source_url: "https://www.usvotefoundation.org/voter-id-laws" }],
          },
        })
      )
    ) as unknown as typeof fetch;

    const result = await enrichStateResources(
      {
        ingestKey: "k10",
        draft: draft(),
        evidence: [
          { url: "https://www.usa.gov/register-to-vote", title: "USA.gov", snippet: "Registration info" },
          { url: "https://www.vote.org/absentee-ballot/", title: "Vote.org", snippet: "Mail vote" },
          { url: "https://www.nass.org/can-i-vote", title: "NASS", snippet: "Hours" },
          { url: "https://www.usvotefoundation.org/voter-id-laws", title: "USVF", snippet: "ID" },
        ],
        promptVersion: "state_resources_v1",
      },
      {
        provider: "openai",
        model: "gpt-5-mini",
        timeoutMs: 1000,
        openAiApiKey: "test-key",
      }
    );

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.payload.polling_place_url).toBe("https://www.vote.org/polling-place-locator");
    }
  });

  it("uses deterministic state polling fallback for known missing states", async () => {
    globalThis.fetch = vi.fn(async () =>
      openAiResponse(
        validPayload({
          state_fips: "01",
          state_abbreviation: "AL",
          state_name: "Alabama",
          polling_place_url: "https://www.usa.gov/register-to-vote",
          sources: {
            polling_place_url: [{ source_name: "USA.gov", source_url: "https://www.usa.gov/register-to-vote" }],
            voter_registration_url: [{ source_name: "USA.gov", source_url: "https://www.usa.gov/register-to-vote" }],
            vote_by_mail_info: [{ source_name: "Vote.org", source_url: "https://www.vote.org/absentee-ballot/" }],
            polling_hours: [{ source_name: "NASS", source_url: "https://www.nass.org/can-i-vote" }],
            id_requirements: [{ source_name: "USVF", source_url: "https://www.usvotefoundation.org/voter-id-laws" }],
          },
        })
      )
    ) as unknown as typeof fetch;

    const alabamaDraftInput = {
      ...draft(),
      state_fips: "01",
      state_abbreviation: "AL",
      state_name: "Alabama",
    };

    const result = await enrichStateResources(
      {
        ingestKey: "k11",
        draft: alabamaDraftInput,
        evidence: [
          { url: "https://www.usa.gov/register-to-vote", title: "USA.gov", snippet: "Registration info" },
          { url: "https://www.vote.org/absentee-ballot/", title: "Vote.org", snippet: "Mail vote" },
          { url: "https://www.nass.org/can-i-vote", title: "NASS", snippet: "Hours" },
          { url: "https://www.usvotefoundation.org/voter-id-laws", title: "USVF", snippet: "ID" },
        ],
        promptVersion: "state_resources_v1",
      },
      {
        provider: "openai",
        model: "gpt-5-mini",
        timeoutMs: 1000,
        openAiApiKey: "test-key",
      }
    );

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.payload.polling_place_url).toBe("https://myinfo.alabamavotes.gov/voterview");
    }
  });

  it("does not keep another state's official polling URL when state-specific signal is missing", async () => {
    globalThis.fetch = vi.fn(async () =>
      openAiResponse(
        validPayload({
          state_fips: "12",
          state_abbreviation: "FL",
          state_name: "Florida",
          polling_place_url: "https://www.sos.ca.gov/elections/polling-place",
          sources: {
            polling_place_url: [
              { source_name: "California SOS", source_url: "https://www.sos.ca.gov/elections/polling-place" },
            ],
            voter_registration_url: [{ source_name: "USA.gov", source_url: "https://www.usa.gov/register-to-vote" }],
            vote_by_mail_info: [{ source_name: "Vote.org", source_url: "https://www.vote.org/absentee-ballot/" }],
            polling_hours: [{ source_name: "NASS", source_url: "https://www.nass.org/can-i-vote" }],
            id_requirements: [{ source_name: "USVF", source_url: "https://www.usvotefoundation.org/voter-id-laws" }],
          },
        })
      )
    ) as unknown as typeof fetch;

    const result = await enrichStateResources(
      {
        ingestKey: "k7",
        draft: floridaDraft(),
        evidence: [
          { url: "https://www.vote.org/polling-place-locator/", title: "Vote.org", snippet: "Polling place locator" },
          { url: "https://www.sos.ca.gov/elections/polling-place", title: "California SOS", snippet: "Find your polling place in California" },
          { url: "https://www.usa.gov/register-to-vote", title: "USA.gov", snippet: "Registration info" },
          { url: "https://www.vote.org/absentee-ballot/", title: "Vote.org", snippet: "Mail vote" },
          { url: "https://www.nass.org/can-i-vote", title: "NASS", snippet: "Hours" },
          { url: "https://www.usvotefoundation.org/voter-id-laws", title: "USVF", snippet: "ID" },
        ],
        promptVersion: "state_resources_v1",
      },
      {
        provider: "openai",
        model: "gpt-5-mini",
        timeoutMs: 1000,
        openAiApiKey: "test-key",
      }
    );

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.payload.polling_place_url).toBe("https://www.vote.org/polling-place-locator");
    }
  });

  it("prefers official citation for id_requirements when available", async () => {
    const officialIdUrl = "https://www.sos.ca.gov/elections/voter-id";
    globalThis.fetch = vi.fn(async () =>
      openAiResponse(
        validPayload({
          sources: {
            polling_place_url: [{ source_name: "Vote.org", source_url: "https://www.vote.org/polling-place-locator/" }],
            voter_registration_url: [{ source_name: "USA.gov", source_url: "https://www.usa.gov/register-to-vote" }],
            vote_by_mail_info: [{ source_name: "Vote.org", source_url: "https://www.vote.org/absentee-ballot/" }],
            polling_hours: [{ source_name: "NASS", source_url: "https://www.nass.org/can-i-vote" }],
            id_requirements: [{ source_name: "USVF", source_url: "https://www.usvotefoundation.org/voter-id-laws" }],
          },
        })
      )
    ) as unknown as typeof fetch;

    const result = await enrichStateResources(
      {
        ingestKey: "k12",
        draft: draft(),
        evidence: [
          { url: "https://www.vote.org/polling-place-locator/", title: "Vote.org", snippet: "Polling place" },
          { url: "https://www.usa.gov/register-to-vote", title: "USA.gov", snippet: "Registration" },
          { url: "https://www.vote.org/absentee-ballot/", title: "Vote.org", snippet: "Mail vote" },
          { url: "https://www.nass.org/can-i-vote", title: "NASS", snippet: "Hours" },
          { url: "https://www.usvotefoundation.org/voter-id-laws", title: "USVF", snippet: "ID info" },
          { url: officialIdUrl, title: "California Secretary of State", snippet: "Voter ID requirements and exceptions" },
        ],
        promptVersion: "state_resources_v1",
      },
      {
        provider: "openai",
        model: "gpt-5-mini",
        timeoutMs: 1000,
        openAiApiKey: "test-key",
      }
    );

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.payload.sources.id_requirements[0].source_url).toBe(officialIdUrl);
    }
  });

  it("accepts explicit 'id is required' phrasing for id_requirements", async () => {
    globalThis.fetch = vi.fn(async () =>
      openAiResponse(
        validPayload({
          id_requirements:
            "Voter ID is required at the polls in California, with limited exceptions for eligible provisional voters.",
        })
      )
    ) as unknown as typeof fetch;

    const result = await enrichStateResources(
      {
        ingestKey: "k13",
        draft: draft(),
        evidence: [
          { url: "https://www.vote.org/polling-place-locator/", title: "Vote.org", snippet: "Polling place" },
          { url: "https://www.usa.gov/register-to-vote", title: "USA.gov", snippet: "Registration" },
          { url: "https://www.vote.org/absentee-ballot/", title: "Vote.org", snippet: "Mail vote" },
          { url: "https://www.nass.org/can-i-vote", title: "NASS", snippet: "Hours" },
          { url: "https://www.usvotefoundation.org/voter-id-laws", title: "USVF", snippet: "ID" },
        ],
        promptVersion: "state_resources_v1",
      },
      {
        provider: "openai",
        model: "gpt-5-mini",
        timeoutMs: 1000,
        openAiApiKey: "test-key",
      }
    );

    expect(result.ok).toBe(true);
  });
});
