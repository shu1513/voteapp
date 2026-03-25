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

function validPayload(overrides: Record<string, unknown> = {}) {
  return {
    state_fips: "06",
    state_abbreviation: "CA",
    state_name: "California",
    polling_place_url: "https://www.vote.org/polling-place-locator/",
    voter_registration_url: "https://www.usa.gov/register-to-vote",
    vote_by_mail_info: "Mail voting info.",
    polling_hours: "Polling hours info.",
    id_requirements: "ID requirements info.",
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

  it("rejects citations that are not grounded in evidence URLs", async () => {
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

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.errorCode).toBe("SCHEMA_MISMATCH");
      expect(result.reason).toContain("citation URL must come from collected evidence URLs");
    }
    expect(globalThis.fetch).toHaveBeenCalledTimes(1);
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
});

