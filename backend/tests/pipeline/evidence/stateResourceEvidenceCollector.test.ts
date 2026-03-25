import { describe, expect, it } from "vitest";
import { collectStateResourceEvidence } from "../../../src/pipeline/evidence/stateResourceEvidenceCollector.ts";
import type { StateResourceDraftPayload } from "../../../src/types/stateResource.ts";

function draft(overrides: Partial<StateResourceDraftPayload> = {}): StateResourceDraftPayload {
  return {
    state_fips: "06",
    state_abbreviation: "CA",
    state_name: "California",
    population_estimate: 39_538_223,
    census_source_url: "https://api.census.gov/data/2024/acs/acs5?get=NAME,B01001_001E&for=state:*",
    state_abbreviation_reference_url: "https://pe.usps.com/text/pub28/28apb.htm",
    seed_sources: ["https://seed.example.org/polling/"],
    allow_open_web_research: true,
    ...overrides,
  };
}

describe("collectStateResourceEvidence", () => {
  it("does not crawl discovered links when allow_open_web_research is false", async () => {
    const hits: string[] = [];
    const fetchImpl: typeof fetch = async (input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : input.toString();
      hits.push(url);

      if (url.startsWith("https://seed.example.org/polling")) {
        return new Response(
          '<html><head><title>Seed Polling</title></head><body>California polling <a href="/register">Register</a></body></html>',
          { status: 200, headers: { "content-type": "text/html; charset=utf-8" } }
        );
      }

      return new Response("not found", { status: 404, headers: { "content-type": "text/plain" } });
    };

    const evidence = await collectStateResourceEvidence(draft({ allow_open_web_research: false }), { fetchImpl });

    expect(evidence.length).toBeGreaterThan(0);
    expect(hits.some((url) => url.includes("/register"))).toBe(false);
  });

  it("blocks private discovered hosts", async () => {
    const hits: string[] = [];
    const fetchImpl: typeof fetch = async (input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : input.toString();
      hits.push(url);

      if (url.startsWith("https://seed.example.org/polling")) {
        return new Response(
          '<html><head><title>Seed Polling</title></head><body>California polling <a href="http://127.0.0.1/admin">Admin</a></body></html>',
          { status: 200, headers: { "content-type": "text/html; charset=utf-8" } }
        );
      }

      if (url.startsWith("http://127.0.0.1")) {
        throw new Error("private host should have been blocked");
      }

      return new Response("not found", { status: 404, headers: { "content-type": "text/plain" } });
    };

    const evidence = await collectStateResourceEvidence(draft(), { fetchImpl });
    expect(evidence.length).toBeGreaterThan(0);
    expect(hits.some((url) => url.startsWith("http://127.0.0.1"))).toBe(false);
  });

  it("falls back when response is non-text or oversized", async () => {
    const binaryFetch: typeof fetch = async () =>
      new Response(new Uint8Array([1, 2, 3, 4]), {
        status: 200,
        headers: { "content-type": "application/octet-stream" },
      });

    const binaryEvidence = await collectStateResourceEvidence(draft(), { fetchImpl: binaryFetch });
    expect(binaryEvidence.length).toBe(1);
    expect(binaryEvidence[0].snippet).toContain("Live page fetch was unavailable");

    const oversizedText = "x".repeat(1_200_000);
    const largeFetch: typeof fetch = async () =>
      new Response(oversizedText, {
        status: 200,
        headers: { "content-type": "text/plain", "content-length": String(oversizedText.length) },
      });

    const largeEvidence = await collectStateResourceEvidence(draft(), { fetchImpl: largeFetch });
    expect(largeEvidence.length).toBe(1);
    expect(largeEvidence[0].snippet).toContain("Live page fetch was unavailable");
  });
});
