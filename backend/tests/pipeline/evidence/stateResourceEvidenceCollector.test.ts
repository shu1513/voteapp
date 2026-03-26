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

  it("rejects unsafe redirected final URL targets", async () => {
    const fetchImpl: typeof fetch = async () =>
      ({
        ok: true,
        headers: new Headers({ "content-type": "text/plain" }),
        url: "http://[::1]/secret",
        body: null,
        text: async () => "California polling details",
      }) as unknown as Response;

    const evidence = await collectStateResourceEvidence(draft(), { fetchImpl });
    expect(evidence.length).toBe(1);
    expect(evidence[0].snippet).toContain("Live page fetch was unavailable");
  });

  it("can enforce DNS safety checks for hostnames", async () => {
    const hits: string[] = [];
    const fetchImpl: typeof fetch = async (input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : input.toString();
      hits.push(url);
      return new Response("California polling details", {
        status: 200,
        headers: { "content-type": "text/plain" },
      });
    };

    const evidence = await collectStateResourceEvidence(draft(), {
      fetchImpl,
      enforceDnsResolution: true,
      dnsLookupImpl: async () => ["127.0.0.1"],
    });

    expect(hits.length).toBe(0);
    expect(evidence.length).toBe(0);
  });

  it("prioritizes state-specific polling locator links discovered from pages", async () => {
    const hits: string[] = [];
    const fetchImpl: typeof fetch = async (input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : input.toString();
      hits.push(url);

      if (url.startsWith("https://seed.example.org/polling")) {
        return new Response(
          `
            <html><head><title>Polling</title></head><body>
              <a href="/register">Register</a>
              <a href="https://www.vote.org/polling-place-locator/">Vote.org Polling</a>
              <a href="https://www.sos.ca.gov/elections/polling-place/">California polling place locator</a>
            </body></html>
          `,
          { status: 200, headers: { "content-type": "text/html; charset=utf-8" } }
        );
      }

      if (url.startsWith("https://www.sos.ca.gov/elections/polling-place")) {
        return new Response(
          "<html><head><title>CA SOS</title></head><body>Find your polling place in California.</body></html>",
          { status: 200, headers: { "content-type": "text/html; charset=utf-8" } }
        );
      }

      if (url.startsWith("https://www.vote.org/polling-place-locator/")) {
        return new Response(
          "<html><head><title>Vote.org</title></head><body>General polling locator page.</body></html>",
          { status: 200, headers: { "content-type": "text/html; charset=utf-8" } }
        );
      }

      return new Response("not found", { status: 404, headers: { "content-type": "text/plain" } });
    };

    const evidence = await collectStateResourceEvidence(draft(), {
      fetchImpl,
      maxSeedUrls: 1,
      maxDiscoveredUrls: 2,
      maxEvidenceSnippets: 3,
    });

    expect(evidence.some((item) => item.url.startsWith("https://www.sos.ca.gov/elections/polling-place"))).toBe(true);
    expect(hits.some((url) => url.startsWith("https://www.sos.ca.gov/elections/polling-place"))).toBe(true);
  });

  it("extracts Vote.org state-specific polling link for the target state", async () => {
    const hits: string[] = [];
    const fetchImpl: typeof fetch = async (input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : input.toString();
      hits.push(url);

      if (url.startsWith("https://www.vote.org/polling-place-locator")) {
        return new Response(
          `
            <html><head><title>Vote.org Polling Place Locator</title></head><body>
              <tr><td><p id="california">California</p></td><td><p><a href="https://www.sos.ca.gov/elections/polling-place/">California<!-- --> polling place locator</a></p></td></tr>
              <tr><td><p id="florida">Florida</p></td><td><p><a href="https://myinfo.alabamavotes.gov/voterview">Alabama<!-- --> polling place locator</a></p></td></tr>
              <tr><td><p id="florida">Florida</p></td><td><p><a href="https://www.voterfocus.com/PrecinctFinder/addressSearch?county=ALA">Florida<!-- --> polling place locator</a></p></td></tr>
            </body></html>
          `,
          { status: 200, headers: { "content-type": "text/html; charset=utf-8" } }
        );
      }

      if (url.startsWith("https://www.voterfocus.com/PrecinctFinder/addressSearch")) {
        return new Response(
          "<html><head><title>Florida Polling</title></head><body>Florida polling place finder.</body></html>",
          { status: 200, headers: { "content-type": "text/html; charset=utf-8" } }
        );
      }

      return new Response("not found", { status: 404, headers: { "content-type": "text/plain" } });
    };

    const evidence = await collectStateResourceEvidence(
      draft({
        state_fips: "12",
        state_abbreviation: "FL",
        state_name: "Florida",
        seed_sources: ["https://www.vote.org/polling-place-locator/"],
      }),
      {
        fetchImpl,
        maxSeedUrls: 1,
        maxDiscoveredUrls: 2,
        maxEvidenceSnippets: 3,
      }
    );

    expect(
      evidence.some((item) =>
        item.url.startsWith("https://www.voterfocus.com/PrecinctFinder/addressSearch?county=ALA")
      )
    ).toBe(true);
    expect(hits.some((url) => url.startsWith("https://www.vote.org/polling-place-locator"))).toBe(true);
  });
});
