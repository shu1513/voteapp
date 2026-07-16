import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const { agentCloseMock, agentOptionsMock, dnsLookupMock } = vi.hoisted(() => ({
  agentCloseMock: vi.fn(),
  agentOptionsMock: vi.fn(),
  dnsLookupMock: vi.fn(),
}));

vi.mock("node:dns/promises", () => ({
  lookup: dnsLookupMock,
}));

vi.mock("undici", () => ({
  Agent: class MockAgent {
    constructor(options: unknown) {
      agentOptionsMock(options);
    }

    close() {
      return agentCloseMock();
    }
  },
}));

import {
  isTlsCertificateReachabilityFailure,
  verifyHttpUrlReachability,
} from "../../src/ai/urlReachability.js";

describe("urlReachability", () => {
  it("detects TLS certificate reachability failures", () => {
    expect(
      isTlsCertificateReachabilityFailure(
        "citation URL fetch failed: fetch failed: UNABLE_TO_VERIFY_LEAF_SIGNATURE"
      )
    ).toBe(true);
    expect(
      isTlsCertificateReachabilityFailure(
        "citation URL fetch failed: unable to get local issuer certificate"
      )
    ).toBe(true);
    expect(
      isTlsCertificateReachabilityFailure("citation URL fetch failed: self-signed certificate")
    ).toBe(true);
    expect(
      isTlsCertificateReachabilityFailure("citation URL fetch failed: self signed certificate")
    ).toBe(true);
    expect(
      isTlsCertificateReachabilityFailure("citation URL fetch failed: DEPTH_ZERO_SELF_SIGNED_CERT")
    ).toBe(true);
    expect(
      isTlsCertificateReachabilityFailure("citation URL fetch failed: SELF_SIGNED_CERT_IN_CHAIN")
    ).toBe(true);
    expect(isTlsCertificateReachabilityFailure("citation URL fetch failed: CERT_HAS_EXPIRED")).toBe(
      true
    );
  });

  it("does not classify ordinary reachability failures as TLS certificate issues", () => {
    expect(isTlsCertificateReachabilityFailure("citation fetch returned status 404")).toBe(false);
    expect(isTlsCertificateReachabilityFailure("citation URL fetch timed out")).toBe(false);
    expect(isTlsCertificateReachabilityFailure("citation URL is not a valid http(s) URL")).toBe(
      false
    );
    expect(isTlsCertificateReachabilityFailure("unable to verify hostname")).toBe(false);
  });
});

describe("verifyHttpUrlReachability HEAD->GET fallback", () => {
  beforeEach(() => {
    agentCloseMock.mockReset();
    agentCloseMock.mockResolvedValue(undefined);
    agentOptionsMock.mockReset();
    dnsLookupMock.mockReset();
    dnsLookupMock.mockResolvedValue([{ address: "93.184.216.34", family: 4 }]);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  type StubStep = {
    status: number;
    location?: string;
    contentType?: string;
    contentLength?: number;
  };

  function stubFetch(handler: (method: string, url: string) => StubStep) {
    const calls: { method: string; url: string }[] = [];
    const dispatchers: unknown[] = [];
    const signals: (AbortSignal | undefined)[] = [];
    vi.stubGlobal(
      "fetch",
      async (
        url: string,
        init?: { dispatcher?: unknown; method?: string; signal?: AbortSignal }
      ) => {
        const method = init?.method ?? "GET";
        calls.push({ method, url });
        dispatchers.push(init?.dispatcher);
        signals.push(init?.signal);
        const { status, location, contentLength, contentType } = handler(method, url);
        return {
          ok: status >= 200 && status < 300,
          status,
          url,
          headers: {
            get: (name: string) => {
              const lowerName = name.toLowerCase();
              if (lowerName === "location") {
                return location ?? null;
              }
              if (lowerName === "content-type") {
                return contentType ?? null;
              }
              if (lowerName === "content-length") {
                return contentLength === undefined ? null : String(contentLength);
              }
              return null;
            },
          },
          body: null,
        } as unknown as Response;
      }
    );
    return { calls, dispatchers, signals };
  }

  it("blocks an IPv4-mapped IPv6 loopback literal before fetch", async () => {
    const { calls } = stubFetch(() => ({ status: 200 }));

    const result = await verifyHttpUrlReachability("http://[::ffff:127.0.0.1]:9/internal");

    expect(calls).toEqual([]);
    expect(result).toEqual({ ok: false, reason: "citation URL points to a blocked/private host" });
  });

  it.each([
    "http://0.0.0.0/source",
    "http://10.0.0.1/source",
    "http://100.64.0.1/source",
    "http://127.0.0.1/source",
    "http://169.254.169.254/source",
    "http://172.16.0.1/source",
    "http://192.0.2.1/source",
    "http://192.168.0.1/source",
    "http://198.18.0.1/source",
    "http://224.0.0.1/source",
    "http://240.0.0.1/source",
    "http://[::]/source",
    "http://[::1]/source",
    "http://[fc00::1]/source",
    "http://[fe80::1]/source",
    "http://[ff02::1]/source",
    "http://[2001:db8::1]/source",
    "http://[::ffff:10.0.0.1]/source",
    // RFC 6052 well-known NAT64 prefix embedding loopback
    "http://[64:ff9b::7f00:1]/source",
    // RFC 8215 local-use NAT64 prefix embedding loopback (ipaddr.js labels
    // it plain unicast, so it needs the explicit range check)
    "http://[64:ff9b:1::7f00:1]/source",
  ])("blocks reserved IP literal %s before fetch", async (url) => {
    const { calls } = stubFetch(() => ({ status: 200 }));

    const result = await verifyHttpUrlReachability(url);

    expect(calls).toEqual([]);
    expect(result).toEqual({ ok: false, reason: "citation URL points to a blocked/private host" });
  });

  it("rejects a hostname that resolves to a local-use NAT64 address", async () => {
    const { calls } = stubFetch(() => ({ status: 200 }));
    dnsLookupMock.mockResolvedValueOnce([{ address: "64:ff9b:1::7f00:1", family: 6 }]);

    const result = await verifyHttpUrlReachability("https://nat64-rebind.example/source");

    expect(calls).toEqual([]);
    expect(result).toEqual({
      ok: false,
      reason: "citation URL hostname resolves to a blocked/private IP",
    });
  });

  it("never contacts a local-use NAT64 redirect target", async () => {
    const { calls } = stubFetch(() => ({
      status: 302,
      location: "http://[64:ff9b:1::7f00:1]/internal-admin",
    }));

    const result = await verifyHttpUrlReachability("https://example.gov/redirects-to-nat64");

    expect(calls).toEqual([
      { method: "HEAD", url: "https://example.gov/redirects-to-nat64" },
    ]);
    expect(result).toEqual({ ok: false, reason: "citation URL points to a blocked/private host" });
  });

  it("fails closed when hostname resolution fails", async () => {
    const { calls } = stubFetch(() => ({ status: 200 }));
    dnsLookupMock.mockRejectedValueOnce(
      Object.assign(new Error("getaddrinfo ENOTFOUND"), { code: "ENOTFOUND" })
    );

    const result = await verifyHttpUrlReachability("https://does-not-resolve.example/source");

    expect(calls).toEqual([]);
    expect(result).toEqual({ ok: false, reason: "citation URL hostname could not be resolved" });
  });

  it("reports a transient reason for resolver failures like EAI_AGAIN", async () => {
    const { calls } = stubFetch(() => ({ status: 200 }));
    dnsLookupMock.mockRejectedValueOnce(
      Object.assign(new Error("getaddrinfo EAI_AGAIN"), { code: "EAI_AGAIN" })
    );

    const result = await verifyHttpUrlReachability("https://flaky-resolver.example/source");

    expect(calls).toEqual([]);
    // "dns lookup failed transiently" is matched by every
    // classifyCitationVerificationFailure copy, keeping resolver outages
    // retryable while ENOTFOUND stays permanent.
    expect(result).toEqual({
      ok: false,
      reason: "citation URL DNS lookup failed transiently: EAI_AGAIN",
    });
  });

  it("bounds DNS resolution with the same timeout budget as the fetch", async () => {
    const { calls } = stubFetch(() => ({ status: 200 }));
    dnsLookupMock.mockImplementationOnce(() => new Promise(() => {}));

    const result = await verifyHttpUrlReachability("https://stalled-resolver.example/source", {
      timeoutMs: 25,
    });

    expect(calls).toEqual([]);
    // "timed out" already classifies as transient downstream.
    expect(result).toEqual({ ok: false, reason: "citation URL DNS lookup timed out" });
  });

  it("rejects a hostname if any resolved address is private", async () => {
    const { calls } = stubFetch(() => ({ status: 200 }));
    dnsLookupMock.mockResolvedValueOnce([
      { address: "93.184.216.34", family: 4 },
      { address: "10.0.0.7", family: 4 },
    ]);

    const result = await verifyHttpUrlReachability("https://mixed-addresses.example/source");

    expect(calls).toEqual([]);
    expect(result).toEqual({
      ok: false,
      reason: "citation URL hostname resolves to a blocked/private IP",
    });
  });

  it("pins the connection lookup to the already-validated DNS answers", async () => {
    const { dispatchers } = stubFetch(() => ({ status: 200 }));
    dnsLookupMock.mockResolvedValueOnce([
      { address: "93.184.216.34", family: 4 },
      { address: "2606:4700:4700::1111", family: 6 },
    ]);

    const result = await verifyHttpUrlReachability("https://pinned.example/source");

    expect(result.ok).toBe(true);
    expect(dnsLookupMock).toHaveBeenCalledTimes(1);
    expect(dispatchers[0]).toBeDefined();
    expect(agentCloseMock).toHaveBeenCalledTimes(1);

    const agentOptions = agentOptionsMock.mock.calls[0]?.[0] as {
      connect?: {
        lookup?: (
          hostname: string,
          options: { all: boolean; family: number },
          callback: (
            error: Error | null,
            addresses: string | Array<{ address: string; family: number }>,
            family?: number
          ) => void
        ) => void;
      };
    };
    const lookup = agentOptions.connect?.lookup;
    expect(lookup).toBeTypeOf("function");
    const pinnedAddresses = await new Promise<Array<{ address: string; family: number }>>(
      (resolve, reject) => {
        lookup?.("pinned.example", { all: true, family: 0 }, (error, addresses) => {
          if (error) {
            reject(error);
            return;
          }
          resolve(addresses as Array<{ address: string; family: number }>);
        });
      }
    );
    expect(pinnedAddresses).toEqual([
      { address: "93.184.216.34", family: 4 },
      { address: "2606:4700:4700::1111", family: 6 },
    ]);
    expect(dnsLookupMock).toHaveBeenCalledTimes(1);
  });

  it("does not let a rejected dispatcher close mask a successful response", async () => {
    stubFetch(() => ({ status: 200 }));
    agentCloseMock.mockRejectedValueOnce(new Error("lingering connection"));

    const result = await verifyHttpUrlReachability("https://example.gov/fine");

    expect(result.ok).toBe(true);
  });

  it("supports a direct GET check and returns final response metadata", async () => {
    const { calls } = stubFetch(() => ({
      status: 200,
      contentType: "text/html; charset=utf-8",
      contentLength: 1234,
    }));

    const result = await verifyHttpUrlReachability("https://example.gov/source", {
      method: "GET",
    });

    expect(calls.map((call) => call.method)).toEqual(["GET"]);
    expect(result).toEqual({
      ok: true,
      normalizedUrl: "https://example.gov/source",
      finalUrl: "https://example.gov/source",
      status: 200,
      contentType: "text/html; charset=utf-8",
      contentLength: 1234,
    });
  });

  it("accepts a URL whose host answers HEAD 404 but GET 200 (CivicPlus DocumentCenter)", async () => {
    const { calls, signals } = stubFetch((method) => ({ status: method === "HEAD" ? 404 : 200 }));

    const result = await verifyHttpUrlReachability("https://ropl.org/DocumentCenter/View/33411/Minutes");

    expect(calls.map((call) => call.method)).toEqual(["HEAD", "GET"]);
    expect(result.ok).toBe(true);
    // Every request runs on its own AbortController, so a slow hop cannot
    // starve the next one's timeout budget.
    expect(signals[0]).toBeDefined();
    expect(signals[1]).toBeDefined();
    expect(signals[1]).not.toBe(signals[0]);
  });

  it("still retries GET on method-not-allowed hosts", async () => {
    const { calls } = stubFetch((method) => ({ status: method === "HEAD" ? 405 : 200 }));

    const result = await verifyHttpUrlReachability("https://example.gov/doc");

    expect(calls.map((call) => call.method)).toEqual(["HEAD", "GET"]);
    expect(result.ok).toBe(true);
  });

  it("fails with the GET status when both methods fail", async () => {
    const { calls } = stubFetch(() => ({ status: 404 }));

    const result = await verifyHttpUrlReachability("https://example.gov/really-gone");

    expect(calls.map((call) => call.method)).toEqual(["HEAD", "GET"]);
    expect(result).toEqual({ ok: false, reason: "citation fetch returned status 404" });
  });

  it("does not issue a GET when HEAD already succeeded", async () => {
    const { calls } = stubFetch(() => ({ status: 200 }));

    const result = await verifyHttpUrlReachability("https://example.gov/fine");

    expect(calls.map((call) => call.method)).toEqual(["HEAD"]);
    expect(result.ok).toBe(true);
  });

  it("does not issue a GET when the HEAD status is in the default allowlist", async () => {
    const { calls } = stubFetch(() => ({ status: 403 }));

    const result = await verifyHttpUrlReachability("https://example.gov/anti-bot");

    expect(calls.map((call) => call.method)).toEqual(["HEAD"]);
    expect(result.ok).toBe(true);
  });

  it("follows public redirects hop by hop and reports the final URL", async () => {
    const { calls } = stubFetch((_method, url) => {
      if (url === "https://example.gov/start") {
        return { status: 301, location: "https://example.gov/middle" };
      }
      if (url === "https://example.gov/middle") {
        return { status: 302, location: "https://docs.example.gov/final" };
      }
      return { status: 200 };
    });

    const result = await verifyHttpUrlReachability("https://example.gov/start");

    expect(calls.map((call) => call.url)).toEqual([
      "https://example.gov/start",
      "https://example.gov/middle",
      "https://docs.example.gov/final",
    ]);
    expect(result).toEqual(
      expect.objectContaining({ ok: true, finalUrl: "https://docs.example.gov/final", status: 200 })
    );
  });

  it("never contacts a private redirect target reached by HEAD", async () => {
    const { calls } = stubFetch((method) =>
      method === "HEAD"
        ? { status: 302, location: "http://127.0.0.1:8080/internal-admin" }
        : { status: 200 }
    );

    const result = await verifyHttpUrlReachability("https://example.gov/redirects-internally");

    expect(calls).toEqual([{ method: "HEAD", url: "https://example.gov/redirects-internally" }]);
    expect(result).toEqual({ ok: false, reason: "citation URL points to a blocked/private host" });
  });

  it("never contacts an IPv4-mapped IPv6 private redirect target", async () => {
    const { calls } = stubFetch(() => ({
      status: 302,
      location: "http://[::ffff:127.0.0.1]:8080/internal-admin",
    }));

    const result = await verifyHttpUrlReachability(
      "https://example.gov/redirects-to-mapped-loopback"
    );

    expect(calls).toEqual([
      { method: "HEAD", url: "https://example.gov/redirects-to-mapped-loopback" },
    ]);
    expect(result).toEqual({ ok: false, reason: "citation URL points to a blocked/private host" });
  });

  it("never contacts a redirect hostname that resolves to a private address", async () => {
    const { calls } = stubFetch((_method, url) =>
      url === "https://example.gov/start"
        ? { status: 302, location: "https://private-after-redirect.example/internal" }
        : { status: 200 }
    );
    dnsLookupMock
      .mockResolvedValueOnce([{ address: "93.184.216.34", family: 4 }])
      .mockResolvedValueOnce([{ address: "192.168.1.20", family: 4 }]);

    const result = await verifyHttpUrlReachability("https://example.gov/start");

    expect(calls).toEqual([{ method: "HEAD", url: "https://example.gov/start" }]);
    expect(result).toEqual({
      ok: false,
      reason: "citation URL hostname resolves to a blocked/private IP",
    });
  });

  it("never contacts a private redirect target reached only by the GET fallback", async () => {
    // HEAD 404s in place; the GET (method-specific behavior) redirects to a
    // private host. The Location must be rejected before any request to it.
    const { calls } = stubFetch((method) =>
      method === "HEAD"
        ? { status: 404 }
        : { status: 302, location: "http://169.254.169.254/latest/meta-data/" }
    );

    const result = await verifyHttpUrlReachability("https://example.gov/head-404-get-redirects");

    expect(calls.map((call) => call.url)).toEqual([
      "https://example.gov/head-404-get-redirects",
      "https://example.gov/head-404-get-redirects",
    ]);
    expect(calls.map((call) => call.method)).toEqual(["HEAD", "GET"]);
    expect(result).toEqual({ ok: false, reason: "citation URL points to a blocked/private host" });
  });

  it("fails a genuine self-redirect as a loop on the first repeat instead of burning the hop limit", async () => {
    const { calls } = stubFetch(() => ({ status: 301, location: "https://example.gov/loop" }));

    const result = await verifyHttpUrlReachability("https://example.gov/loop");

    // One request proves the cycle; the reason names the walked hops so the
    // repeated target is diagnosable.
    expect(calls.length).toBe(1);
    const hop = "https://example.gov/loop";
    expect(result).toEqual({
      ok: false,
      reason: `citation URL redirect loop detected (chain: ${hop} -> ${hop})`,
    });
  });

  it("follows a slashless-to-trailing-slash redirect instead of walking a synthetic self-loop", async () => {
    // The stored citation is normalized slashless; the host 301s to the
    // trailing-slash form. Before the fix the hop target was re-normalized
    // slashless, recreating the original URL until the redirect limit —
    // rejecting ordinary readable pages (Open States, Copper Courier,
    // campaign /about/ pages, all live).
    const { calls } = stubFetch((_method, url) =>
      url === "https://example.org/about"
        ? { status: 301, location: "https://example.org/about/" }
        : { status: 200 }
    );

    const result = await verifyHttpUrlReachability("https://example.org/about");

    expect(calls.map((call) => call.url)).toEqual([
      "https://example.org/about",
      "https://example.org/about/",
    ]);
    expect(result).toEqual(
      expect.objectContaining({ ok: true, finalUrl: "https://example.org/about/", status: 200 })
    );
  });

  it("catches a root self-loop one hop late because the slash-stripped input is a different visited form", async () => {
    // The input normalizes slashless while hop targets keep their slash, so
    // the first redirect to the slashed root is deliberately treated as a new
    // URL (slash-insensitive matching would falsely flag legitimate
    // slashless->slashed redirects as loops). The cycle is still caught on
    // the second, exact-form repeat — one extra request, well inside the hop
    // limit.
    const { calls } = stubFetch(() => ({ status: 301, location: "https://example.org/" }));

    const result = await verifyHttpUrlReachability("https://example.org/");

    expect(calls.map((call) => call.url)).toEqual([
      "https://example.org",
      "https://example.org/",
    ]);
    // The reported chain re-parses each hop, so the slashless first entry
    // renders with the root slash restored.
    expect(result).toEqual({
      ok: false,
      reason:
        "citation URL redirect loop detected (chain: https://example.org/ -> https://example.org/ -> https://example.org/)",
    });
  });

  it("detects an https/http scheme-oscillation cycle as a redirect loop", async () => {
    // Arizona Capitol Times live: https redirects to http, which redirects
    // back to https — a genuine cycle that must fail fast with the loop
    // reason, not the hop-limit reason.
    const { calls } = stubFetch((_method, url) => ({
      status: 302,
      location: url.startsWith("https://")
        ? url.replace("https://", "http://")
        : url.replace("http://", "https://"),
    }));

    const result = await verifyHttpUrlReachability("https://news.example.com/story");

    expect(calls.length).toBe(2);
    expect(result).toEqual({
      ok: false,
      reason:
        "citation URL redirect loop detected (chain: https://news.example.com/story -> http://news.example.com/story -> https://news.example.com/story)",
    });
  });

  it("redacts credentials and query strings from the reported redirect chain", async () => {
    // The signed hop was never in the citation payload; the reason must not
    // persist its token, userinfo, or fragment.
    const signedHop = "https://user:secretpass@files.gov/document.pdf?X-Amz-Signature=secret123#frag";
    stubFetch(() => ({ status: 302, location: signedHop }));

    const result = await verifyHttpUrlReachability("https://portal.gov/doc");

    expect(result.ok).toBe(false);
    const reason = (result as { ok: false; reason: string }).reason;
    expect(reason).toContain("citation URL redirect loop detected");
    expect(reason).toContain("https://files.gov/document.pdf?[redacted]");
    expect(reason).not.toContain("secret123");
    expect(reason).not.toContain("secretpass");
    expect(reason).not.toContain("X-Amz-Signature");
    expect(reason).not.toContain("#frag");
  });

  it("resolves relative Location headers against the current hop", async () => {
    const { calls } = stubFetch((_method, url) =>
      url.endsWith("/start") ? { status: 302, location: "/moved/here" } : { status: 200 }
    );

    const result = await verifyHttpUrlReachability("https://example.gov/start");

    expect(calls.map((call) => call.url)).toEqual([
      "https://example.gov/start",
      "https://example.gov/moved/here",
    ]);
    expect(result.ok).toBe(true);
  });

  it("fails a redirect that has no Location header", async () => {
    const { calls } = stubFetch((method) => (method === "HEAD" ? { status: 302 } : { status: 200 }));

    const result = await verifyHttpUrlReachability("https://example.gov/broken-redirect");

    expect(calls.map((call) => call.method)).toEqual(["HEAD"]);
    expect(result).toEqual({ ok: false, reason: "citation URL redirect is missing a Location header" });
  });
});
