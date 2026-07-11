import { describe, expect, it } from "vitest";

import { afterEach, vi } from "vitest";

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
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  type StubStep = { status: number; location?: string };

  function stubFetch(handler: (method: string, url: string) => StubStep) {
    const calls: { method: string; url: string }[] = [];
    const signals: (AbortSignal | undefined)[] = [];
    vi.stubGlobal("fetch", async (url: string, init?: { method?: string; signal?: AbortSignal }) => {
      const method = init?.method ?? "GET";
      calls.push({ method, url });
      signals.push(init?.signal);
      const { status, location } = handler(method, url);
      return {
        ok: status >= 200 && status < 300,
        status,
        url,
        headers: { get: (name: string) => (name.toLowerCase() === "location" ? location ?? null : null) },
        body: null,
      } as unknown as Response;
    });
    return { calls, signals };
  }

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

  it("stops at the redirect limit instead of looping", async () => {
    const { calls } = stubFetch(() => ({ status: 301, location: "https://example.gov/loop" }));

    const result = await verifyHttpUrlReachability("https://example.gov/loop");

    expect(calls.length).toBe(6);
    // The reason names every walked hop so the terminal target is diagnosable.
    const hop = "https://example.gov/loop";
    expect(result).toEqual({
      ok: false,
      reason: `citation URL exceeded the redirect limit (chain: ${Array(7).fill(hop).join(" -> ")})`,
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
    expect(reason).toContain("citation URL exceeded the redirect limit");
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
