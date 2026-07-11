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

  function stubFetch(handler: (method: string) => { status: number }) {
    const calls: string[] = [];
    vi.stubGlobal("fetch", async (url: string, init?: { method?: string }) => {
      const method = init?.method ?? "GET";
      calls.push(method);
      const { status } = handler(method);
      return {
        ok: status >= 200 && status < 300,
        status,
        url,
        body: null,
      } as Response;
    });
    return calls;
  }

  it("accepts a URL whose host answers HEAD 404 but GET 200 (CivicPlus DocumentCenter)", async () => {
    const calls = stubFetch((method) => ({ status: method === "HEAD" ? 404 : 200 }));

    const result = await verifyHttpUrlReachability("https://ropl.org/DocumentCenter/View/33411/Minutes");

    expect(calls).toEqual(["HEAD", "GET"]);
    expect(result.ok).toBe(true);
  });

  it("still retries GET on method-not-allowed hosts", async () => {
    const calls = stubFetch((method) => ({ status: method === "HEAD" ? 405 : 200 }));

    const result = await verifyHttpUrlReachability("https://example.gov/doc");

    expect(calls).toEqual(["HEAD", "GET"]);
    expect(result.ok).toBe(true);
  });

  it("fails with the GET status when both methods fail", async () => {
    const calls = stubFetch(() => ({ status: 404 }));

    const result = await verifyHttpUrlReachability("https://example.gov/really-gone");

    expect(calls).toEqual(["HEAD", "GET"]);
    expect(result).toEqual({ ok: false, reason: "citation fetch returned status 404" });
  });

  it("does not issue a GET when HEAD already succeeded", async () => {
    const calls = stubFetch(() => ({ status: 200 }));

    const result = await verifyHttpUrlReachability("https://example.gov/fine");

    expect(calls).toEqual(["HEAD"]);
    expect(result.ok).toBe(true);
  });

  it("does not issue a GET when the HEAD status is explicitly allowed", async () => {
    const calls = stubFetch(() => ({ status: 403 }));

    const result = await verifyHttpUrlReachability("https://example.gov/anti-bot");

    expect(calls).toEqual(["HEAD"]);
    expect(result.ok).toBe(true);
  });
});
