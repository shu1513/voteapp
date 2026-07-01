import { describe, expect, it } from "vitest";

import { isTlsCertificateReachabilityFailure } from "../../src/ai/urlReachability.js";

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
