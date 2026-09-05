// @vitest-environment node
import { describe, expect, it } from "vitest";
import { canonicalHostRedirectUrl } from "./canonicalHostRedirect";

const CANONICAL = "https://electionssimplified.com";
const SECRET = "edge-secret-value";

function directRequest(path = "/ballot?x=1", headers: Record<string, string> = {}): Request {
  return new Request(`https://voteapp-ssr.onrender.com${path}`, { headers });
}

describe("canonicalHostRedirectUrl", () => {
  it("is inert when CANONICAL_SITE_ORIGIN is unset (dev, or the operator has not opted in)", () => {
    expect(canonicalHostRedirectUrl(directRequest(), { EDGE_SHARED_SECRET: SECRET })).toBeNull();
    expect(canonicalHostRedirectUrl(directRequest(), { CANONICAL_SITE_ORIGIN: "  ", EDGE_SHARED_SECRET: SECRET })).toBeNull();
  });

  it("is inert without EDGE_SHARED_SECRET, since there is no proof to check", () => {
    expect(canonicalHostRedirectUrl(directRequest(), { CANONICAL_SITE_ORIGIN: CANONICAL })).toBeNull();
  });

  it("serves a request carrying the Worker's proof normally", () => {
    const request = directRequest("/ballot?x=1", { "x-edge-secret": SECRET });
    expect(canonicalHostRedirectUrl(request, { CANONICAL_SITE_ORIGIN: CANONICAL, EDGE_SHARED_SECRET: SECRET })).toBeNull();
  });

  it("redirects a request without proof to the same path and query on the canonical origin", () => {
    const env = { CANONICAL_SITE_ORIGIN: CANONICAL, EDGE_SHARED_SECRET: SECRET };
    expect(canonicalHostRedirectUrl(directRequest("/ballot?x=1"), env)).toBe("https://electionssimplified.com/ballot?x=1");
    expect(canonicalHostRedirectUrl(directRequest("/"), env)).toBe("https://electionssimplified.com/");
  });

  it("redirects a request whose proof does not match", () => {
    const request = directRequest("/mission", { "x-edge-secret": "wrong" });
    expect(canonicalHostRedirectUrl(request, { CANONICAL_SITE_ORIGIN: CANONICAL, EDGE_SHARED_SECRET: SECRET })).toBe(
      "https://electionssimplified.com/mission"
    );
  });

  it("tolerates a trailing slash on the configured origin", () => {
    expect(
      canonicalHostRedirectUrl(directRequest("/terms"), { CANONICAL_SITE_ORIGIN: `${CANONICAL}/`, EDGE_SHARED_SECRET: SECRET })
    ).toBe("https://electionssimplified.com/terms");
  });
});
