import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";

import worker, { isApiPath, resolveUpstreamHost, withSecurityHeaders } from "./router-worker.js";

const ENV = {
  API_ORIGIN: "voteapp-api-pzns.onrender.com",
  SSR_ORIGIN: "voteapp-ssr-abcd.onrender.com",
};

const realFetch = globalThis.fetch;

/** Stubs global fetch, recording each proxied Request. */
function stubFetch() {
  const calls = [];
  globalThis.fetch = async (input) => {
    const request = input instanceof Request ? input : new Request(input);
    calls.push(request);
    return new Response("upstream ok", { status: 200 });
  };
  return calls;
}

afterEach(() => {
  globalThis.fetch = realFetch;
});

describe("resolveUpstreamHost", () => {
  const accepted = [
    ["plain Render hostname", "voteapp-api-pzns.onrender.com", "voteapp-api-pzns.onrender.com"],
    ["surrounding whitespace trimmed", "  voteapp-api-pzns.onrender.com  ", "voteapp-api-pzns.onrender.com"],
    ["uppercase lowercased", "HOST.Example.COM", "host.example.com"],
    ["FQDN trailing dot canonicalized", "host.example.com.", "host.example.com"],
    ["single label", "localhost", "localhost"],
  ];
  for (const [name, input, expected] of accepted) {
    it(`accepts ${name}`, () => {
      assert.equal(resolveUpstreamHost(input), expected);
    });
  }

  const rejected = [
    ["undefined", undefined],
    ["empty string", ""],
    ["whitespace only", "   "],
    ["scheme prefix", "https://api.example.com"],
    ["port", "host.example.com:8443"],
    ["path", "host.example.com/path"],
    ["credentials", "user:pass@host.example.com"],
    ["query string", "host.example.com?x=1"],
    ["bare dot", "."],
    ["empty label", "foo..bar"],
    ["label starting with hyphen", "-bad.example"],
    ["label ending with hyphen", "bad-.example"],
    ["underscore label", "_bad.example"],
    ["label over 63 chars", `${"a".repeat(64)}.example`],
    ["hostname over 253 chars", `${`${"a".repeat(63)}.`.repeat(4)}${"b".repeat(10)}`],
    ["spaces inside", "not a host"],
  ];
  for (const [name, input] of rejected) {
    it(`rejects ${name}`, () => {
      assert.equal(resolveUpstreamHost(input), null);
    });
  }
});

describe("isApiPath", () => {
  it("routes API and sitemap paths to the API service", () => {
    for (const path of ["/api", "/api/", "/api/elections/x", "/sitemap.xml"]) {
      assert.equal(isApiPath(path), true, path);
    }
  });

  it("routes everything else to SSR", () => {
    for (const path of ["/", "/apifoo", "/elections/x", "/sitemap.xml.bak"]) {
      assert.equal(isApiPath(path), false, path);
    }
  });
});

describe("fetch handler", () => {
  it("301s www to the apex even with no origin config", async () => {
    const response = await worker.fetch(new Request("https://www.impactperdollar.com/ballot?d=1"), {});
    assert.equal(response.status, 301);
    assert.equal(response.headers.get("location"), "https://impactperdollar.com/ballot?d=1");
  });

  it("503s when either origin var is missing", async () => {
    for (const env of [{}, { API_ORIGIN: ENV.API_ORIGIN }, { SSR_ORIGIN: ENV.SSR_ORIGIN }]) {
      const response = await worker.fetch(new Request("https://impactperdollar.com/"), env);
      assert.equal(response.status, 503);
    }
  });

  it("503s on malformed origins instead of proxying", async () => {
    for (const bad of ["   ", "https://api.example.com", "foo..bar", "-bad.example"]) {
      const response = await worker.fetch(new Request("https://impactperdollar.com/"), {
        ...ENV,
        SSR_ORIGIN: bad,
      });
      assert.equal(response.status, 503, bad);
      // Malformed values must be rejected by validation, not reach the
      // self-proxy guard.
      assert.match(await response.text(), /must both be set/, new Error(bad));
    }
  });

  it("503s when an origin points back at the apex, www, or dotted self", async () => {
    for (const self of ["impactperdollar.com", "www.impactperdollar.com", "impactperdollar.com."]) {
      const response = await worker.fetch(new Request("https://impactperdollar.com/"), {
        ...ENV,
        SSR_ORIGIN: self,
      });
      assert.equal(response.status, 503, self);
      // The body pins WHICH guard fired: these are syntactically valid
      // hostnames that must reach and trip the self-proxy check (the dotted
      // form specifically proves canonicalization ran before the equality).
      assert.match(await response.text(), /upstream origin equals/, new Error(self));
    }
  });

  it("proxies API paths to the API origin and the rest to SSR", async () => {
    const calls = stubFetch();

    await worker.fetch(new Request("https://impactperdollar.com/api/elections/x?y=1"), ENV);
    await worker.fetch(new Request("https://impactperdollar.com/sitemap.xml"), ENV);
    await worker.fetch(new Request("https://impactperdollar.com/elections/x"), ENV);

    assert.deepEqual(
      calls.map((request) => new URL(request.url).hostname),
      [ENV.API_ORIGIN, ENV.API_ORIGIN, ENV.SSR_ORIGIN]
    );
    assert.equal(new URL(calls[0].url).search, "?y=1");
  });

  it("preserves method and headers on the proxied request", async () => {
    const calls = stubFetch();

    await worker.fetch(
      new Request("https://impactperdollar.com/api/auth/login", {
        method: "POST",
        headers: { "content-type": "application/json", "cf-connecting-ip": "203.0.113.9" },
        body: "{}",
      }),
      ENV
    );

    assert.equal(calls[0].method, "POST");
    assert.equal(calls[0].headers.get("cf-connecting-ip"), "203.0.113.9");
    assert.equal(new URL(calls[0].url).pathname, "/api/auth/login");
  });
});

describe("security headers", () => {
  const EXPECTED = {
    "strict-transport-security": "max-age=31536000; includeSubDomains",
    "x-content-type-options": "nosniff",
    "x-frame-options": "DENY",
    "referrer-policy": "strict-origin-when-cross-origin",
    "permissions-policy": "camera=(), microphone=(), geolocation=()",
  };

  function assertSecurityHeaders(response) {
    for (const [name, value] of Object.entries(EXPECTED)) {
      assert.equal(response.headers.get(name), value, name);
    }
  }

  it("stamps proxied responses while preserving status, body, and upstream headers", async () => {
    globalThis.fetch = async () =>
      new Response("upstream body", { status: 201, headers: { "x-upstream": "kept" } });

    const response = await worker.fetch(new Request("https://impactperdollar.com/api/elections"), ENV);

    assertSecurityHeaders(response);
    assert.equal(response.status, 201);
    assert.equal(response.headers.get("x-upstream"), "kept");
    assert.equal(await response.text(), "upstream body");
  });

  it("overwrites upstream copies so the edge stays authoritative", async () => {
    globalThis.fetch = async () =>
      new Response("ok", { headers: { "x-frame-options": "ALLOWALL" } });

    const response = await worker.fetch(new Request("https://impactperdollar.com/"), ENV);

    assert.equal(response.headers.get("x-frame-options"), "DENY");
  });

  it("stamps the www redirect and misconfiguration responses", async () => {
    const redirect = await worker.fetch(new Request("https://www.impactperdollar.com/ballot"), {});
    assertSecurityHeaders(redirect);
    assert.equal(redirect.status, 301);
    assert.equal(redirect.headers.get("location"), "https://impactperdollar.com/ballot");

    const misconfigured = await worker.fetch(new Request("https://impactperdollar.com/"), {});
    assertSecurityHeaders(misconfigured);
    assert.equal(misconfigured.status, 503);
  });

  it("withSecurityHeaders copies immutable-header responses instead of mutating", () => {
    const original = Response.redirect("https://impactperdollar.com/", 301);
    const stamped = withSecurityHeaders(original);

    assert.equal(stamped.headers.get("x-content-type-options"), "nosniff");
    assert.equal(stamped.headers.get("location"), "https://impactperdollar.com/");
    assert.equal(original.headers.get("x-content-type-options"), null);
  });
});
