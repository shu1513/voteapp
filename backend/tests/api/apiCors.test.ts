import { describe, expect, it, vi } from "vitest";

import { buildAllowedOrigins, resolveCorsHeaders } from "../../src/api/apiCors.js";

describe("resolveCorsHeaders", () => {
  it("enables credentialed CORS for explicit allowed origins", () => {
    expect(
      resolveCorsHeaders(
        {
          origin: "https://frontend.example",
        },
        ["https://frontend.example"]
      )
    ).toEqual({
      ok: true,
      headers: {
        "access-control-allow-origin": "https://frontend.example",
        "access-control-allow-credentials": "true",
        "access-control-allow-methods": "GET, POST, PUT, DELETE, OPTIONS",
        "access-control-allow-headers": "authorization, content-type, x-voteapp-client",
        "access-control-max-age": "600",
        vary: "Origin",
      },
    });
  });

  it("does not advertise credentials for wildcard origins", () => {
    expect(resolveCorsHeaders({ origin: "https://frontend.example" }, ["*"])).toEqual({
      ok: true,
      headers: {
        "access-control-allow-origin": "*",
        "access-control-allow-methods": "GET, POST, PUT, DELETE, OPTIONS",
        "access-control-allow-headers": "authorization, content-type, x-voteapp-client",
        "access-control-max-age": "600",
        vary: "Origin",
      },
    });
  });
});

describe("buildAllowedOrigins", () => {
  it("splits and trims the env value", () => {
    expect(buildAllowedOrigins(" https://a.example , https://b.example ,")).toEqual([
      "https://a.example",
      "https://b.example",
    ]);
  });

  it("derives origins from fallback URLs when the env value is unset", () => {
    expect(buildAllowedOrigins(undefined, ["https://electionssimplified.com/api/email/unsubscribe"])).toEqual([
      "https://electionssimplified.com",
    ]);
  });

  it("merges env entries with fallback origins without duplicates", () => {
    expect(
      buildAllowedOrigins("https://electionssimplified.com,https://staging.example", [
        "https://electionssimplified.com",
        "https://electionssimplified.com/",
      ])
    ).toEqual(["https://electionssimplified.com", "https://staging.example"]);
  });

  it("ignores blank and unparseable fallback URLs", () => {
    expect(buildAllowedOrigins("", [undefined, "  ", "not a url"])).toEqual([]);
  });

  it("rejects non-http(s) fallback URLs instead of allowlisting the null origin", () => {
    expect(buildAllowedOrigins(undefined, ["data:text/html,x", "javascript:alert(1)", "file:///etc/passwd"])).toEqual(
      []
    );
  });

  it("normalizes http(s) env entries so slashes, paths, case, and default ports match the browser Origin", () => {
    expect(
      buildAllowedOrigins("https://staging.example/, https://EXAMPLE.com:443/app, http://localhost:5173")
    ).toEqual(["https://staging.example", "https://example.com", "http://localhost:5173"]);
  });

  it("keeps wildcard and non-URL env entries verbatim", () => {
    expect(buildAllowedOrigins("*, custom-scheme://thing")).toEqual(["*", "custom-scheme://thing"]);
  });

  it("warns when a fallback URL is dropped", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      buildAllowedOrigins(undefined, ["not a url"]);
      expect(warn).toHaveBeenCalledWith(
        'buildAllowedOrigins: ignoring non-http(s) or unparseable fallback URL "not a url"'
      );
    } finally {
      warn.mockRestore();
    }
  });

  it("keeps same-origin browser POSTs allowed when only fallbacks are configured", () => {
    const allowedOrigins = buildAllowedOrigins(undefined, ["https://electionssimplified.com"]);
    expect(resolveCorsHeaders({ origin: "https://electionssimplified.com" }, allowedOrigins).ok).toBe(true);
  });
});
