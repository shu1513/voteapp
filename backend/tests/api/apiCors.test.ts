import { describe, expect, it } from "vitest";

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
    expect(buildAllowedOrigins(undefined, ["https://impactperdollar.com/api/email/unsubscribe"])).toEqual([
      "https://impactperdollar.com",
    ]);
  });

  it("merges env entries with fallback origins without duplicates", () => {
    expect(
      buildAllowedOrigins("https://impactperdollar.com,https://staging.example", [
        "https://impactperdollar.com",
        "https://impactperdollar.com/",
      ])
    ).toEqual(["https://impactperdollar.com", "https://staging.example"]);
  });

  it("ignores blank and unparseable fallback URLs", () => {
    expect(buildAllowedOrigins("", [undefined, "  ", "not a url"])).toEqual([]);
  });

  it("keeps same-origin browser POSTs allowed when only fallbacks are configured", () => {
    const allowedOrigins = buildAllowedOrigins(undefined, ["https://impactperdollar.com"]);
    expect(resolveCorsHeaders({ origin: "https://impactperdollar.com" }, allowedOrigins).ok).toBe(true);
  });
});
