import { describe, expect, it } from "vitest";

import { resolveCorsHeaders } from "../../src/api/apiCors.js";

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
        "access-control-allow-headers": "content-type",
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
        "access-control-allow-headers": "content-type",
        "access-control-max-age": "600",
        vary: "Origin",
      },
    });
  });
});
