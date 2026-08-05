import { describe, expect, it, vi } from "vitest";

import {
  buildAllowedOrigins,
  createCorsRejectionLogThrottle,
  resolveCorsHeaders,
  truncateForLog,
} from "../../src/api/apiCors.js";

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
        vary: "Origin, Sec-Fetch-Site",
      },
    });
  });

  it("rejects unknown origins without a same-origin attestation", () => {
    expect(resolveCorsHeaders({ origin: "https://evil.example" }, ["https://frontend.example"])).toEqual({
      ok: false,
      headers: { vary: "Origin, Sec-Fetch-Site" },
    });
  });

  it("accepts an unrecognized origin when the browser attests Sec-Fetch-Site: same-origin", () => {
    // Privacy browsers / in-app webviews can send "Origin: null" on
    // same-origin POSTs; Sec-Fetch-Site is forbidden-header truth.
    expect(
      resolveCorsHeaders({ origin: "null", "sec-fetch-site": "Same-Origin" }, ["https://frontend.example"])
    ).toEqual({
      ok: true,
      headers: { vary: "Origin, Sec-Fetch-Site" },
    });
  });

  it("does not extend the same-origin fallback to cross-site or same-site requests", () => {
    for (const site of ["cross-site", "same-site", "none"]) {
      expect(
        resolveCorsHeaders({ origin: "null", "sec-fetch-site": site }, ["https://frontend.example"])
      ).toEqual({
        ok: false,
        headers: { vary: "Origin, Sec-Fetch-Site" },
      });
    }
  });

  it("does not advertise credentials for wildcard origins", () => {
    expect(resolveCorsHeaders({ origin: "https://frontend.example" }, ["*"])).toEqual({
      ok: true,
      headers: {
        "access-control-allow-origin": "*",
        "access-control-allow-methods": "GET, POST, PUT, DELETE, OPTIONS",
        "access-control-allow-headers": "authorization, content-type, x-voteapp-client",
        "access-control-max-age": "600",
        vary: "Origin, Sec-Fetch-Site",
      },
    });
  });
});

describe("truncateForLog", () => {
  it("marks an absent header instead of printing undefined", () => {
    expect(truncateForLog(undefined)).toBe("(absent)");
  });

  it("passes short values through unchanged", () => {
    expect(truncateForLog("https://frontend.example")).toBe("https://frontend.example");
  });

  it("caps an oversized value and reports its real length", () => {
    const result = truncateForLog("x".repeat(9000));
    expect(result.length).toBeLessThan(200);
    expect(result).toBe(`${"x".repeat(128)}…[9000 chars]`);
  });
});

describe("createCorsRejectionLogThrottle", () => {
  it("logs the first occurrence of a value and suppresses repeats within the cooldown", () => {
    const shouldLog = createCorsRejectionLogThrottle();
    expect(shouldLog("null same-origin", 1_000)).toEqual({ shouldLog: true, suppressed: 0 });
    expect(shouldLog("null same-origin", 2_000)).toEqual({ shouldLog: false, suppressed: 0 });
    expect(shouldLog("null same-origin", 60_000)).toEqual({ shouldLog: false, suppressed: 0 });
  });

  it("logs a distinct value immediately, so a real report is never hidden behind a flood", () => {
    const shouldLog = createCorsRejectionLogThrottle();
    shouldLog("https://evil.example cross-site", 1_000);
    expect(shouldLog("null same-site", 1_001)).toMatchObject({ shouldLog: true });
  });

  it("re-logs a value once its cooldown lapses, reporting what was dropped", () => {
    const shouldLog = createCorsRejectionLogThrottle();
    shouldLog("null same-origin", 0);
    shouldLog("null same-origin", 1_000);
    shouldLog("null same-origin", 2_000);
    expect(shouldLog("null same-origin", 10 * 60_000)).toEqual({ shouldLog: true, suppressed: 2 });
  });

  it("caps randomized origins at the per-window ceiling instead of logging each one", () => {
    const shouldLog = createCorsRejectionLogThrottle();
    let logged = 0;
    for (let index = 0; index < 5_000; index += 1) {
      if (shouldLog(`https://random-${index}.example cross-site`, 1_000).shouldLog) {
        logged += 1;
      }
    }
    expect(logged).toBe(20);
  });

  it("allows the ceiling to refill in the next window", () => {
    const shouldLog = createCorsRejectionLogThrottle();
    for (let index = 0; index < 5_000; index += 1) {
      shouldLog(`https://random-${index}.example cross-site`, 1_000);
    }
    expect(shouldLog("https://late.example cross-site", 90_000)).toMatchObject({ shouldLog: true });
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
