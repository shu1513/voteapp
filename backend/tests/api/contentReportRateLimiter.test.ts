import { describe, expect, it } from "vitest";

import { createInMemoryContentReportRateLimiter } from "../../src/api/contentReportRateLimiter.js";

function request(clientIp: string) {
  return { clientIp, method: "POST", pathname: "/api/content-reports" };
}

describe("createInMemoryContentReportRateLimiter", () => {
  it("limits content reports by IP", () => {
    const limiter = createInMemoryContentReportRateLimiter({ windowMs: 60_000, maxRequests: 2, now: () => 1_000 });

    expect(limiter(request("203.0.113.10"))).toEqual({ allowed: true });
    expect(limiter(request("203.0.113.10"))).toEqual({ allowed: true });
    expect(limiter(request("203.0.113.10"))).toEqual({ allowed: false, retryAfterSeconds: 60 });
    expect(limiter(request("203.0.113.11"))).toEqual({ allowed: true });
  });
});
