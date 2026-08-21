import { beforeEach, describe, expect, it, vi } from "vitest";

const { verifyHttpUrlReachabilityMock } = vi.hoisted(() => ({
  verifyHttpUrlReachabilityMock: vi.fn(),
}));

vi.mock("../../../src/ai/urlReachability.js", () => ({
  verifyHttpUrlReachability: verifyHttpUrlReachabilityMock,
}));

import type { CurrentRaceRatingPayload } from "../../../src/contracts/currentRaceRatingPayloadContract.js";
import {
  collectCurrentRaceRatingSourceUrls,
  validateCurrentRaceRatingSourceUrls,
} from "../../../src/pipeline/competitiveness/currentRaceRatingSourceValidation.js";

const IE_URL = "https://insideelections.com/ratings/senate";
const SABATO_URL = "https://centerforpolitics.org/crystalball/2026-senate";
const WIKI_URL = "https://en.wikipedia.org/wiki/2026_United_States_Senate_elections";

function payload(): CurrentRaceRatingPayload {
  return {
    ratings: [
      {
        election_id: "11111111-1111-4111-8111-111111111111",
        method: "outlet_consensus",
        evidence_status: "rated",
        competitiveness_label: "competitive",
        confidence: "high",
        as_of: "2026-08-06",
        decisive_round: null,
        evidence: {
          mean_intensity: 2.5,
          observations: [
            { outlet: "inside_elections", raw_rating: "Tilt Democrat", favored: "D", intensity: 2, as_of: "2026-08-06", url: IE_URL },
            { outlet: "sabato", raw_rating: "Leans Democratic", favored: "D", intensity: 3, as_of: "2026-07-30", url: SABATO_URL },
          ],
        },
        source_url: WIKI_URL,
      },
      {
        election_id: "22222222-2222-4222-8222-222222222222",
        method: "outlet_consensus",
        evidence_status: "none_found",
        competitiveness_label: null,
        confidence: null,
        as_of: null,
        decisive_round: null,
        evidence: { observations: [] },
        // Duplicate of row 1's source_url on purpose: it must be checked once.
        source_url: WIKI_URL,
      },
    ],
  };
}

function reachable(url: string, overrides: Record<string, unknown> = {}) {
  return { ok: true, normalizedUrl: url, finalUrl: url, status: 200, ...overrides };
}

describe("collectCurrentRaceRatingSourceUrls", () => {
  it("collects deduplicated urls with their outlet binding", () => {
    expect(collectCurrentRaceRatingSourceUrls(payload())).toEqual([
      { url: WIKI_URL, outlet: null },
      { url: IE_URL, outlet: "inside_elections" },
      { url: SABATO_URL, outlet: "sabato" },
    ]);
  });
});

describe("validateCurrentRaceRatingSourceUrls", () => {
  beforeEach(() => {
    verifyHttpUrlReachabilityMock.mockReset();
  });

  it("allows 403 for insideelections.com only and notes it", async () => {
    verifyHttpUrlReachabilityMock.mockImplementation(async (url: string) =>
      reachable(url, { status: url.includes("insideelections") ? 403 : 200 })
    );

    const result = await validateCurrentRaceRatingSourceUrls(payload(), { timeoutMs: 90_000 });
    expect(result.ok).toBe(true);
    if (result.ok) {
      const ie = result.verifications.find((verification) => verification.url === IE_URL);
      expect(ie).toEqual({ url: IE_URL, finalUrl: IE_URL, status: 403, note: "reachable_403" });
    }
    expect(verifyHttpUrlReachabilityMock).toHaveBeenCalledTimes(3);
    // The shared verifier defaults to allowing 403, so non-IE hosts must
    // pass an explicit empty allowance.
    for (const call of verifyHttpUrlReachabilityMock.mock.calls) {
      const [url, options] = call as [string, { timeoutMs: number; allowStatusCodes: number[] }];
      expect(options).toEqual({
        timeoutMs: 10_000,
        allowStatusCodes: url.includes("insideelections") ? [403] : [],
      });
    }
  });

  it("fails with every unreachable url listed", async () => {
    verifyHttpUrlReachabilityMock.mockImplementation(async (url: string) =>
      url === WIKI_URL ? reachable(url) : { ok: false, reason: "HTTP 404" }
    );

    const result = await validateCurrentRaceRatingSourceUrls(payload(), { timeoutMs: 90_000 });
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("failed verification");
      expect(result.failedUrls.map((failed) => failed.url).sort()).toEqual([SABATO_URL, IE_URL].sort());
    }
  });

  it("fails an observation url whose redirect target leaves the outlet's domain", async () => {
    verifyHttpUrlReachabilityMock.mockImplementation(async (url: string) =>
      url === IE_URL
        ? reachable(url, { finalUrl: "https://parked.example.com/ratings" })
        : reachable(url)
    );

    const result = await validateCurrentRaceRatingSourceUrls(payload(), { timeoutMs: 90_000 });
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.failedUrls).toEqual([
        {
          url: IE_URL,
          reason: expect.stringContaining("redirects to a disallowed target https://parked.example.com/ratings"),
        },
      ]);
    }
  });

  it("fails a source url whose redirect target is banned", async () => {
    verifyHttpUrlReachabilityMock.mockImplementation(async (url: string) =>
      url === WIKI_URL
        ? reachable(url, { finalUrl: "https://www.cookpolitical.com/ratings/senate" })
        : reachable(url)
    );

    const result = await validateCurrentRaceRatingSourceUrls(payload(), { timeoutMs: 90_000 });
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.failedUrls[0]?.url).toBe(WIKI_URL);
      expect(result.failedUrls[0]?.reason).toContain("banned as a rating source");
    }
  });
});
