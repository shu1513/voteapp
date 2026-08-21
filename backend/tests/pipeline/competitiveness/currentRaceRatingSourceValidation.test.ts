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

describe("collectCurrentRaceRatingSourceUrls", () => {
  it("collects row source urls and nested observation urls, deduplicated", () => {
    expect(collectCurrentRaceRatingSourceUrls(payload())).toEqual([WIKI_URL, IE_URL, SABATO_URL]);
  });
});

describe("validateCurrentRaceRatingSourceUrls", () => {
  beforeEach(() => {
    verifyHttpUrlReachabilityMock.mockReset();
  });

  it("passes when every url is reachable and notes 403s instead of failing them", async () => {
    verifyHttpUrlReachabilityMock.mockImplementation(async (url: string) => ({
      ok: true,
      normalizedUrl: url,
      finalUrl: url,
      status: url.includes("insideelections") ? 403 : 200,
    }));

    const result = await validateCurrentRaceRatingSourceUrls(payload(), { timeoutMs: 90_000 });
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.verifications).toHaveLength(3);
      const ie = result.verifications.find((verification) => verification.url === IE_URL);
      expect(ie).toEqual({ url: IE_URL, finalUrl: IE_URL, status: 403, note: "reachable_403" });
      const wiki = result.verifications.find((verification) => verification.url === WIKI_URL);
      expect(wiki?.note).toBe("ok");
    }
    // Each unique URL is checked exactly once, with the 403 allowance and a
    // capped timeout.
    expect(verifyHttpUrlReachabilityMock).toHaveBeenCalledTimes(3);
    expect(verifyHttpUrlReachabilityMock.mock.calls[0]?.[1]).toEqual({
      timeoutMs: 10_000,
      allowStatusCodes: [403],
    });
  });

  it("fails with every unreachable url listed", async () => {
    verifyHttpUrlReachabilityMock.mockImplementation(async (url: string) =>
      url === WIKI_URL
        ? { ok: true, normalizedUrl: url, finalUrl: url, status: 200 }
        : { ok: false, reason: "HTTP 404" }
    );

    const result = await validateCurrentRaceRatingSourceUrls(payload(), { timeoutMs: 90_000 });
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("not reachable");
      expect(result.failedUrls.map((failed) => failed.url).sort()).toEqual([SABATO_URL, IE_URL].sort());
    }
  });
});
