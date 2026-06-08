import { beforeEach, describe, expect, it, vi } from "vitest";

const { verifyHttpUrlReachabilityMock } = vi.hoisted(() => ({
  verifyHttpUrlReachabilityMock: vi.fn(),
}));

vi.mock("../../src/ai/urlReachability.js", () => ({
  verifyHttpUrlReachability: verifyHttpUrlReachabilityMock,
}));

import { validateElectionResultSourceUrls } from "../../src/pipeline/electionResults/electionResultSourceValidation.js";
import type { ElectionResultPayload } from "../../src/contracts/electionResultPayloadContract.js";

function makePayload(sourceUrl = "https://elections.example.gov/results"): ElectionResultPayload {
  return {
    results: [
      {
        election_id: "00000000-0000-0000-0000-000000000001",
        result_status: "unofficial",
        outcome: "won",
        winners: [
          {
            candidate_election_id: "10000000-0000-0000-0000-000000000001",
            candidate_id: "20000000-0000-0000-0000-000000000001",
            candidate_name: "Jane Candidate",
          },
        ],
        match_status: "matched",
        source_url: sourceUrl,
        source_type: "official",
        notes: "",
      },
    ],
  };
}

describe("validateElectionResultSourceUrls", () => {
  beforeEach(() => {
    verifyHttpUrlReachabilityMock.mockReset();
  });

  it("verifies result source urls and replaces with final urls", async () => {
    verifyHttpUrlReachabilityMock.mockResolvedValue({
      ok: true,
      normalizedUrl: "https://elections.example.gov/results",
      finalUrl: "https://elections.example.gov/results/final",
      status: 200,
    });

    const result = await validateElectionResultSourceUrls(makePayload(), { timeoutMs: 30_000 });

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.payload.results[0]?.source_url).toBe("https://elections.example.gov/results/final");
      expect(result.sourceVerifications[0]).toEqual({
        sourceUrl: "https://elections.example.gov/results",
        finalUrl: "https://elections.example.gov/results/final",
        status: 200,
        authority: "verified",
      });
    }
  });

  it("returns retry feedback for unreachable URLs", async () => {
    verifyHttpUrlReachabilityMock.mockResolvedValue({
      ok: false,
      reason: "citation fetch returned status 404",
    });

    const result = await validateElectionResultSourceUrls(makePayload("https://bad.example/results"), {
      timeoutMs: 30_000,
    });

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.blockedUrls).toEqual(["https://bad.example/results"]);
      expect(result.reviewFeedbackLines.join("\n")).toContain("Do not reuse this unreachable/dead");
      expect(result.reason).toContain("not reachable");
    }
  });

  it("accepts 403 URLs as weak authority instead of fully verified", async () => {
    verifyHttpUrlReachabilityMock.mockResolvedValue({
      ok: true,
      normalizedUrl: "https://elections.example.gov/results",
      finalUrl: "https://elections.example.gov/results",
      status: 403,
    });

    const result = await validateElectionResultSourceUrls(makePayload(), { timeoutMs: 30_000 });

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.sourceVerifications[0]?.authority).toBe("weak");
      expect(result.sourceVerifications[0]?.status).toBe(403);
    }
  });

  it("verifies duplicate URLs only once", async () => {
    verifyHttpUrlReachabilityMock.mockResolvedValue({
      ok: true,
      normalizedUrl: "https://elections.example.gov/results",
      finalUrl: "https://elections.example.gov/results",
      status: 200,
    });
    const payload = makePayload();
    payload.results.push({
      ...payload.results[0]!,
      election_id: "00000000-0000-0000-0000-000000000002",
    });

    const result = await validateElectionResultSourceUrls(payload, { timeoutMs: 30_000 });

    expect(result.ok).toBe(true);
    expect(verifyHttpUrlReachabilityMock).toHaveBeenCalledTimes(1);
  });
});
