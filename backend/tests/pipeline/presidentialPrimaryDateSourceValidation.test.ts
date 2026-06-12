import { beforeEach, describe, expect, it, vi } from "vitest";

const { verifyHttpUrlReachabilityMock } = vi.hoisted(() => ({
  verifyHttpUrlReachabilityMock: vi.fn(),
}));

vi.mock("../../src/ai/urlReachability.js", () => ({
  verifyHttpUrlReachability: verifyHttpUrlReachabilityMock,
}));

import type { PresidentialPrimaryDatePayload } from "../../src/contracts/presidentialPrimaryDatePayloadContract.js";
import {
  classifyPresidentialPrimaryDateSource,
  validatePresidentialPrimaryDateSourceUrls,
  validatePresidentialPrimaryDateSourceUrlsPartial,
} from "../../src/pipeline/presidential/presidentialPrimaryDateSourceValidation.js";

function makePayload(overrides: Partial<PresidentialPrimaryDatePayload["results"][number]> = {}): PresidentialPrimaryDatePayload {
  return {
    results: [
      {
        state_fips: "06",
        state_name: "California",
        status: "official_found",
        primary_date: "2028-03-07",
        sources: ["https://elections.example.gov/2028-primary"],
        notes: "",
        ...overrides,
      },
    ],
  };
}

describe("classifyPresidentialPrimaryDateSource", () => {
  it("classifies government and national-party sources as official-like", () => {
    expect(classifyPresidentialPrimaryDateSource("https://www.sos.ca.gov/elections")).toBe(
      "official_like"
    );
    expect(classifyPresidentialPrimaryDateSource("https://sos.state.co.us/pubs/elections")).toBe(
      "official_like"
    );
    expect(classifyPresidentialPrimaryDateSource("https://www.state.nj.us/state/elections")).toBe(
      "official_like"
    );
    expect(classifyPresidentialPrimaryDateSource("https://www.democrats.org/calendar")).toBe(
      "official_like"
    );
    expect(classifyPresidentialPrimaryDateSource("https://gop.com/primary")).toBe("official_like");
  });

  it("classifies known secondary/news sources as secondary", () => {
    expect(classifyPresidentialPrimaryDateSource("https://www.nytimes.com/elections")).toBe("secondary");
    expect(classifyPresidentialPrimaryDateSource("https://ballotpedia.org/Presidential_primary")).toBe(
      "secondary"
    );
    expect(classifyPresidentialPrimaryDateSource("https://someblog.us/elections")).toBe("secondary");
  });
});

describe("validatePresidentialPrimaryDateSourceUrls", () => {
  beforeEach(() => {
    verifyHttpUrlReachabilityMock.mockReset();
  });

  it("verifies source URLs and replaces them with final URLs", async () => {
    verifyHttpUrlReachabilityMock.mockResolvedValue({
      ok: true,
      normalizedUrl: "https://elections.example.gov/2028-primary",
      finalUrl: "https://www.sos.ca.gov/elections/2028-primary",
      status: 200,
    });

    const result = await validatePresidentialPrimaryDateSourceUrls(makePayload(), {
      timeoutMs: 30_000,
    });

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.payload.results[0]?.sources).toEqual([
        "https://www.sos.ca.gov/elections/2028-primary",
      ]);
      expect(result.sourceVerifications).toEqual([
        {
          sourceUrl: "https://elections.example.gov/2028-primary",
          finalUrl: "https://www.sos.ca.gov/elections/2028-primary",
          status: 200,
          authority: "verified",
          sourceKind: "official_like",
        },
      ]);
    }
  });

  it("returns retry feedback for unreachable source URLs", async () => {
    verifyHttpUrlReachabilityMock.mockResolvedValue({
      ok: false,
      reason: "citation fetch returned status 404",
    });

    const result = await validatePresidentialPrimaryDateSourceUrls(makePayload(), {
      timeoutMs: 30_000,
    });

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.blockedUrls).toEqual(["https://elections.example.gov/2028-primary"]);
      expect(result.reason).toContain("not reachable");
      expect(result.reviewFeedbackLines.join("\n")).toContain("Do not reuse this unreachable/dead");
    }
  });

  it("accepts 403 source URLs as weak authority", async () => {
    verifyHttpUrlReachabilityMock.mockResolvedValue({
      ok: true,
      normalizedUrl: "https://www.sos.ca.gov/elections/2028-primary",
      finalUrl: "https://www.sos.ca.gov/elections/2028-primary",
      status: 403,
    });

    const result = await validatePresidentialPrimaryDateSourceUrls(
      makePayload({ sources: ["https://www.sos.ca.gov/elections/2028-primary"] }),
      { timeoutMs: 30_000 }
    );

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.sourceVerifications[0]?.authority).toBe("weak");
      expect(result.sourceVerifications[0]?.sourceKind).toBe("official_like");
    }
  });

  it("rejects official_found rows backed only by secondary sources", async () => {
    verifyHttpUrlReachabilityMock.mockResolvedValue({
      ok: true,
      normalizedUrl: "https://www.nytimes.com/elections/2028-primary",
      finalUrl: "https://www.nytimes.com/elections/2028-primary",
      status: 200,
    });

    const result = await validatePresidentialPrimaryDateSourceUrls(
      makePayload({ sources: ["https://www.nytimes.com/elections/2028-primary"] }),
      { timeoutMs: 30_000 }
    );

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("requires an official-looking source");
      expect(result.reviewFeedbackLines.join("\n")).toContain("Find an official source for state_fips=06");
    }
  });

  it("does not treat arbitrary .us domains as official-like", async () => {
    verifyHttpUrlReachabilityMock.mockResolvedValue({
      ok: true,
      normalizedUrl: "https://someblog.us/elections/2028-primary",
      finalUrl: "https://someblog.us/elections/2028-primary",
      status: 200,
    });

    const result = await validatePresidentialPrimaryDateSourceUrls(
      makePayload({ sources: ["https://someblog.us/elections/2028-primary"] }),
      { timeoutMs: 30_000 }
    );

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toContain("requires an official-looking source");
    }
  });

  it("allows not_official_yet rows with reachable secondary sources", async () => {
    verifyHttpUrlReachabilityMock.mockResolvedValue({
      ok: true,
      normalizedUrl: "https://ballotpedia.org/Presidential_primary",
      finalUrl: "https://ballotpedia.org/Presidential_primary",
      status: 200,
    });

    const result = await validatePresidentialPrimaryDateSourceUrls(
      makePayload({
        status: "not_official_yet",
        primary_date: null,
        sources: ["https://ballotpedia.org/Presidential_primary"],
      }),
      { timeoutMs: 30_000 }
    );

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.sourceVerifications[0]?.sourceKind).toBe("secondary");
    }
  });

  it("verifies duplicate URLs only once", async () => {
    verifyHttpUrlReachabilityMock.mockResolvedValue({
      ok: true,
      normalizedUrl: "https://www.sos.ca.gov/elections/2028-primary",
      finalUrl: "https://www.sos.ca.gov/elections/2028-primary",
      status: 200,
    });

    const payload = makePayload({ sources: ["https://www.sos.ca.gov/elections/2028-primary"] });
    payload.results.push({
      ...payload.results[0]!,
      state_fips: "11",
      state_name: "District of Columbia",
      sources: ["https://www.sos.ca.gov/elections/2028-primary"],
    });

    const result = await validatePresidentialPrimaryDateSourceUrls(payload, { timeoutMs: 30_000 });

    expect(result.ok).toBe(true);
    expect(verifyHttpUrlReachabilityMock).toHaveBeenCalledTimes(1);
  });

  it("partially accepts rows with reachable official sources while reporting row-level failures", async () => {
    verifyHttpUrlReachabilityMock.mockImplementation(async (url: string) => {
      if (url.includes("dead")) {
        return {
          ok: false,
          reason: "citation fetch returned status 404",
        };
      }
      return {
        ok: true,
        normalizedUrl: url,
        finalUrl: url,
        status: 200,
      };
    });

    const payload = makePayload({ sources: ["https://www.sos.ca.gov/elections/2028-primary"] });
    payload.results.push({
      state_fips: "12",
      state_name: "Florida",
      status: "official_found",
      primary_date: "2028-03-14",
      sources: ["https://dead.example.gov/calendar"],
      notes: "",
    });

    const result = await validatePresidentialPrimaryDateSourceUrlsPartial(payload, {
      timeoutMs: 30_000,
    });

    expect(result.payload.results.map((row) => row.state_fips)).toEqual(["06"]);
    expect(result.failedRows).toEqual([
      expect.objectContaining({
        state_fips: "12",
        blockedUrls: ["https://dead.example.gov/calendar"],
      }),
    ]);
    expect(result.reviewFeedbackLines.join("\n")).toContain("state_fips=12");
  });
});
