import { afterEach, describe, expect, it, vi } from "vitest";

import {
  OpenFecClientError,
  buildOpenFecPresidentialCandidateByIdUrl,
  buildOpenFecPresidentialCandidateSearchUrl,
  createOpenFecRateLimiter,
  fetchOpenFecJsonWithKeyRotation,
  getPresidentialCandidateByFecId,
  readOpenFecApiKeysFromEnv,
  searchPresidentialCandidatesByName,
} from "../../../src/pipeline/presidential/openFecClient.js";

function jsonResponse(payload: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(payload), {
    status: 200,
    statusText: "OK",
    ...init,
  });
}

function timeoutOnAbort(_input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  return new Promise((_resolve, reject) => {
    init?.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), { once: true });
  });
}

function sampleCandidate(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    candidate_id: "P80000001",
    name: "JANE PRESIDENT",
    party: "DEM",
    party_full: "Democratic Party",
    office: "P",
    office_full: "President",
    election_years: [2028, "2024"],
    first_file_date: "2026-01-02",
    last_file_date: "2026-02-03",
    principal_committees: [
      {
        committee_id: "C00800001",
        name: "Jane President for America",
        designation: "P",
        organization_type: "P",
        state: "CA",
      },
    ],
    ...overrides,
  };
}

describe("openFecClient", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("reads OpenFEC API keys from env in priority order with dedupe", () => {
    expect(
      readOpenFecApiKeysFromEnv({
        FEC_API_KEY_1: " k1 ",
        FEC_API_KEY_2: "k2",
        FEC_API_KEY_3: "k2",
        FEC_API_KEY: "legacy",
      } as NodeJS.ProcessEnv)
    ).toEqual(["k1", "k2", "legacy"]);
  });

  it("builds presidential candidate search URLs", () => {
    const url = new URL(
      buildOpenFecPresidentialCandidateSearchUrl({
        electionYear: 2028,
        name: " Jane President ",
        partyCode: " dem ",
        perPage: 50,
      })
    );

    expect(url.origin + url.pathname).toBe("https://api.open.fec.gov/v1/candidates/search/");
    expect(url.searchParams.get("office")).toBe("P");
    expect(url.searchParams.get("election_year")).toBe("2028");
    expect(url.searchParams.get("q")).toBe("Jane President");
    expect(url.searchParams.get("party")).toBe("DEM");
    expect(url.searchParams.get("per_page")).toBe("50");
    expect(url.searchParams.has("api_key")).toBe(false);
  });

  it("builds presidential candidate-by-id URLs", () => {
    const url = new URL(buildOpenFecPresidentialCandidateByIdUrl(" p80000001 "));

    expect(url.origin + url.pathname).toBe("https://api.open.fec.gov/v1/candidate/P80000001/");
    expect(url.searchParams.get("office")).toBe("P");
    expect(url.searchParams.has("api_key")).toBe(false);
  });

  it("searches and parses presidential candidate results", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse({
        results: [sampleCandidate()],
      })
    ) as unknown as typeof fetch;

    const candidates = await searchPresidentialCandidatesByName(
      { electionYear: 2028, name: "Jane President", partyCode: "DEM" },
      { apiKeys: ["k1"], fetchImpl, timeoutMs: 1000 }
    );

    expect(candidates).toEqual([
      {
        fecCandidateId: "P80000001",
        name: "JANE PRESIDENT",
        party: "DEM",
        partyFull: "Democratic Party",
        office: "P",
        officeFull: "President",
        electionYears: [2024, 2028],
        firstFileDate: "2026-01-02",
        lastFileDate: "2026-02-03",
        principalCommittees: [
          {
            committeeId: "C00800001",
            name: "Jane President for America",
            designation: "P",
            organizationType: "P",
            state: "CA",
          },
        ],
        fecCandidateUrl: "https://www.fec.gov/data/candidate/P80000001",
      },
    ]);

    const requestUrl = new URL(String(vi.mocked(fetchImpl).mock.calls[0]?.[0]));
    expect(requestUrl.searchParams.get("api_key")).toBe("k1");
  });

  it("gets one presidential candidate by FEC ID", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      jsonResponse({
        results: [sampleCandidate()],
      })
    ) as unknown as typeof fetch;

    const candidate = await getPresidentialCandidateByFecId("p80000001", {
      apiKeys: ["k1"],
      fetchImpl,
      timeoutMs: 1000,
    });

    expect(candidate?.fecCandidateId).toBe("P80000001");
    const requestUrl = new URL(String(vi.mocked(fetchImpl).mock.calls[0]?.[0]));
    expect(requestUrl.pathname).toBe("/v1/candidate/P80000001/");
    expect(requestUrl.searchParams.get("api_key")).toBe("k1");
  });

  it("rotates API keys on retryable upstream failures", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(new Response("rate limited", { status: 429, statusText: "Too Many Requests" }))
      .mockResolvedValueOnce(jsonResponse({ ok: true })) as unknown as typeof fetch;

    await expect(
      fetchOpenFecJsonWithKeyRotation("https://api.open.fec.gov/v1/candidates/search/?office=P", {
        apiKeys: ["k1", "k2"],
        fetchImpl,
        timeoutMs: 1000,
      })
    ).resolves.toEqual({ ok: true });

    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(new URL(String(vi.mocked(fetchImpl).mock.calls[0]?.[0])).searchParams.get("api_key")).toBe("k1");
    expect(new URL(String(vi.mocked(fetchImpl).mock.calls[1]?.[0])).searchParams.get("api_key")).toBe("k2");
  });

  it("defers once only after every API key is rate limited", async () => {
    let nowMs = 1000;
    const limiterWaits: number[] = [];
    const retryWaits: number[] = [];
    const rateLimiter = createOpenFecRateLimiter({
      minIntervalMs: 100,
      now: () => nowMs,
      sleep: async (ms) => {
        limiterWaits.push(ms);
        nowMs += ms;
      },
    });
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        new Response("rate limited", {
          status: 429,
          statusText: "Too Many Requests",
          headers: { "Retry-After": "2" },
        })
      )
      .mockResolvedValueOnce(
        new Response("rate limited", {
          status: 429,
          statusText: "Too Many Requests",
          headers: { "Retry-After": "2" },
        })
      )
      .mockResolvedValueOnce(jsonResponse({ ok: true })) as unknown as typeof fetch;

    await expect(
      fetchOpenFecJsonWithKeyRotation("https://api.open.fec.gov/v1/candidates/search/?office=P", {
        apiKeys: ["k1", "k2"],
        fetchImpl,
        timeoutMs: 1000,
        rateLimiter,
        sleep: async (ms) => {
          retryWaits.push(ms);
          nowMs += ms;
        },
      })
    ).resolves.toEqual({ ok: true });

    expect(limiterWaits).toEqual([100]);
    expect(retryWaits).toEqual([2000]);
    expect(
      vi.mocked(fetchImpl).mock.calls.map((call) => new URL(String(call[0])).searchParams.get("api_key"))
    ).toEqual(["k1", "k2", "k1"]);
  });

  it("honors Retry-After seconds once after all keys are rate limited", async () => {
    const waits: number[] = [];
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        new Response("rate limited", {
          status: 429,
          statusText: "Too Many Requests",
          headers: {
            "Retry-After": "2",
            "X-RateLimit-Limit": "60",
            "X-RateLimit-Remaining": "0",
          },
        })
      )
      .mockResolvedValueOnce(jsonResponse({ ok: true })) as unknown as typeof fetch;

    await expect(
      fetchOpenFecJsonWithKeyRotation("https://api.open.fec.gov/v1/candidates/search/?office=P", {
        apiKeys: ["k1"],
        fetchImpl,
        timeoutMs: 1000,
        sleep: async (ms) => {
          waits.push(ms);
        },
      })
    ).resolves.toEqual({ ok: true });

    expect(waits).toEqual([2000]);
    expect(fetchImpl).toHaveBeenCalledTimes(2);
  });

  it("parses an HTTP-date Retry-After value", async () => {
    const nowMs = Date.parse("2026-07-23T12:00:00.000Z");
    const waits: number[] = [];
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(
        new Response("rate limited", {
          status: 429,
          statusText: "Too Many Requests",
          headers: { "Retry-After": new Date(nowMs + 2000).toUTCString() },
        })
      )
      .mockResolvedValueOnce(jsonResponse({ ok: true })) as unknown as typeof fetch;

    await expect(
      fetchOpenFecJsonWithKeyRotation("https://api.open.fec.gov/v1/candidates/search/?office=P", {
        apiKeys: ["k1"],
        fetchImpl,
        timeoutMs: 1000,
        now: () => nowMs,
        sleep: async (ms) => {
          waits.push(ms);
        },
      })
    ).resolves.toEqual({ ok: true });

    expect(waits).toEqual([2000]);
    expect(fetchImpl).toHaveBeenCalledTimes(2);
  });

  it("surfaces 429 metadata without retrying when Retry-After is absent", async () => {
    const sleep = vi.fn(async () => undefined);
    const fetchImpl = vi.fn().mockResolvedValue(
      new Response("rate limited", {
        status: 429,
        statusText: "Too Many Requests",
        headers: {
          "X-RateLimit-Limit": "60",
          "X-RateLimit-Remaining": "0",
        },
      })
    ) as unknown as typeof fetch;

    await expect(
      fetchOpenFecJsonWithKeyRotation("https://api.open.fec.gov/v1/candidates/search/?office=P", {
        apiKeys: ["k1"],
        fetchImpl,
        timeoutMs: 1000,
        sleep,
      })
    ).rejects.toMatchObject({
      code: "rate_limited",
      status: 429,
      retryAfterMs: undefined,
      rateLimit: 60,
      rateLimitRemaining: 0,
    });

    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(sleep).not.toHaveBeenCalled();
  });

  it("does not retry before a Retry-After value longer than the configured maximum", async () => {
    const sleep = vi.fn(async () => undefined);
    const fetchImpl = vi.fn().mockResolvedValue(
      new Response("rate limited", {
        status: 429,
        statusText: "Too Many Requests",
        headers: { "Retry-After": "61" },
      })
    ) as unknown as typeof fetch;

    await expect(
      fetchOpenFecJsonWithKeyRotation("https://api.open.fec.gov/v1/candidates/search/?office=P", {
        apiKeys: ["k1"],
        fetchImpl,
        timeoutMs: 1000,
        maxRateLimitWaitMs: 60_000,
        sleep,
      })
    ).rejects.toMatchObject({ code: "rate_limited", retryAfterMs: 61_000 });

    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(sleep).not.toHaveBeenCalled();
  });

  it("surfaces a persistent 429 after one bounded retry", async () => {
    const waits: number[] = [];
    const fetchImpl = vi.fn().mockImplementation(async () =>
      new Response("rate limited", {
        status: 429,
        statusText: "Too Many Requests",
        headers: { "Retry-After": "1" },
      })
    ) as unknown as typeof fetch;

    await expect(
      fetchOpenFecJsonWithKeyRotation("https://api.open.fec.gov/v1/candidates/search/?office=P", {
        apiKeys: ["k1"],
        fetchImpl,
        timeoutMs: 1000,
        sleep: async (ms) => {
          waits.push(ms);
        },
      })
    ).rejects.toMatchObject({ code: "rate_limited", retryAfterMs: 1000 });

    expect(waits).toEqual([1000]);
    expect(fetchImpl).toHaveBeenCalledTimes(2);
  });

  it("does not rotate API keys on non-retryable request errors", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(
      new Response("bad request", { status: 400, statusText: "Bad Request" })
    ) as unknown as typeof fetch;

    await expect(
      fetchOpenFecJsonWithKeyRotation("https://api.open.fec.gov/v1/candidates/search/?office=P", {
        apiKeys: ["k1", "k2"],
        fetchImpl,
        timeoutMs: 1000,
      })
    ).rejects.toMatchObject({ code: "http_error" });

    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it("preserves malformed-JSON key rotation without adding retries", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce(new Response("not json", { status: 200 }))
      .mockResolvedValueOnce(jsonResponse({ ok: true })) as unknown as typeof fetch;

    await expect(
      fetchOpenFecJsonWithKeyRotation("https://api.open.fec.gov/v1/candidates/search/?office=P", {
        apiKeys: ["k1", "k2"],
        fetchImpl,
        timeoutMs: 1000,
      })
    ).resolves.toEqual({ ok: true });

    expect(fetchImpl).toHaveBeenCalledTimes(2);
  });

  it("serializes concurrent calls through the configured rate limiter", async () => {
    let nowMs = 1000;
    const waits: number[] = [];
    const starts: number[] = [];
    const limiter = createOpenFecRateLimiter({
      minIntervalMs: 100,
      now: () => nowMs,
      sleep: async (ms) => {
        waits.push(ms);
        nowMs += ms;
      },
    });

    await Promise.all([
      limiter().then(() => starts.push(nowMs)),
      limiter().then(() => starts.push(nowMs)),
      limiter().then(() => starts.push(nowMs)),
    ]);

    expect(waits).toEqual([100, 100]);
    expect(starts).toEqual([1000, 1100, 1200]);
  });

  it("defers queued rate-limited calls until Retry-After elapses", async () => {
    let nowMs = 1000;
    const waits: number[] = [];
    const limiter = createOpenFecRateLimiter({
      minIntervalMs: 100,
      now: () => nowMs,
      sleep: async (ms) => {
        waits.push(ms);
        nowMs += ms;
      },
    });

    await limiter();
    const queued = limiter();
    limiter.deferFor(500);
    await queued;

    expect(waits).toEqual([500]);
    expect(nowMs).toBe(1500);
  });

  it("retries a transient timeout once on the same key", async () => {
    vi.useFakeTimers();
    const fetchImpl = vi
      .fn()
      .mockImplementationOnce(timeoutOnAbort)
      .mockResolvedValueOnce(jsonResponse({ ok: true })) as unknown as typeof fetch;

    try {
      const request = fetchOpenFecJsonWithKeyRotation(
        "https://api.open.fec.gov/v1/candidates/search/?office=P",
        {
          apiKeys: ["k1", "k2"],
          fetchImpl,
          timeoutMs: 123,
        }
      );

      await vi.runAllTimersAsync();
      await expect(request).resolves.toEqual({ ok: true });

      expect(fetchImpl).toHaveBeenCalledTimes(2);
      expect(new URL(String(vi.mocked(fetchImpl).mock.calls[0]?.[0])).searchParams.get("api_key")).toBe("k1");
      expect(new URL(String(vi.mocked(fetchImpl).mock.calls[1]?.[0])).searchParams.get("api_key")).toBe("k1");
    } finally {
      vi.useRealTimers();
    }
  });

  it("maps an exhausted timeout retry to a timeout error without rotating keys", async () => {
    vi.useFakeTimers();
    const fetchImpl = vi.fn().mockImplementation(timeoutOnAbort) as unknown as typeof fetch;

    try {
      const request = fetchOpenFecJsonWithKeyRotation("https://api.open.fec.gov/v1/candidates/search/?office=P", {
        apiKeys: ["k1", "k2"],
        fetchImpl,
        timeoutMs: 123,
      });

      const rejection = expect(request).rejects.toMatchObject({
        code: "timeout",
        message: "OpenFEC request timed out after 123ms",
      });
      await vi.runAllTimersAsync();
      await rejection;

      expect(fetchImpl).toHaveBeenCalledTimes(2);
      expect(new URL(String(vi.mocked(fetchImpl).mock.calls[0]?.[0])).searchParams.get("api_key")).toBe("k1");
      expect(new URL(String(vi.mocked(fetchImpl).mock.calls[1]?.[0])).searchParams.get("api_key")).toBe("k1");
    } finally {
      vi.useRealTimers();
    }
  });

  it("rejects invalid inputs before fetch", async () => {
    expect(() =>
      buildOpenFecPresidentialCandidateSearchUrl({
        electionYear: 2026,
        name: "Jane President",
      })
    ).toThrow(OpenFecClientError);

    expect(() => buildOpenFecPresidentialCandidateByIdUrl("H0CA00001")).toThrow(OpenFecClientError);
    expect(() => buildOpenFecPresidentialCandidateByIdUrl("PABCDEFGH")).toThrow(OpenFecClientError);

    await expect(
      fetchOpenFecJsonWithKeyRotation("https://api.open.fec.gov/v1/candidates/search/?office=P", {
        apiKeys: [],
      })
    ).rejects.toMatchObject({ code: "configuration_error" });
  });
});
