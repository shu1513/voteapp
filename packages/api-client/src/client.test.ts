import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, apiRequest, configureApi, REQUEST_TIMEOUT_MS } from "./client";

function mockFetch(response: {
  ok?: boolean;
  status?: number;
  jsonBody?: unknown;
  headers?: Record<string, string>;
}) {
  const headers = new Headers(response.headers ?? {});
  const fetchMock = vi.fn().mockResolvedValue({
    ok: response.ok ?? true,
    status: response.status ?? 200,
    headers,
    json: async () => response.jsonBody,
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  // Config is module-level; restore the web defaults between tests.
  configureApi({ baseUrl: "", getAuthHeader: null, requestTimeoutMs: REQUEST_TIMEOUT_MS });
});

describe("apiRequest", () => {
  it("sends JSON bodies with content-type and same-origin credentials", async () => {
    const fetchMock = mockFetch({ jsonBody: { ok: true } });

    await apiRequest("/api/address/resolve", { method: "POST", body: { address: "123 Main St" } });

    expect(fetchMock).toHaveBeenCalledWith("/api/address/resolve", {
      method: "POST",
      credentials: "include",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ address: "123 Main St" }),
      signal: expect.any(AbortSignal),
    });
  });

  it("parses the error envelope into a typed ApiError", async () => {
    mockFetch({
      ok: false,
      status: 422,
      jsonBody: { error: { code: "address_not_found", message: "No match" } },
    });

    const error = await apiRequest("/api/address/resolve", {
      method: "POST",
      body: { address: "x" },
    }).catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(ApiError);
    expect((error as ApiError).status).toBe(422);
    expect((error as ApiError).code).toBe("address_not_found");
    expect((error as ApiError).message).toBe("No match");
  });

  it("carries the backend request_id from unexpected-500 envelopes", async () => {
    mockFetch({
      ok: false,
      status: 500,
      jsonBody: {
        error: { code: "internal_error", message: "Internal error", request_id: "abc-123" },
      },
    });

    const error = await apiRequest("/api/ballot").catch((caught: unknown) => caught);
    expect((error as ApiError).requestId).toBe("abc-123");

    mockFetch({
      ok: false,
      status: 422,
      jsonBody: { error: { code: "address_not_found", message: "No match" } },
    });
    const plain = await apiRequest("/api/ballot").catch((caught: unknown) => caught);
    expect((plain as ApiError).requestId).toBeNull();
  });

  it("surfaces retry-after seconds on 429", async () => {
    mockFetch({
      ok: false,
      status: 429,
      headers: { "retry-after": "17" },
      jsonBody: { error: { code: "rate_limited", message: "Too many requests. Try again later." } },
    });

    const error = await apiRequest("/api/ballot").catch((caught: unknown) => caught);
    expect((error as ApiError).retryAfterSeconds).toBe(17);
  });

  it("still sends requests when AbortSignal.any is unavailable (older browsers)", async () => {
    const originalAny = AbortSignal.any;
    // @ts-expect-error simulating a browser without AbortSignal.any
    AbortSignal.any = undefined;
    try {
      const fetchMock = mockFetch({ jsonBody: { ok: true } });
      const controller = new AbortController();

      await apiRequest("/api/address/autocomplete", {
        method: "POST",
        body: { input: "200 N", session_token: "t" },
        signal: controller.signal,
      });

      expect(fetchMock).toHaveBeenCalledTimes(1);
      const passed = (fetchMock.mock.calls[0][1] as { signal: AbortSignal }).signal;
      expect(passed).toBeInstanceOf(AbortSignal);
      expect(passed.aborted).toBe(false);
    } finally {
      AbortSignal.any = originalAny;
    }
  });

  it("prefixes paths with the configured base URL (mobile transport)", async () => {
    const fetchMock = mockFetch({ jsonBody: { ok: true } });
    configureApi({ baseUrl: "https://api.example.com" });

    await apiRequest("/api/me");

    expect(fetchMock).toHaveBeenCalledWith("https://api.example.com/api/me", expect.anything());
  });

  it("normalizes a trailing slash on the base URL", async () => {
    const fetchMock = mockFetch({ jsonBody: { ok: true } });
    configureApi({ baseUrl: "https://api.example.com/" });

    await apiRequest("/api/me");

    expect(fetchMock).toHaveBeenCalledWith("https://api.example.com/api/me", expect.anything());
  });

  it("uses the default request timeout when none is configured", async () => {
    const timeoutSpy = vi.spyOn(AbortSignal, "timeout");
    mockFetch({ jsonBody: { ok: true } });

    await apiRequest("/api/me");

    expect(timeoutSpy).toHaveBeenCalledWith(REQUEST_TIMEOUT_MS);
  });

  it("applies a configured request timeout (free-tier cold-start headroom)", async () => {
    const timeoutSpy = vi.spyOn(AbortSignal, "timeout");
    mockFetch({ jsonBody: { ok: true } });
    configureApi({ requestTimeoutMs: 75_000 });

    await apiRequest("/api/me");

    expect(timeoutSpy).toHaveBeenCalledWith(75_000);
  });

  it("falls back to the default timeout on invalid configured values", async () => {
    const timeoutSpy = vi.spyOn(AbortSignal, "timeout");
    mockFetch({ jsonBody: { ok: true } });
    configureApi({ requestTimeoutMs: Number.NaN });

    await apiRequest("/api/me");

    expect(timeoutSpy).toHaveBeenCalledWith(REQUEST_TIMEOUT_MS);
  });

  it("attaches the configured auth header, resolving async providers", async () => {
    const fetchMock = mockFetch({ jsonBody: { ok: true } });
    configureApi({ getAuthHeader: async () => "Bearer session-abc" });

    await apiRequest("/api/me");

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/me",
      expect.objectContaining({ headers: { authorization: "Bearer session-abc" } })
    );
  });

  it("sends no auth header when the provider returns null (logged out)", async () => {
    const fetchMock = mockFetch({ jsonBody: { ok: true } });
    configureApi({ getAuthHeader: () => null });

    await apiRequest("/api/ballot");

    expect(fetchMock).toHaveBeenCalledWith("/api/ballot", expect.objectContaining({ headers: undefined }));
  });

  it("keeps a generic message when the error body is not JSON", async () => {
    const headers = new Headers();
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: false,
        status: 502,
        headers,
        json: async () => {
          throw new SyntaxError("not json");
        },
      })
    );

    const error = await apiRequest("/api/ballot").catch((caught: unknown) => caught);
    expect((error as ApiError).status).toBe(502);
    expect((error as ApiError).code).toBe("unknown_error");
  });
});
