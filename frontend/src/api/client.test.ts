import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, apiRequest } from "./client";

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
