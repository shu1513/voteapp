import { afterEach, describe, expect, it, vi } from "vitest";
import { loadFromApi } from "./loadFromApi";

function stubFetch(impl: (url: string, init?: RequestInit) => Promise<unknown>) {
  const fetchMock = vi.fn(impl);
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

function incoming(headers: Record<string, string> = {}) {
  return new Request("http://ssr.local/elections/e-1", { headers });
}

async function thrownBy(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (error) {
    return error;
  }
  throw new Error("expected promise to reject");
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
});

describe("loadFromApi", () => {
  it("returns the parsed body on success", async () => {
    stubFetch(async () => ({ ok: true, status: 200, json: async () => ({ id: "e-1" }) }));
    await expect(loadFromApi("/api/elections/e-1", incoming())).resolves.toEqual({ id: "e-1" });
  });

  it("relays the trusted client-IP header when configured and present", async () => {
    vi.stubEnv("ADDRESS_API_TRUSTED_CLIENT_IP_HEADER", "x-real-client-ip");
    const fetchMock = stubFetch(async () => ({ ok: true, status: 200, json: async () => ({}) }));

    await loadFromApi("/api/elections/e-1", incoming({ "x-real-client-ip": "203.0.113.9" }));

    const init = fetchMock.mock.calls[0][1] as RequestInit;
    expect(new Headers(init.headers).get("x-real-client-ip")).toBe("203.0.113.9");
  });

  it("sends no relay header when the env var is unset", async () => {
    const fetchMock = stubFetch(async () => ({ ok: true, status: 200, json: async () => ({}) }));

    await loadFromApi("/api/elections/e-1", incoming({ "x-real-client-ip": "203.0.113.9" }));

    const init = fetchMock.mock.calls[0][1] as RequestInit;
    expect([...new Headers(init.headers).keys()]).toEqual([]);
  });

  it("never forwards cookies even when the incoming request has them", async () => {
    vi.stubEnv("ADDRESS_API_TRUSTED_CLIENT_IP_HEADER", "x-real-client-ip");
    const fetchMock = stubFetch(async () => ({ ok: true, status: 200, json: async () => ({}) }));

    await loadFromApi(
      "/api/elections/e-1",
      incoming({ cookie: "session=secret", "x-real-client-ip": "203.0.113.9" })
    );

    const sent = new Headers((fetchMock.mock.calls[0][1] as RequestInit).headers);
    expect(sent.get("cookie")).toBeNull();
    expect([...sent.keys()]).toEqual(["x-real-client-ip"]);
  });

  it.each([404, 400])("maps upstream %d to a thrown 404 Response", async (status) => {
    stubFetch(async () => ({ ok: false, status, json: async () => ({}) }));
    const error = await thrownBy(loadFromApi("/api/elections/x", incoming()));
    expect(error).toBeInstanceOf(Response);
    expect((error as Response).status).toBe(404);
  });

  it("passes upstream 429 through with its retry-after", async () => {
    stubFetch(async () => ({
      ok: false,
      status: 429,
      headers: new Headers({ "retry-after": "42" }),
      json: async () => ({}),
    }));
    const error = await thrownBy(loadFromApi("/api/elections/x", incoming()));
    expect(error).toBeInstanceOf(Response);
    expect((error as Response).status).toBe(429);
    expect((error as Response).headers.get("retry-after")).toBe("42");
  });

  it("maps other upstream failures to a thrown 502 Response", async () => {
    stubFetch(async () => ({ ok: false, status: 500, json: async () => ({}) }));
    const error = await thrownBy(loadFromApi("/api/elections/x", incoming()));
    expect(error).toBeInstanceOf(Response);
    expect((error as Response).status).toBe(502);
  });

  it("maps a fetch timeout to a thrown 504 Response", async () => {
    stubFetch(async () => {
      throw new DOMException("The operation timed out.", "TimeoutError");
    });
    const error = await thrownBy(loadFromApi("/api/elections/x", incoming()));
    expect(error).toBeInstanceOf(Response);
    expect((error as Response).status).toBe(504);
  });

  it("maps a body-read timeout to a thrown 504 Response", async () => {
    stubFetch(async () => ({
      ok: true,
      status: 200,
      json: async () => {
        throw new DOMException("The operation timed out.", "TimeoutError");
      },
    }));
    const error = await thrownBy(loadFromApi("/api/elections/x", incoming()));
    expect(error).toBeInstanceOf(Response);
    expect((error as Response).status).toBe(504);
  });

  it("rethrows non-timeout fetch failures untouched", async () => {
    const refused = new TypeError("fetch failed");
    stubFetch(async () => {
      throw refused;
    });
    await expect(loadFromApi("/api/elections/x", incoming())).rejects.toBe(refused);
  });
});
