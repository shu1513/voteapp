import { vi } from "vitest";

type StubResult = { status?: number; body: unknown };
export type ApiRoute = StubResult | ((url: URL, init?: RequestInit) => StubResult);

export function apiError(status: number, code: string, message: string): StubResult {
  return { status, body: { error: { code, message } } };
}

function toResponse({ status = 200, body }: StubResult) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: new Headers(),
    json: async () => body,
  };
}

/**
 * Stubs global fetch with a pathname-keyed route table. Unmatched paths
 * reject loudly so a page quietly calling an unmocked endpoint fails the
 * test instead of pinning its query in pending.
 */
export function stubApiRoutes(routes: Record<string, ApiRoute>) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(String(input), "http://localhost");
    const route = routes[url.pathname];
    if (!route) {
      throw new Error(`Unmocked API call: ${url.pathname}`);
    }
    return toResponse(typeof route === "function" ? route(url, init) : route);
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}
