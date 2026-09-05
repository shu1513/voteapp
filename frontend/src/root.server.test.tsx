// @vitest-environment node
import { afterEach, describe, expect, it, vi } from "vitest";
import { renderToString } from "react-dom/server";
import { QueryClientProvider, useQuery } from "@tanstack/react-query";
import { getQueryClient, loader } from "./root";

describe("root loader canonical-host redirect", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  function callLoader(request: Request): null {
    return loader({ request, params: {}, context: {} } as never);
  }

  it("does nothing when the redirect is not configured", () => {
    vi.stubEnv("CANONICAL_SITE_ORIGIN", "");
    expect(callLoader(new Request("https://voteapp-ssr.onrender.com/ballot"))).toBeNull();
  });

  it("throws a 301 to the canonical origin for a request that skipped the edge Worker", () => {
    vi.stubEnv("CANONICAL_SITE_ORIGIN", "https://electionssimplified.com");
    vi.stubEnv("EDGE_SHARED_SECRET", "edge-secret-value");
    let thrown: unknown;
    try {
      callLoader(new Request("https://voteapp-ssr.onrender.com/ballot?x=1"));
    } catch (error) {
      thrown = error;
    }
    expect(thrown).toBeInstanceOf(Response);
    const response = thrown as Response;
    expect(response.status).toBe(301);
    expect(response.headers.get("location")).toBe("https://electionssimplified.com/ballot?x=1");
  });

  it("serves a request that carries the Worker's proof", () => {
    vi.stubEnv("CANONICAL_SITE_ORIGIN", "https://electionssimplified.com");
    vi.stubEnv("EDGE_SHARED_SECRET", "edge-secret-value");
    const request = new Request("https://voteapp-ssr.onrender.com/ballot", {
      headers: { "x-edge-secret": "edge-secret-value" },
    });
    expect(callLoader(request)).toBeNull();
  });
});

// The SSR bundle keeps root.tsx loaded for the lifetime of the process, so a
// shared QueryClient would accumulate one never-gc'd cache entry (server
// gcTime is Infinity) per unique URL-derived query key — verification
// tokens, district ids — until the process runs out of memory.

function PageWithQuery({ token }: { token: string }) {
  useQuery({
    queryKey: ["verify-token", token],
    queryFn: () => Promise.resolve({ status: "ok" }),
  });
  return <div>page</div>;
}

describe("getQueryClient on the server", () => {
  it("returns a fresh client per call so requests cannot share a cache", () => {
    expect(getQueryClient()).not.toBe(getQueryClient());
  });

  it("keeps one request's URL-derived cache entries out of the next request's client", () => {
    const first = getQueryClient();
    renderToString(
      <QueryClientProvider client={first}>
        <PageWithQuery token="secret-token-a" />
      </QueryClientProvider>
    );
    // Rendering useQuery inserts the key into the active client even though
    // the queryFn never runs during SSR — this is what would leak.
    expect(first.getQueryCache().getAll()).toHaveLength(1);

    const second = getQueryClient();
    renderToString(
      <QueryClientProvider client={second}>
        <PageWithQuery token="secret-token-b" />
      </QueryClientProvider>
    );
    const keys = second
      .getQueryCache()
      .getAll()
      .map((query) => query.queryKey);
    expect(keys).toEqual([["verify-token", "secret-token-b"]]);
  });
});
