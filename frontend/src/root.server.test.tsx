// @vitest-environment node
import { describe, expect, it } from "vitest";
import { renderToString } from "react-dom/server";
import { QueryClientProvider, useQuery } from "@tanstack/react-query";
import { getQueryClient } from "./root";

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
