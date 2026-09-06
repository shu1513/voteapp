import { afterEach, describe, expect, it, vi } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { useMe, type Me } from "./useMe";
import { useMyAccountDistricts } from "./useMyAccountDistricts";

/**
 * Cross-tab account transitions on a shared browser: another tab signs out
 * or signs in as someone else, and this tab's next GET /api/me comes back
 * with the new identity. The private caches must not survive that — and no
 * render may ever pair the new identity with the previous account's data.
 * Render histories are asserted, not just the final cache.
 */

afterEach(() => {
  vi.unstubAllGlobals();
});

const A: Me = { id: "user-a", email: "a@example.com", first_name: "A", email_verified: true, accepted_terms_version: "1.3", has_password: true };
const B: Me = { id: "user-b", email: "b@example.com", first_name: "B", email_verified: true, accepted_terms_version: "1.3", has_password: true };
/** a@example.com deleted its account and registered again: same email, new account. */
const A_AGAIN: Me = { ...A, id: "user-a2" };

function jsonResponse(body: unknown, status = 200) {
  return { ok: status < 400, status, headers: new Headers(), json: async () => body };
}

type Snapshot = { me: Me | null | undefined; districts: string[] | undefined };

/** /api/me answers from `identities` in order; /api/me/districts answers per identity. */
function setup(identities: Array<Me | null>) {
  let meCalls = 0;
  let current: Me | null = identities[0] ?? null;
  const fetchMock = vi.fn((input: RequestInfo | URL) => {
    const url = String(input);
    if (url.endsWith("/api/me")) {
      current = identities[Math.min(meCalls, identities.length - 1)] ?? null;
      meCalls += 1;
      return Promise.resolve(current ? jsonResponse({ user: current }) : jsonResponse({ error: { code: "unauthorized" } }, 401));
    }
    if (url.endsWith("/api/me/districts")) {
      const ids = current?.id === A.id ? ["a-1", "a-2"] : ["b-1"];
      return Promise.resolve(jsonResponse({ district_ids: ids }));
    }
    throw new Error(`unexpected fetch: ${url}`);
  });
  vi.stubGlobal("fetch", fetchMock);
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );
  const history: Snapshot[] = [];
  const hook = renderHook(
    () => {
      const { me } = useMe();
      const { districtIds } = useMyAccountDistricts();
      const snapshot: Snapshot = { me, districts: districtIds ? [...districtIds].sort() : undefined };
      history.push(snapshot);
      return snapshot;
    },
    { wrapper }
  );
  return { queryClient, history, fetchMock, ...hook };
}

async function settledAs(result: { current: Snapshot }, id: string | null, districts: string[] | undefined) {
  await waitFor(() => {
    expect(result.current.me === null ? null : result.current.me?.id).toBe(id);
    expect(result.current.districts).toEqual(districts);
  });
}

describe("account transition observed through GET /api/me", () => {
  it("A → B: B never renders with A's districts, and ends up with B's", async () => {
    const { queryClient, history, result } = setup([A, B]);
    await settledAs(result, A.id, ["a-1", "a-2"]);

    // Another tab switched accounts; this tab's identity goes stale and refetches.
    await queryClient.invalidateQueries({ queryKey: ["me"], exact: true });
    await settledAs(result, B.id, ["b-1"]);

    const leaked = history.filter((s) => s.me?.id === B.id && s.districts?.some((id) => id.startsWith("a-")));
    expect(leaked).toEqual([]);
  });

  it("A → signed out: the private cache is dropped even though the districts hook is now disabled", async () => {
    const { queryClient, history, result } = setup([A, null]);
    await settledAs(result, A.id, ["a-1", "a-2"]);

    await queryClient.invalidateQueries({ queryKey: ["me"], exact: true });
    await settledAs(result, null, undefined);

    expect(queryClient.getQueryData(["me", "districts"])).toBeUndefined();
    const leaked = history.filter((s) => s.me === null && s.districts !== undefined);
    expect(leaked).toEqual([]);
  });

  it("same email, different account (deleted, then registered again): the old account's cache is dropped", async () => {
    const { queryClient, history, result } = setup([A, A_AGAIN]);
    await settledAs(result, A.id, ["a-1", "a-2"]);

    await queryClient.invalidateQueries({ queryKey: ["me"], exact: true });
    await settledAs(result, A_AGAIN.id, ["b-1"]);

    const leaked = history.filter((s) => s.me?.id === A_AGAIN.id && s.districts?.some((id) => id.startsWith("a-")));
    expect(leaked).toEqual([]);
  });

  it("same account with a changed email: nothing is purged", async () => {
    const { queryClient, fetchMock, result } = setup([A, { ...A, email: "renamed@example.com" }]);
    await settledAs(result, A.id, ["a-1", "a-2"]);

    await queryClient.invalidateQueries({ queryKey: ["me"], exact: true });
    await waitFor(() => expect(result.current.me?.email).toBe("renamed@example.com"));

    expect(queryClient.getQueryData(["me", "districts"])).toEqual({ district_ids: ["a-1", "a-2"] });
    expect(fetchMock.mock.calls.filter((c) => String(c[0]).endsWith("/api/me/districts"))).toHaveLength(1);
  });

  it("same account refreshed: nothing is purged and no refetch of private data happens", async () => {
    const { queryClient, fetchMock, result } = setup([A, A]);
    await settledAs(result, A.id, ["a-1", "a-2"]);
    const districtFetches = () => fetchMock.mock.calls.filter((c) => String(c[0]).endsWith("/api/me/districts")).length;
    expect(districtFetches()).toBe(1);

    await queryClient.invalidateQueries({ queryKey: ["me"], exact: true });
    await settledAs(result, A.id, ["a-1", "a-2"]);

    expect(queryClient.getQueryData(["me", "districts"])).toEqual({ district_ids: ["a-1", "a-2"] });
    expect(districtFetches()).toBe(1);
  });
});
