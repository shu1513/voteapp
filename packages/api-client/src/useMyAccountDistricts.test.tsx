import { afterEach, describe, expect, it, vi } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";
import type { Me } from "./useMe";
import { useMyAccountDistricts } from "./useMyAccountDistricts";

afterEach(() => {
  vi.unstubAllGlobals();
});

function jsonResponse(body: unknown) {
  return { ok: true, status: 200, headers: new Headers(), json: async () => body };
}

function renderWithSession(me: Me | null) {
  const fetchMock = vi.fn((input: RequestInfo | URL) => {
    const url = String(input);
    if (url.endsWith("/api/me/districts")) {
      return Promise.resolve(jsonResponse({ district_ids: ["d1", "d2"] }));
    }
    throw new Error(`unexpected fetch: ${url}`);
  });
  vi.stubGlobal("fetch", fetchMock);
  const queryClient = new QueryClient();
  // Seed the identity instead of fetching it: the gate under test is this
  // hook's, not useMe's.
  queryClient.setQueryData(["me"], me);
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );
  return { fetchMock, ...renderHook(() => useMyAccountDistricts(), { wrapper }) };
}

const VERIFIED: Me = {
  id: "user-a",
  email: "a@example.com",
  first_name: "A",
  email_verified: true,
  accepted_terms_version: "1.3",
  has_password: true,
};

describe("useMyAccountDistricts", () => {
  it("fetches for verified users and returns the id set", async () => {
    const { result } = renderWithSession(VERIFIED);
    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
    });
    expect(result.current.districtIds).toEqual(new Set(["d1", "d2"]));
  });

  it("never fetches for unverified users and settles as unknown", async () => {
    const { result, fetchMock } = renderWithSession({ ...VERIFIED, email_verified: false });
    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
    });
    expect(result.current.districtIds).toBeUndefined();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("treats an empty district list as unknown, not as an empty set", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(jsonResponse({ district_ids: [] })));
    const queryClient = new QueryClient();
    queryClient.setQueryData(["me"], VERIFIED);
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const { result } = renderHook(() => useMyAccountDistricts(), { wrapper });
    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
    });
    // No address on file: the gate must show the nudge, not a clean
    // "known-foreign" read-only page.
    expect(result.current.districtIds).toBeUndefined();
  });
});
