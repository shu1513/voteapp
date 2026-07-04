import { afterEach, describe, expect, it, vi } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { useLogout } from "./useMe";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("useLogout", () => {
  it("purges account-scoped cache entries so the next login cannot see them", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        headers: new Headers(),
        json: async () => ({ status: "ok" }),
      })
    );
    const queryClient = new QueryClient();
    // Simulate a logged-in session with cached account data.
    queryClient.setQueryData(["me"], { email: "a@example.com", first_name: "A", email_verified: true });
    queryClient.setQueryData(["me", "ballot"], { districts: [{ id: "d1" }], elections: [] });
    queryClient.setQueryData(["election", "e1"], { id: "e1" });

    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    const { result } = renderHook(() => useLogout(), { wrapper });
    result.current.mutate();

    await waitFor(() => {
      expect(result.current.isSuccess).toBe(true);
    });
    // Identity reset to logged-out, private ballot gone (shared-browser
    // leak), public data gone too (harmless but consistent).
    expect(queryClient.getQueryData(["me"])).toBeNull();
    expect(queryClient.getQueryData(["me", "ballot"])).toBeUndefined();
    expect(queryClient.getQueryData(["election", "e1"])).toBeUndefined();
  });
});
