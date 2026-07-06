import { afterEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import { createMemoryRouter, RouterProvider } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { StrictMode } from "react";
import { VerifyTokenPage } from "./VerifyTokenPage";

function renderVerify(initialEntry: string) {
  const queryClient = new QueryClient({ defaultOptions: { mutations: { retry: false } } });
  const router = createMemoryRouter(
    [
      {
        path: "/verify-email",
        element: (
          <VerifyTokenPage
            endpoint="/api/auth/verify-email"
            title="Verifying your email"
            successMessage="Verified."
          />
        ),
      },
      { path: "/login", element: <p /> },
    ],
    { initialEntries: [initialEntry] }
  );
  // StrictMode double-invokes effects in dev; the page must still post the
  // single-use token exactly once.
  return render(
    <StrictMode>
      <QueryClientProvider client={queryClient}>
        <RouterProvider router={router} />
      </QueryClientProvider>
    </StrictMode>
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("VerifyTokenPage", () => {
  it("posts the token exactly once, even under StrictMode double effects", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers(),
      json: async () => ({ status: "ok" }),
    });
    vi.stubGlobal("fetch", fetchMock);

    renderVerify("/verify-email?token=single-use-token");

    await waitFor(() => {
      expect(screen.getByText("Verified.")).toBeInTheDocument();
    });
    const verifyCalls = fetchMock.mock.calls.filter(([path]) => path === "/api/auth/verify-email");
    expect(verifyCalls).toHaveLength(1);
    expect(JSON.parse((verifyCalls[0][1] as { body: string }).body)).toEqual({ token: "single-use-token" });
  });

  it("shows the invalid-link state without posting when the token is missing", () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);

    renderVerify("/verify-email");

    expect(screen.getByText("Invalid link")).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
