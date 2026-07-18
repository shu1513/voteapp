import { afterEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createMemoryRouter, RouterProvider } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { RegisterPage } from "./RegisterPage";
import { TERMS_VERSION } from "@voteapp/api-client";

function renderRegister() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const router = createMemoryRouter([
    { path: "/", element: <RegisterPage /> },
    { path: "/login", element: <p /> },
    { path: "/terms", element: <p /> },
    { path: "/privacy", element: <p /> },
    { path: "/disclaimer", element: <p /> },
  ]);
  return render(
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("RegisterPage clickwrap", () => {
  it("keeps Create account disabled until the signup box is checked", async () => {
    const user = userEvent.setup();
    renderRegister();

    await user.type(screen.getByLabelText("Email"), "voter@example.com");
    await user.type(screen.getByLabelText("Password"), "correct horse battery staple");
    expect(screen.getByRole("button", { name: "Create account" })).toBeDisabled();

    await user.click(screen.getByRole("checkbox"));
    expect(screen.getByRole("button", { name: "Create account" })).toBeEnabled();
  });

  it("sends accepted_terms_version with the register payload and shows the check-email state", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers(),
      json: async () => ({ status: "ok" }),
    });
    vi.stubGlobal("fetch", fetchMock);
    const user = userEvent.setup();
    renderRegister();

    await user.type(screen.getByLabelText("Email"), "voter@example.com");
    await user.type(screen.getByLabelText(/First Name/), "Val");
    await user.type(screen.getByLabelText("Password"), "correct horse battery staple");
    await user.click(screen.getByRole("checkbox"));
    await user.click(screen.getByRole("button", { name: "Create account" }));

    await waitFor(() => {
      expect(screen.getByText("Check your email")).toBeInTheDocument();
    });
    const [path, init] = fetchMock.mock.calls[0] as [string, { body: string }];
    expect(path).toBe("/api/auth/register");
    expect(JSON.parse(init.body)).toEqual({
      email: "voter@example.com",
      password: "correct horse battery staple",
      accepted_terms_version: TERMS_VERSION,
      first_name: "Val",
    });
  });
});
