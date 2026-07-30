import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createMemoryRouter, RouterProvider } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { PRE_SEARCH_CHECKBOX_LABEL, PRIVACY_NOTICE } from "@voteapp/api-client";
import { HomePage } from "./HomePage";

function renderHome() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const router = createMemoryRouter([
    { path: "/", element: <HomePage /> },
    { path: "/disclaimer", element: <p /> },
    { path: "/terms", element: <p /> },
    { path: "/privacy", element: <p /> },
  ]);
  return render(
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  );
}

async function openGate(user: ReturnType<typeof userEvent.setup>) {
  await user.type(
    screen.getByLabelText("Enter your address to see the elections you can vote in:"),
    "123 Main St, Austin, TX"
  );
  await user.click(screen.getByRole("button", { name: "Search" }));
}

beforeEach(() => localStorage.clear());
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("HomePage legal gate", () => {
  it("keeps legal copy hidden until Search opens the review dialog", async () => {
    const user = userEvent.setup();
    renderHome();

    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(screen.queryByText(PRIVACY_NOTICE)).not.toBeInTheDocument();
    await openGate(user);

    expect(screen.getByRole("dialog", { name: "Review and agree" })).toBeInTheDocument();
    expect(screen.getByText(PRIVACY_NOTICE)).toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: PRE_SEARCH_CHECKBOX_LABEL })).not.toBeChecked();
    expect(screen.getByRole("button", { name: "Agree and search" })).toBeDisabled();
  });

  it("requires explicit assent to every named agreement before search", async () => {
    const user = userEvent.setup();
    renderHome();
    await openGate(user);

    expect(screen.getByRole("link", { name: "Terms of Use" })).toHaveAttribute("href", "/terms");
    expect(screen.getByRole("link", { name: "Privacy Policy" })).toHaveAttribute("href", "/privacy");
    expect(screen.getByRole("link", { name: "Disclaimer" })).toHaveAttribute("href", "/disclaimer");
    expect(screen.getByText(/binding individual arbitration/i)).toBeInTheDocument();

    await user.click(screen.getByRole("checkbox"));
    expect(screen.getByRole("button", { name: "Agree and search" })).toBeEnabled();
  });

  it("Cancel closes the dialog and never carries a checked box forward", async () => {
    const user = userEvent.setup();
    renderHome();
    await openGate(user);
    await user.click(screen.getByRole("checkbox"));
    await user.click(screen.getByRole("button", { name: "Cancel" }));

    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Search" }));
    expect(screen.getByRole("checkbox")).not.toBeChecked();
    expect(localStorage.length).toBe(0);
  });
});
