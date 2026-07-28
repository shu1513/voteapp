import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createMemoryRouter, RouterProvider } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { HomePage } from "./HomePage";

function renderHome() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
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

beforeEach(() => {
  localStorage.clear();
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("HomePage legal gate (clickwrap)", () => {
  it("starts unchecked and keeps Search disabled until the box is checked", async () => {
    const user = userEvent.setup();
    renderHome();

    const checkbox = screen.getByRole("checkbox");
    const search = screen.getByRole("button", { name: "Search" });
    expect(checkbox).not.toBeChecked();

    await user.type(screen.getByLabelText("Your address"), "123 Main St, Austin, TX");
    expect(search).toBeDisabled();

    await user.click(checkbox);
    expect(search).toBeEnabled();
  });

  it("requires an address too — checkbox alone does not enable Search", async () => {
    const user = userEvent.setup();
    renderHome();

    await user.click(screen.getByRole("checkbox"));
    expect(screen.getByRole("button", { name: "Search" })).toBeDisabled();
  });

  it("never pre-checks the box, and stores nothing that could pre-check it later", async () => {
    const user = userEvent.setup();
    renderHome();

    // Accepting is an affirmative act every visit: nothing a previous visit
    // did may hand this visitor a ticked box, so acceptance is not persisted
    // at all. Storage staying empty is what keeps a later mount unchecked.
    await user.click(screen.getByRole("checkbox"));
    expect(screen.getByRole("checkbox")).toBeChecked();
    expect(localStorage.length).toBe(0);

    cleanup();
    renderHome();
    expect(screen.getByRole("checkbox")).not.toBeChecked();
  });

  it("links all three named agreements next to the checkbox", () => {
    renderHome();
    // Clickwrap adjacency: every document named in the checkbox copy must be
    // reviewable right next to it.
    expect(screen.getByRole("link", { name: "Terms of Use" })).toHaveAttribute("href", "/terms");
    expect(screen.getByRole("link", { name: "Privacy Policy" })).toHaveAttribute("href", "/privacy");
    expect(screen.getByRole("link", { name: "Disclaimer" })).toHaveAttribute("href", "/disclaimer");
  });
});
