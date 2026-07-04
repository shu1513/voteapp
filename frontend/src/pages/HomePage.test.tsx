import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createMemoryRouter, RouterProvider } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { HomePage } from "./HomePage";
import { PRE_SEARCH_ACCEPTANCE_STORAGE_KEY } from "../legal/copy";

function renderHome() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const router = createMemoryRouter([{ path: "/", element: <HomePage /> }, { path: "/disclaimer", element: <p /> }]);
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

    await user.type(screen.getByLabelText("Home address"), "123 Main St, Austin, TX");
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

  it("persists acceptance per terms version in localStorage", async () => {
    const user = userEvent.setup();
    renderHome();

    await user.click(screen.getByRole("checkbox"));
    expect(localStorage.getItem(PRE_SEARCH_ACCEPTANCE_STORAGE_KEY)).toBe("true");

    await user.click(screen.getByRole("checkbox"));
    expect(localStorage.getItem(PRE_SEARCH_ACCEPTANCE_STORAGE_KEY)).toBeNull();
  });

  it("pre-checks the box for a visitor who already accepted this version", () => {
    localStorage.setItem(PRE_SEARCH_ACCEPTANCE_STORAGE_KEY, "true");
    renderHome();
    expect(screen.getByRole("checkbox")).toBeChecked();
  });

  it("links to the full disclaimer next to the checkbox", () => {
    renderHome();
    expect(screen.getByRole("link", { name: "Read the full Disclaimer" })).toHaveAttribute(
      "href",
      "/disclaimer"
    );
  });
});
