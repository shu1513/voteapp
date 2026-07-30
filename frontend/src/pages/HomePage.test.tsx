import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createMemoryRouter, RouterProvider } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { PRE_SEARCH_AGREEMENT_PARAGRAPHS, TERMS_VERSION } from "@voteapp/api-client";
import { HomePage } from "./HomePage";

function renderHome() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const router = createMemoryRouter([
    { path: "/", element: <HomePage /> },
    { path: "/ballot", element: <p /> },
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

    await user.type(
      screen.getByLabelText("Enter your address to see the elections you can vote in"),
      "123 Main St, Austin, TX"
    );
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

  it("keeps the summarized label's full wording one click away", async () => {
    const user = userEvent.setup();
    renderHome();

    // The label is a summary, so nothing it dropped may be unreachable.
    for (const paragraph of PRE_SEARCH_AGREEMENT_PARAGRAPHS) {
      expect(screen.queryByText(paragraph)).not.toBeInTheDocument();
    }

    await user.click(screen.getByRole("button", { name: "Read the full agreement" }));

    const dialog = screen.getByRole("dialog");
    for (const paragraph of PRE_SEARCH_AGREEMENT_PARAGRAPHS) {
      expect(dialog).toHaveTextContent(paragraph);
    }
  });

  it("lets the visitor agree from inside the full agreement", async () => {
    const user = userEvent.setup();
    renderHome();

    await user.click(screen.getByRole("button", { name: "Read the full agreement" }));
    await user.click(screen.getByRole("button", { name: "I agree" }));

    expect(screen.getByRole("checkbox")).toBeChecked();
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("leaves the box untouched when the full agreement is dismissed", async () => {
    const user = userEvent.setup();
    renderHome();

    await user.click(screen.getByRole("button", { name: "Read the full agreement" }));
    await user.click(screen.getByRole("button", { name: "Close" }));

    // Opening and closing a dialog is not agreeing to anything.
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(screen.getByRole("checkbox")).not.toBeChecked();
  });

  it("sends the accepted terms version with the search, because the endpoint enforces it too", async () => {
    const fetchMock = vi.fn().mockImplementation(async (path: string) => ({
      ok: true,
      status: 200,
      headers: new Headers(),
      json: async () =>
        path === "/api/address/resolve"
          ? { matched_address: "123 Main St", address_match_count: 1, districts: [] }
          : { user: null },
    }));
    vi.stubGlobal("fetch", fetchMock);
    const user = userEvent.setup();
    renderHome();

    await user.type(
      screen.getByLabelText("Enter your address to see the elections you can vote in"),
      "123 Main St, Austin, TX"
    );
    await user.click(screen.getByRole("checkbox"));
    await user.click(screen.getByRole("button", { name: "Search" }));

    await waitFor(() => {
      const resolveCall = fetchMock.mock.calls.find(([path]) => path === "/api/address/resolve");
      expect(resolveCall).toBeDefined();
      expect(JSON.parse((resolveCall as [string, { body: string }])[1].body)).toEqual({
        address: "123 Main St, Austin, TX",
        accepted_terms_version: TERMS_VERSION,
      });
    });
  });
});
