import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createMemoryRouter, RouterProvider } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { PRE_SEARCH_AGREEMENT_PARAGRAPHS, TERMS_VERSION } from "@voteapp/api-client";
import { HomePage } from "./HomePage";

const ADDRESS_LABEL = "Enter your address to see the elections you can vote in";
const STORAGE_KEY = "voteapp_terms_acceptance";
const NINETY_DAYS_MS = 90 * 24 * 60 * 60 * 1000;

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

function stubResolveFetch() {
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
  return fetchMock;
}

async function typeAddress(user: ReturnType<typeof userEvent.setup>) {
  await user.type(screen.getByLabelText(ADDRESS_LABEL), "123 Main St, Austin, TX");
}

beforeEach(() => {
  localStorage.clear();
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("HomePage pre-search clickwrap", () => {
  it("keeps the landing page free of the gate until Search is pressed", async () => {
    const user = userEvent.setup();
    renderHome();

    // The wall of legal text is what moved off the page; a first-time visitor
    // sees an address field, not an agreement.
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();

    await typeAddress(user);
    await user.click(screen.getByRole("button", { name: "Search" }));

    expect(screen.getByRole("dialog")).toBeInTheDocument();
  });

  it("keeps the privacy note beside the address field, where collection starts", () => {
    renderHome();
    // The autocomplete forwards what is typed before Search is ever pressed,
    // so this notice may never move into the dialog.
    expect(
      screen.getByText(
        /Your address is used only to find your voting districts\. We don’t save it to your account or sell it; lookup data may be temporarily cached for up to 14 days\./
      )
    ).toBeInTheDocument();
    // New tab: the address lives in page state, so reading the policy in this
    // tab would return the visitor to an empty field.
    const notice = screen.getByRole("link", { name: "Privacy notice" });
    expect(notice).toHaveAttribute("href", "/privacy");
    expect(notice).toHaveAttribute("target", "_blank");
  });

  it("explains why a full address is needed without treating the explanation as consent", async () => {
    const user = userEvent.setup();
    renderHome();

    const trigger = screen.getByRole("button", { name: "Why do we need the full address?" });
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();

    await user.click(trigger);

    const dialog = screen.getByRole("dialog", { name: "Why do we need the full address?" });
    expect(dialog).toHaveTextContent("ZIP codes are designed for mail delivery, not elections.");
    expect(dialog).toHaveTextContent("A single ZIP code can contain multiple voting districts");
    expect(dialog).toHaveTextContent("homes on the same street");
    expect(dialog).toHaveTextContent("We don’t save it to your account or sell it");
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();

    const privacyPolicy = screen.getByRole("link", { name: "Read our Privacy Policy" });
    expect(privacyPolicy).toHaveAttribute("href", "/privacy");
    expect(privacyPolicy).toHaveAttribute("target", "_blank");

    await user.click(screen.getByRole("button", { name: "Got it" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("does not open the dialog without an address", async () => {
    const user = userEvent.setup();
    renderHome();

    await user.click(screen.getByRole("button", { name: "Search" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("opens with an empty box, the action disabled, and every document named", async () => {
    const user = userEvent.setup();
    renderHome();

    await typeAddress(user);
    await user.click(screen.getByRole("button", { name: "Search" }));

    expect(screen.getByRole("checkbox")).not.toBeChecked();
    expect(screen.getByRole("button", { name: "Agree and search" })).toBeDisabled();
    for (const paragraph of PRE_SEARCH_AGREEMENT_PARAGRAPHS) {
      expect(screen.getByRole("dialog")).toHaveTextContent(paragraph);
    }
    // Clickwrap adjacency, and opening one must not discard the dialog.
    for (const [name, href] of [
      ["Terms of Use", "/terms"],
      ["Privacy Policy", "/privacy"],
      ["Disclaimer", "/disclaimer"],
    ] as const) {
      const link = screen.getByRole("link", { name });
      expect(link).toHaveAttribute("href", href);
      expect(link).toHaveAttribute("target", "_blank");
    }
  });

  it("enables the action only once the box is ticked", async () => {
    const user = userEvent.setup();
    renderHome();

    await typeAddress(user);
    await user.click(screen.getByRole("button", { name: "Search" }));
    await user.click(screen.getByRole("checkbox"));

    expect(screen.getByRole("button", { name: "Agree and search" })).toBeEnabled();
  });

  it("cancels without searching, without storing, and without losing the address", async () => {
    const fetchMock = stubResolveFetch();
    const user = userEvent.setup();
    renderHome();

    await typeAddress(user);
    await user.click(screen.getByRole("button", { name: "Search" }));
    await user.click(screen.getByRole("checkbox"));
    await user.click(screen.getByRole("button", { name: "Cancel" }));

    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(fetchMock.mock.calls.filter(([path]) => path === "/api/address/resolve")).toHaveLength(0);
    expect(localStorage.getItem(STORAGE_KEY)).toBeNull();
    // Cancelling the terms is not a request to retype an address.
    expect(screen.getByLabelText(ADDRESS_LABEL)).toHaveValue("123 Main St, Austin, TX");
  });

  it("re-opens with an empty box after a cancel", async () => {
    const user = userEvent.setup();
    renderHome();

    await typeAddress(user);
    await user.click(screen.getByRole("button", { name: "Search" }));
    await user.click(screen.getByRole("checkbox"));
    await user.click(screen.getByRole("button", { name: "Cancel" }));
    await user.click(screen.getByRole("button", { name: "Search" }));

    expect(screen.getByRole("checkbox")).not.toBeChecked();
  });

  it("searches on agreement and sends the accepted version", async () => {
    const fetchMock = stubResolveFetch();
    const user = userEvent.setup();
    renderHome();

    await typeAddress(user);
    await user.click(screen.getByRole("button", { name: "Search" }));
    await user.click(screen.getByRole("checkbox"));
    await user.click(screen.getByRole("button", { name: "Agree and search" }));

    await waitFor(() => {
      const resolveCall = fetchMock.mock.calls.find(([path]) => path === "/api/address/resolve");
      expect(resolveCall).toBeDefined();
      expect(JSON.parse((resolveCall as [string, { body: string }])[1].body)).toEqual({
        address: "123 Main St, Austin, TX",
        accepted_terms_version: TERMS_VERSION,
      });
    });
  });

  it("remembers the acceptance so the next search goes straight through", async () => {
    stubResolveFetch();
    const user = userEvent.setup();
    renderHome();

    await typeAddress(user);
    await user.click(screen.getByRole("button", { name: "Search" }));
    await user.click(screen.getByRole("checkbox"));
    await user.click(screen.getByRole("button", { name: "Agree and search" }));

    await waitFor(() => {
      expect(localStorage.getItem(STORAGE_KEY)).not.toBeNull();
    });

    cleanup();
    renderHome();
    await typeAddress(user);
    await user.click(screen.getByRole("button", { name: "Search" }));

    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("asks again once the stored acceptance expires", async () => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ version: TERMS_VERSION, acceptedAt: Date.now() - NINETY_DAYS_MS - 1000 })
    );
    const user = userEvent.setup();
    renderHome();

    await typeAddress(user);
    await user.click(screen.getByRole("button", { name: "Search" }));

    expect(screen.getByRole("dialog")).toBeInTheDocument();
    expect(screen.getByRole("checkbox")).not.toBeChecked();
  });

  it("asks again when the stored acceptance is for superseded terms", async () => {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ version: "0.9", acceptedAt: Date.now() })
    );
    const user = userEvent.setup();
    renderHome();

    await typeAddress(user);
    await user.click(screen.getByRole("button", { name: "Search" }));

    expect(screen.getByRole("dialog")).toBeInTheDocument();
  });

  it("asks again when the stored acceptance is unreadable", async () => {
    localStorage.setItem(STORAGE_KEY, "{not json");
    const user = userEvent.setup();
    renderHome();

    await typeAddress(user);
    await user.click(screen.getByRole("button", { name: "Search" }));

    // Fails closed: showing the terms once more costs a click, skipping them
    // wrongly costs the agreement.
    expect(screen.getByRole("dialog")).toBeInTheDocument();
  });
});
