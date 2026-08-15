import { afterEach, describe, expect, it, vi } from "vitest";
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { App } from "./App";
import { renderRoutes } from "./test/render";
import { apiError, stubApiRoutes } from "./test/mockApi";
import { ME_VERIFIED } from "./test/fixtures";

function renderApp() {
  return renderRoutes(
    [
      {
        path: "/",
        element: <App />,
        children: [{ index: true, element: <p>home content</p> }],
      },
    ],
    "/"
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("App account nav", () => {
  it("shows log in and sign up when logged out", async () => {
    stubApiRoutes({ "/api/me": apiError(401, "unauthorized", "Not logged in") });
    renderApp();
    expect(await screen.findByRole("link", { name: "Log in" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Sign up" })).toBeInTheDocument();
    // A first-time visitor has no draft to link to — the nav stays clean
    // until they've seen a ballot or made a pick.
    expect(screen.queryByRole("link", { name: "My Ballot Draft" })).not.toBeInTheDocument();
  });

  it("shows the draft link once the guest has looked at a ballot", async () => {
    stubApiRoutes({ "/api/me": apiError(401, "unauthorized", "Not logged in") });
    window.localStorage.setItem(
      "voteapp_ballot_draft",
      JSON.stringify({ v: 1, district_ids: ["d-1"], target: null, choices: {} })
    );
    window.dispatchEvent(new StorageEvent("storage", { key: "voteapp_ballot_draft" }));
    renderApp();
    expect(await screen.findByRole("link", { name: "My Ballot Draft" })).toHaveAttribute(
      "href",
      "/draft"
    );
    window.localStorage.clear();
    window.dispatchEvent(new StorageEvent("storage", { key: "voteapp_ballot_draft" }));
  });

  it("renders inline links plus a small-screen menu with the same destinations", async () => {
    stubApiRoutes({ "/api/me": { body: ME_VERIFIED } });
    renderApp();

    // Inline (sm+) links.
    expect(await screen.findByRole("link", { name: "My Elections" })).toHaveAttribute("href", "/me/ballot");
    // Plain "My Draft" (no counter) while no pick is made / progress unknown.
    expect(screen.getByRole("link", { name: "My Draft" })).toHaveAttribute("href", "/me/picks");
    expect(screen.getByRole("link", { name: "My Candidates" })).toHaveAttribute("href", "/me/follows");
    expect(screen.getByRole("link", { name: "Settings" })).toHaveAttribute("href", "/me/settings");

    // The greeting sits beside the logo as plain text, not a link or button.
    const greeting = screen.getByText("Hi Sam");
    expect(greeting.closest("a")).toBeNull();
    expect(greeting.closest("button")).toBeNull();

    // The mobile menu opens and repeats the destinations — no Log out here,
    // it lives in Settings → Sessions, and My Draft sits last
    // (Headless UI gives menu entries the menuitem role).
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Menu" }));
    const items = screen.getAllByRole("menuitem");
    expect(items.map((item) => item.textContent)).toEqual([
      "My Elections",
      "My Candidates",
      "Settings",
      "My Draft",
    ]);
    expect(screen.getByRole("menuitem", { name: "My Elections" })).toHaveAttribute("href", "/me/ballot");
  });
});
