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

    // The mobile menu opens and repeats the destinations plus log out
    // (Headless UI gives menu entries the menuitem role).
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Menu" }));
    const items = screen.getAllByRole("menuitem");
    expect(items.map((item) => item.textContent)).toEqual([
      "My Elections",
      "My Draft",
      "My Candidates",
      "Settings",
      "Log out",
    ]);
    expect(screen.getByRole("menuitem", { name: "My Elections" })).toHaveAttribute("href", "/me/ballot");
  });
});
