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
    expect(await screen.findByRole("link", { name: "My ballot" })).toHaveAttribute("href", "/me/ballot");
    expect(screen.getByRole("link", { name: "Following" })).toHaveAttribute("href", "/me/follows");
    expect(screen.getByRole("link", { name: "Settings" })).toHaveAttribute("href", "/me/settings");

    // The mobile menu opens and repeats the destinations plus log out
    // (Headless UI gives menu entries the menuitem role).
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: /Hi Sam/ }));
    const items = screen.getAllByRole("menuitem");
    expect(items.map((item) => item.textContent)).toEqual([
      "My ballot",
      "Following",
      "Settings",
      "Log out",
    ]);
    expect(screen.getByRole("menuitem", { name: "My ballot" })).toHaveAttribute("href", "/me/ballot");
  });
});
