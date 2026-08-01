import { afterEach, describe, expect, it, vi } from "vitest";
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useMe } from "@voteapp/api-client";
import { LoginPage } from "./LoginPage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes, type ApiRoute } from "../test/mockApi";
import { ME_UNVERIFIED, ME_VERIFIED } from "../test/fixtures";
import { markWelcomeSkipped } from "../lib/welcomeSkip";

// In the app the header keeps the ["me"] query mounted, which is what the
// login mutation's invalidateQueries awaits to refresh identity. The probe
// stands in for the header so the redirect logic sees the fresh user.
function MeProbe() {
  useMe();
  return null;
}

/** /api/me is 401 until /api/auth/login succeeds, like the real backend. */
function sessionRoutes(me: unknown): Record<string, ApiRoute> {
  let loggedIn = false;
  return {
    "/api/auth/login": () => {
      loggedIn = true;
      return { body: { status: "ok" } };
    },
    "/api/me": () => (loggedIn ? { body: me } : apiError(401, "unauthorized", "Not logged in")),
  };
}

function renderLogin() {
  return renderRoutes(
    [
      {
        path: "/login",
        element: (
          <>
            <MeProbe />
            <LoginPage />
          </>
        ),
      },
      { path: "/me/welcome", element: <p>Welcome placeholder</p> },
      { path: "/me/ballot", element: <p>Saved ballot placeholder</p> },
    ],
    "/login"
  );
}

async function logIn() {
  const user = userEvent.setup();
  await user.type(await screen.findByLabelText("Email"), "voter@example.com");
  await user.type(screen.getByLabelText("Password"), "correct horse battery");
  await user.click(screen.getByRole("button", { name: "Log in" }));
}

afterEach(() => {
  vi.unstubAllGlobals();
  window.localStorage.clear();
});

describe("LoginPage first-login onboarding redirect", () => {
  it("sends a verified user with no saved areas to the welcome step", async () => {
    stubApiRoutes({
      ...sessionRoutes(ME_VERIFIED),
      "/api/me/research-area-preferences": { body: { preferences: [] } },
    });
    renderLogin();
    await logIn();
    expect(await screen.findByText("Welcome placeholder")).toBeInTheDocument();
  });

  it("sends a user with saved areas straight to the ballot", async () => {
    stubApiRoutes({
      ...sessionRoutes(ME_VERIFIED),
      "/api/me/research-area-preferences": {
        body: { preferences: [{ research_area_id: "a-env", slug: "environment", name: "Environment", description: null, rank: 1 }] },
      },
    });
    renderLogin();
    await logIn();
    expect(await screen.findByText("Saved ballot placeholder")).toBeInTheDocument();
  });

  it("respects a remembered skip without even checking preferences", async () => {
    markWelcomeSkipped(ME_VERIFIED.user.email);
    // Preferences endpoint deliberately unmocked: requesting it would fail
    // the test, proving the skip short-circuits the lookup.
    stubApiRoutes(sessionRoutes(ME_VERIFIED));
    renderLogin();
    await logIn();
    expect(await screen.findByText("Saved ballot placeholder")).toBeInTheDocument();
  });

  it("sends unverified users to the ballot, where the verification notice lives", async () => {
    stubApiRoutes(sessionRoutes(ME_UNVERIFIED));
    renderLogin();
    await logIn();
    expect(await screen.findByText("Saved ballot placeholder")).toBeInTheDocument();
  });
});
