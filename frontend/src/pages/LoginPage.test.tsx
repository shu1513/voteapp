import { afterEach, describe, expect, it, vi } from "vitest";
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useMe } from "@voteapp/api-client";
import { LoginPage } from "./LoginPage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes, type ApiRoute } from "../test/mockApi";
import { ME_UNVERIFIED, ME_VERIFIED } from "../test/fixtures";
import { markWelcomeSeen } from "../lib/welcomeSeen";

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

function renderLogin(search = "") {
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
      { path: "/candidates/:candidateId", element: <p>candidate page</p> },
      { path: "/register", element: <p>register page</p> },
    ],
    `/login${search}`
  );
}

async function logIn() {
  const user = userEvent.setup();
  await user.type(await screen.findByLabelText("Email"), "voter@example.com");
  await user.type(screen.getByLabelText("Password"), "correct horse battery");
  await user.click(screen.getByRole("button", { name: "Log in" }));
}

const SAVED_PREFERENCE = {
  research_area_id: "a-env",
  slug: "environment",
  name: "Environment",
  description: null,
  rank: 1,
};

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
      "/api/me/research-area-preferences": { body: { preferences: [SAVED_PREFERENCE] } },
    });
    renderLogin();
    await logIn();
    expect(await screen.findByText("Saved ballot placeholder")).toBeInTheDocument();
  });

  it("respects a completed or skipped welcome without even checking preferences", async () => {
    markWelcomeSeen(ME_VERIFIED.user.email);
    // Preferences endpoint deliberately unmocked: requesting it would fail
    // the test, proving the seen flag short-circuits the lookup.
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

describe("LoginPage next-path return", () => {
  it("returns to the internal next path after login, skipping the welcome step", async () => {
    // Preferences endpoint deliberately unmocked: an explicit return path
    // must win over the onboarding detour without even checking.
    stubApiRoutes(sessionRoutes(ME_VERIFIED));
    renderLogin("?next=/candidates/c-1");

    await logIn();

    expect(await screen.findByText("candidate page")).toBeInTheDocument();
  });

  it("falls back to the ballot when next is missing", async () => {
    stubApiRoutes({
      ...sessionRoutes(ME_VERIFIED),
      "/api/me/research-area-preferences": { body: { preferences: [SAVED_PREFERENCE] } },
    });
    renderLogin();

    await logIn();

    expect(await screen.findByText("Saved ballot placeholder")).toBeInTheDocument();
  });

  it("ignores an external next instead of open-redirecting", async () => {
    stubApiRoutes({
      ...sessionRoutes(ME_VERIFIED),
      "/api/me/research-area-preferences": { body: { preferences: [SAVED_PREFERENCE] } },
    });
    // "//evil.example" is protocol-relative — a browser would leave the site.
    renderLogin(`?next=${encodeURIComponent("//evil.example/phish")}`);

    await logIn();

    expect(await screen.findByText("Saved ballot placeholder")).toBeInTheDocument();
  });

  it("forwards next to the register link", async () => {
    stubApiRoutes(sessionRoutes(ME_VERIFIED));
    renderLogin("?next=/candidates/c-1");

    expect(screen.getByRole("link", { name: "Create an account" })).toHaveAttribute(
      "href",
      "/register?next=%2Fcandidates%2Fc-1"
    );
  });
});
