import { afterEach, describe, expect, it, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { WelcomePage } from "./WelcomePage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { ME_UNVERIFIED, ME_VERIFIED } from "../test/fixtures";
import { hasSkippedWelcome } from "../lib/welcomeSkip";

const CATALOG = {
  research_areas: [
    { id: "a-env", slug: "environment", name: "Environment", description: "Climate, energy, land use." },
    { id: "a-housing", slug: "housing", name: "Housing", description: null },
  ],
};

function renderWelcome() {
  return renderRoutes(
    [
      { path: "/me/welcome", element: <WelcomePage /> },
      { path: "/login", element: <p>Login placeholder</p> },
      { path: "/me/ballot", element: <p>Saved ballot placeholder</p> },
    ],
    "/me/welcome"
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
  window.localStorage.clear();
});

describe("WelcomePage", () => {
  it("redirects logged-out visitors to login", async () => {
    stubApiRoutes({ "/api/me": apiError(401, "unauthorized", "Not logged in") });
    renderWelcome();
    expect(await screen.findByText("Login placeholder")).toBeInTheDocument();
  });

  it("redirects unverified users to the saved ballot", async () => {
    stubApiRoutes({ "/api/me": { body: ME_UNVERIFIED } });
    renderWelcome();
    expect(await screen.findByText("Saved ballot placeholder")).toBeInTheDocument();
  });

  it("saves picked issues as ranked preferences in one PUT, then continues to the ballot", async () => {
    const user = userEvent.setup();
    const fetchMock = stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/research-areas": { body: CATALOG },
      "/api/me/research-area-preferences": (_url, init) => {
        expect(init?.method).toBe("PUT");
        return { body: { preferences: [] } };
      },
    });
    renderWelcome();

    // Save is disabled until something is picked — an empty save would be a
    // no-op that still skips the step.
    const saveButton = await screen.findByRole("button", { name: "Save and continue" });
    expect(saveButton).toBeDisabled();

    // Card descriptions are visible, not tucked into a tooltip.
    expect(screen.getByText("Climate, energy, land use.")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /Housing/ }));
    await user.click(screen.getByRole("button", { name: /Environment/ }));
    expect(screen.getByText("#1")).toBeInTheDocument();
    expect(screen.getByText("#2")).toBeInTheDocument();

    await user.click(saveButton);
    expect(await screen.findByText("Saved ballot placeholder")).toBeInTheDocument();

    const putCall = fetchMock.mock.calls.find(([, init]) => init?.method === "PUT");
    expect(putCall).toBeDefined();
    expect(JSON.parse(String(putCall?.[1]?.body))).toEqual({
      preferences: [
        { research_area_id: "a-housing", rank: 1 },
        { research_area_id: "a-env", rank: 2 },
      ],
    });
  });

  it("skip remembers the choice and goes to the ballot without saving", async () => {
    const user = userEvent.setup();
    const fetchMock = stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/research-areas": { body: CATALOG },
    });
    renderWelcome();

    await user.click(await screen.findByRole("button", { name: "Skip for now" }));
    expect(await screen.findByText("Saved ballot placeholder")).toBeInTheDocument();
    expect(hasSkippedWelcome(ME_VERIFIED.user.email)).toBe(true);
    await waitFor(() => {
      expect(fetchMock.mock.calls.some(([, init]) => init?.method === "PUT")).toBe(false);
    });
  });
});
