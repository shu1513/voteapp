import { describe, expect, it } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import MissionPage from "./MissionPage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { ME_UNVERIFIED, ME_VERIFIED } from "../test/fixtures";

const NOT_MEMBER = { enabled: true, membership: null, total_net_cents: 0, payments: [] };

function renderMission() {
  return renderRoutes(
    [
      { path: "/mission", element: <MissionPage /> },
      { path: "/login", element: <p /> },
      { path: "/register", element: <p /> },
      { path: "/terms", element: <p /> },
    ],
    "/mission"
  );
}

describe("MissionPage", () => {
  it("shows the pitch and the login/signup path to logged-out readers", async () => {
    stubApiRoutes({ "/api/me": apiError(401, "unauthorized", "Not logged in") });
    renderMission();

    expect(await screen.findByRole("heading", { name: "Our mission" })).toBeInTheDocument();
    // ?next returns the prospective supporter here after auth.
    expect(await screen.findByRole("link", { name: "Log in" })).toHaveAttribute(
      "href",
      "/login?next=%2Fmission"
    );
    expect(screen.getByRole("link", { name: "sign up" })).toHaveAttribute(
      "href",
      "/register?next=%2Fmission"
    );
    // No payment forms and no error box — the membership query must not fire
    // without a verified session.
    expect(screen.queryByRole("button", { name: "Support monthly" })).not.toBeInTheDocument();
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("asks unverified accounts to verify instead of showing the forms", async () => {
    stubApiRoutes({ "/api/me": { body: ME_UNVERIFIED } });
    renderMission();

    expect(await screen.findByText(/Verify your email address to support/)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Support monthly" })).not.toBeInTheDocument();
  });

  it("shows the payment forms to a verified user when Stripe is configured", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/membership": { body: NOT_MEMBER },
    });
    renderMission();

    expect(await screen.findByRole("button", { name: "Support monthly" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Support once" })).toBeEnabled();
  });

  it("keeps the pitch but no dead forms when Stripe is not configured", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/membership": { body: { enabled: false } },
    });
    const { queryClient } = renderMission();

    expect(await screen.findByRole("heading", { name: "Our mission" })).toBeInTheDocument();
    await waitFor(() => expect(queryClient.getQueryState(["me", "membership"])?.status).toBe("success"));
    expect(screen.queryByRole("button", { name: "Support monthly" })).not.toBeInTheDocument();
  });
});
