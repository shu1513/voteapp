import { describe, expect, it } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import MissionPage from "./MissionPage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { ME_UNVERIFIED, ME_VERIFIED } from "../test/fixtures";

const NOT_MEMBER = { enabled: true, membership: null, total_net_cents: 0, payments: [] };
const EMAIL_PREFERENCES = {
  email_digest: true,
  email_election_reminders: false,
  email_new_election_alerts: true,
  email_issue_updates: false,
};

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

    expect(await screen.findByRole("heading", { name: "Mission" })).toBeInTheDocument();
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

    // The standard interstitial, with a real resend path.
    expect(await screen.findByRole("heading", { name: "Verify your email" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Resend verification email" })).toBeEnabled();
    expect(screen.queryByRole("button", { name: "Support monthly" })).not.toBeInTheDocument();
  });

  it("shows the payment forms and the two email opt-ins to a verified user", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/membership": { body: NOT_MEMBER },
      "/api/me/email-preferences": { body: EMAIL_PREFERENCES },
    });
    renderMission();

    expect(await screen.findByRole("button", { name: "Support monthly" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Support once" })).toBeEnabled();
    // Way 3: only the two subscription toggles the pitch names, live values.
    expect(
      await screen.findByLabelText(/Updates about my candidates and election results/)
    ).toBeChecked();
    expect(screen.getByLabelText(/Updates about the issues you saved/)).not.toBeChecked();
    expect(
      screen.queryByLabelText(/Election reminder the day before election day/)
    ).not.toBeInTheDocument();
  });

  it("keeps the pitch but no dead forms when Stripe is not configured", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/membership": { body: { enabled: false } },
      "/api/me/email-preferences": { body: EMAIL_PREFERENCES },
    });
    const { queryClient } = renderMission();

    expect(await screen.findByRole("heading", { name: "Mission" })).toBeInTheDocument();
    await waitFor(() => expect(queryClient.getQueryState(["me", "membership"])?.status).toBe("success"));
    expect(screen.queryByRole("button", { name: "Support monthly" })).not.toBeInTheDocument();
  });
});
