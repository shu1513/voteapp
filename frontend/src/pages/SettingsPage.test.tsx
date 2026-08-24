import { afterEach, describe, expect, it, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { SettingsPage } from "./SettingsPage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { ballotSummary, ME_GOOGLE_NO_PASSWORD, ME_UNVERIFIED, ME_VERIFIED } from "../test/fixtures";

const EMAIL_PREFERENCES = {
  email_digest: true,
  email_election_reminders: false,
  email_new_election_alerts: true,
  email_issue_updates: true,
};

function renderSettings() {
  return renderRoutes(
    [
      { path: "/me/settings", element: <SettingsPage /> },
      { path: "/login", element: <p /> },
      { path: "/me/ballot", element: <p>Saved ballot placeholder</p> },
    ],
    "/me/settings"
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("SettingsPage", () => {
  it("asks logged-out visitors to log in", async () => {
    stubApiRoutes({ "/api/me": apiError(401, "unauthorized", "Not logged in") });
    renderSettings();
    expect(await screen.findByText("Log in to manage your account.")).toBeInTheDocument();
  });

  it("hides notification sections until the email is verified", async () => {
    stubApiRoutes({ "/api/me": { body: ME_UNVERIFIED } });
    renderSettings();

    expect(
      await screen.findByText("Verify your email to manage your address and notifications.")
    ).toBeInTheDocument();
    expect(screen.queryByText("Email notifications")).not.toBeInTheDocument();
    // Account basics still work unverified (fixing a typo must not need a
    // verified inbox).
    expect(screen.getByRole("heading", { name: "Settings" })).toBeInTheDocument();
  });

  it("renders the issue editor for verified users — back from its stint on My Picks", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/email-preferences": { body: EMAIL_PREFERENCES },
      "/api/me/membership": { body: { enabled: false } },
      "/api/research-areas": { body: { research_areas: [] } },
      "/api/me/research-area-preferences": { body: { preferences: [] } },
    });
    renderSettings();

    expect(await screen.findByRole("heading", { name: "Email notifications" })).toBeInTheDocument();
    expect(screen.getByText("My most important issues")).toBeInTheDocument();
  });

  it("hides the support section when Stripe is not configured", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/email-preferences": { body: EMAIL_PREFERENCES },
      "/api/me/membership": { body: { enabled: false } },
      "/api/research-areas": { body: { research_areas: [] } },
      "/api/me/research-area-preferences": { body: { preferences: [] } },
    });
    const { queryClient } = renderSettings();

    expect(await screen.findByRole("heading", { name: "Email notifications" })).toBeInTheDocument();
    // The section also renders nothing while its request is pending, so the
    // absence only means "hidden because disabled" once that query settled.
    await waitFor(() => expect(queryClient.getQueryState(["me", "membership"])?.status).toBe("success"));
    expect(screen.queryByRole("heading", { name: "Support Elections Simplified" })).not.toBeInTheDocument();
  });

  it("shows the support section to verified users when Stripe is configured", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/email-preferences": { body: EMAIL_PREFERENCES },
      "/api/me/membership": { body: { enabled: true, membership: null, total_net_cents: 0, payments: [] } },
      "/api/research-areas": { body: { research_areas: [] } },
      "/api/me/research-area-preferences": { body: { preferences: [] } },
    });
    renderSettings();

    expect(await screen.findByRole("heading", { name: "Support Elections Simplified" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Support monthly" })).toBeInTheDocument();
    // Non-member: the Profile box invites, pointing at the mission page.
    expect(screen.getByRole("link", { name: "Become a member" })).toHaveAttribute("href", "/mission");
    expect(screen.queryByText(/Thank you for being a supporting member/)).not.toBeInTheDocument();
  });

  it("thanks a member in the Profile box and anchors to the support section", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/email-preferences": { body: EMAIL_PREFERENCES },
      "/api/me/membership": {
        body: {
          enabled: true,
          membership: {
            stripe_status: "active",
            monthly_amount_cents: 500,
            cancel_at_period_end: false,
            current_period_end: "2026-09-15T12:00:00.000Z",
            started_at: "2026-08-15T12:00:00.000Z",
          },
          total_net_cents: 500,
          payments: [],
        },
      },
      "/api/research-areas": { body: { research_areas: [] } },
      "/api/me/research-area-preferences": { body: { preferences: [] } },
    });
    renderSettings();

    expect(await screen.findByText(/Thank you for being a supporting member/)).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Manage membership" })).toHaveAttribute("href", "#support");
    expect(screen.queryByRole("link", { name: "Become a member" })).not.toBeInTheDocument();
  });

  it("swaps password-gated sections for the add-a-password hint on Google-only accounts", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_GOOGLE_NO_PASSWORD },
      "/api/me/email-preferences": { body: EMAIL_PREFERENCES },
      "/api/me/membership": { body: { enabled: false } },
      "/api/research-areas": { body: { research_areas: [] } },
      "/api/me/research-area-preferences": { body: { preferences: [] } },
    });
    renderSettings();

    expect(await screen.findByRole("heading", { name: "Add a password" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Add a password" })).toHaveAttribute("href", "/forgot-password");
    // The three password-gated forms are replaced, not left to fail.
    expect(screen.queryByRole("heading", { name: "Change password" })).not.toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "Change email" })).not.toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "Delete account" })).not.toBeInTheDocument();
    // Sessions need no password and stay.
    expect(screen.getByRole("heading", { name: "Sessions" })).toBeInTheDocument();
  });

  it("keeps the password-gated sections for accounts with a password", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/email-preferences": { body: EMAIL_PREFERENCES },
      "/api/me/membership": { body: { enabled: false } },
      "/api/research-areas": { body: { research_areas: [] } },
      "/api/me/research-area-preferences": { body: { preferences: [] } },
    });
    renderSettings();

    expect(await screen.findByRole("heading", { name: "Change password" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Change email" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Delete account" })).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "Add a password" })).not.toBeInTheDocument();
  });

  it("confirms a saved first name, then clears the confirmation", async () => {
    const user = userEvent.setup();
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/email-preferences": { body: EMAIL_PREFERENCES },
      "/api/me/membership": { body: { enabled: false } },
    });
    renderSettings();

    const input = await screen.findByLabelText("First Name");
    await user.clear(input);
    await user.type(input, "Alex");
    await user.click(screen.getByRole("button", { name: "Save" }));

    // Confirmation is a live region so screen readers announce the save.
    const status = await screen.findByRole("status");
    expect(status).toHaveTextContent("Saved");

    // ...and it clears itself, so a stale "Saved" never sits beside a
    // later, unsaved edit.
    await waitFor(() => expect(status).toHaveTextContent(""), { timeout: 4000 });
  });

  it("saves a new home address and redirects to the saved ballot", async () => {
    const user = userEvent.setup();
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/email-preferences": { body: EMAIL_PREFERENCES },
      "/api/me/membership": { body: { enabled: false } },
      "/api/research-areas": { body: { research_areas: [] } },
      "/api/me/research-area-preferences": { body: { preferences: [] } },
      "/api/address/autocomplete": { body: { suggestions: [] } },
      "/api/me/address": {
        body: { ...ballotSummary([]), matched_address: "123 MAIN ST, AUSTIN, TX", address_match_count: 1 },
      },
    });
    renderSettings();

    await user.type(await screen.findByLabelText("New address"), "123 Main St, Austin, TX");
    await user.click(screen.getByRole("button", { name: "Save address" }));

    // A successful save lands on the election list; the confirmation itself
    // renders there (covered by the SavedBallotPage tests).
    expect(await screen.findByText("Saved ballot placeholder")).toBeInTheDocument();
  });

  it("shows all four email toggles with the saved values for verified users", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/email-preferences": { body: EMAIL_PREFERENCES },
      "/api/me/membership": { body: { enabled: false } },
      "/api/research-areas": { body: { research_areas: [] } },
      "/api/me/research-area-preferences": { body: { preferences: [] } },
    });
    renderSettings();

    expect(await screen.findByLabelText(/Updates about my candidates and election results/)).toBeChecked();
    expect(screen.getByLabelText(/Election reminder the day before election day/)).not.toBeChecked();
    expect(screen.getByLabelText("Notify me about new elections coming up in my districts")).toBeChecked();
    expect(screen.getByLabelText(/Updates about the issues you saved/)).toBeChecked();
  });
});
