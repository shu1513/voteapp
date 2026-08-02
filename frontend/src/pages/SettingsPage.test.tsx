import { afterEach, describe, expect, it, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { SettingsPage } from "./SettingsPage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { ballotSummary, ME_UNVERIFIED, ME_VERIFIED } from "../test/fixtures";

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

  it("keeps the issue editor off settings — it lives on My Picks", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/email-preferences": { body: EMAIL_PREFERENCES },
    });
    renderSettings();

    expect(await screen.findByRole("heading", { name: "Email notifications" })).toBeInTheDocument();
    expect(screen.queryByText("Issues you care about")).not.toBeInTheDocument();
  });

  it("confirms a saved first name, then clears the confirmation", async () => {
    const user = userEvent.setup();
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/email-preferences": { body: EMAIL_PREFERENCES },
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
      "/api/research-areas": { body: { research_areas: [] } },
      "/api/me/research-area-preferences": { body: { preferences: [] } },
    });
    renderSettings();

    expect(await screen.findByLabelText("Daily digest about candidates you follow")).toBeChecked();
    expect(screen.getByLabelText(/Remind me the day before each election/)).not.toBeChecked();
    expect(screen.getByLabelText("New elections in your districts")).toBeChecked();
    expect(screen.getByLabelText(/Updates about the issues you saved/)).toBeChecked();
  });
});
