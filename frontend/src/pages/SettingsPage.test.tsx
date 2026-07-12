import { afterEach, describe, expect, it, vi } from "vitest";
import { screen } from "@testing-library/react";
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

  it("hides notification and issue sections until the email is verified", async () => {
    stubApiRoutes({ "/api/me": { body: ME_UNVERIFIED } });
    renderSettings();

    expect(
      await screen.findByText("Verify your email to manage your address, notifications and issue preferences.")
    ).toBeInTheDocument();
    expect(screen.queryByText("Email notifications")).not.toBeInTheDocument();
    // Account basics still work unverified (fixing a typo must not need a
    // verified inbox).
    expect(screen.getByRole("heading", { name: "Settings" })).toBeInTheDocument();
  });

  it("saves a new home address and confirms the match", async () => {
    const user = userEvent.setup();
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/email-preferences": { body: EMAIL_PREFERENCES },
      "/api/research-areas": { body: { research_areas: [] } },
      "/api/me/research-area-preferences": { body: { preferences: [] } },
      "/api/address/autocomplete": { body: { suggestions: [] } },
      "/api/me/address": { body: { ...ballotSummary([]), matched_address: "123 MAIN ST, AUSTIN, TX" } },
    });
    renderSettings();

    await user.type(await screen.findByLabelText("New address"), "123 Main St, Austin, TX");
    await user.click(screen.getByRole("button", { name: "Save address" }));

    const confirmation = await screen.findByRole("status");
    expect(confirmation).toHaveTextContent("matched to 123 MAIN ST, AUSTIN, TX");
    expect(confirmation).toHaveTextContent("1 district");
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
