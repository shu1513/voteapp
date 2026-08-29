import { describe, expect, it } from "vitest";
import { screen } from "@testing-library/react";
import SupportMemberPage from "./SupportMemberPage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { ME_UNVERIFIED, ME_VERIFIED } from "../test/fixtures";

const NOT_MEMBER = { enabled: true, membership: null, total_net_cents: 0, payments: [] };
const ACTIVE_MEMBER = {
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
};

function renderPage(search = "") {
  return renderRoutes(
    [
      { path: "/support/member", element: <SupportMemberPage /> },
      { path: "/login", element: <p /> },
      { path: "/register", element: <p /> },
      { path: "/terms", element: <p /> },
    ],
    { pathname: "/support/member", search }
  );
}

describe("SupportMemberPage", () => {
  it("shows the pitch and the login/signup path to logged-out readers", async () => {
    stubApiRoutes({ "/api/me": apiError(401, "unauthorized", "Not logged in") });
    renderPage();

    expect(
      await screen.findByRole("heading", { name: "Become an honorary member" })
    ).toBeInTheDocument();
    expect(await screen.findByRole("link", { name: "Log in" })).toHaveAttribute(
      "href",
      "/login?next=%2Fsupport%2Fmember"
    );
    expect(screen.getByRole("link", { name: "sign up" })).toHaveAttribute(
      "href",
      "/register?next=%2Fsupport%2Fmember"
    );
    expect(screen.queryByRole("button", { name: "Become a member" })).not.toBeInTheDocument();
  });

  it("carries a Stripe return outcome through the login round-trip", async () => {
    stubApiRoutes({ "/api/me": apiError(401, "unauthorized", "Not logged in") });
    renderPage("?membership=success");

    // The success param must survive auth, or the returning supporter loses
    // the banner and the double-charge lock.
    expect(await screen.findByRole("link", { name: "Log in" })).toHaveAttribute(
      "href",
      "/login?next=%2Fsupport%2Fmember%3Fmembership%3Dsuccess"
    );
  });

  it("asks unverified accounts to verify instead of showing the form", async () => {
    stubApiRoutes({ "/api/me": { body: ME_UNVERIFIED } });
    renderPage();

    expect(await screen.findByRole("heading", { name: "Verify your email" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Become a member" })).not.toBeInTheDocument();
  });

  it("shows only the monthly form, with a $10 default, to a verified non-member", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/membership": { body: NOT_MEMBER },
    });
    renderPage();

    expect(await screen.findByLabelText(/Monthly amount/)).toHaveValue(10);
    expect(screen.getByRole("button", { name: "Become a member" })).toBeEnabled();
    // The one-time form does not compete here — the visitor already chose.
    expect(screen.queryByLabelText(/One-time support/)).not.toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Terms of Use" })).toHaveAttribute("href", "/terms");
  });

  it("shows an existing member their plan instead of the form", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/membership": { body: ACTIVE_MEMBER },
    });
    renderPage();

    expect(
      await screen.findByText(/Monthly supporter: \$5\.00\/month since August 15, 2026/)
    ).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Manage membership" })).toBeEnabled();
    expect(screen.queryByRole("button", { name: "Become a member" })).not.toBeInTheDocument();
  });

  it("thanks the supporter returning from Checkout and locks the form", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/membership": { body: NOT_MEMBER },
    });
    renderPage("?membership=success");

    expect(await screen.findByText(/Thank you for your support!/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Become a member" })).toBeDisabled();
  });

  it("shows an unavailable notice instead of a dead form when Stripe is not configured", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/membership": { body: { enabled: false } },
    });
    renderPage();

    expect(await screen.findByText(/Payments are temporarily unavailable/)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Become a member" })).not.toBeInTheDocument();
  });
});
