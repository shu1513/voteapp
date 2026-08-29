import { describe, expect, it, vi, afterEach } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import SupportOncePage from "./SupportOncePage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { ME_UNVERIFIED, ME_VERIFIED } from "../test/fixtures";

vi.mock("../lib/externalNavigation", () => ({ navigateExternal: vi.fn() }));
import { navigateExternal } from "../lib/externalNavigation";

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
      { path: "/support/once", element: <SupportOncePage /> },
      { path: "/login", element: <p /> },
      { path: "/register", element: <p /> },
      { path: "/terms", element: <p /> },
    ],
    { pathname: "/support/once", search }
  );
}

afterEach(() => {
  vi.mocked(navigateExternal).mockReset();
});

describe("SupportOncePage", () => {
  it("shows the pitch and the login/signup path to logged-out readers", async () => {
    stubApiRoutes({ "/api/me": apiError(401, "unauthorized", "Not logged in") });
    renderPage();

    expect(
      await screen.findByRole("heading", { name: "Make a one-time contribution" })
    ).toBeInTheDocument();
    expect(await screen.findByRole("link", { name: "Log in" })).toHaveAttribute(
      "href",
      "/login?next=%2Fsupport%2Fonce"
    );
    expect(screen.queryByRole("button", { name: "Contribute" })).not.toBeInTheDocument();
  });

  it("carries a Stripe return outcome through the login round-trip", async () => {
    stubApiRoutes({ "/api/me": apiError(401, "unauthorized", "Not logged in") });
    renderPage("?membership=success");

    expect(await screen.findByRole("link", { name: "Log in" })).toHaveAttribute(
      "href",
      "/login?next=%2Fsupport%2Fonce%3Fmembership%3Dsuccess"
    );
  });

  it("asks unverified accounts to verify instead of showing the form", async () => {
    stubApiRoutes({ "/api/me": { body: ME_UNVERIFIED } });
    renderPage();

    expect(await screen.findByRole("heading", { name: "Verify your email" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Contribute" })).not.toBeInTheDocument();
  });

  it("posts a one-time checkout with the $10 default and redirects to Stripe", async () => {
    const user = userEvent.setup();
    let checkoutBody: unknown = null;
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/membership": { body: NOT_MEMBER },
      "/api/me/membership/checkout": (_url, init) => {
        checkoutBody = JSON.parse(String(init?.body));
        return { body: { url: "https://checkout.stripe.com/c/pay/cs_test_456" } };
      },
    });
    renderPage();

    expect(await screen.findByLabelText(/Contribution amount/)).toHaveValue(10);
    // The monthly form does not compete here — the visitor already chose.
    expect(screen.queryByLabelText(/Monthly amount/)).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Contribute" }));
    await waitFor(() =>
      expect(navigateExternal).toHaveBeenCalledWith("https://checkout.stripe.com/c/pay/cs_test_456")
    );
    expect(checkoutBody).toEqual({ kind: "one_time", amount_cents: 1000 });
  });

  it("still offers the one-time form to an existing member", async () => {
    stubApiRoutes({
      "/api/me": { body: ME_VERIFIED },
      "/api/me/membership": { body: ACTIVE_MEMBER },
    });
    renderPage();

    // A one-time gift on top of a membership is allowed, so no member branch here.
    expect(await screen.findByLabelText(/Contribution amount/)).toHaveValue(10);
    expect(screen.getByRole("button", { name: "Contribute" })).toBeEnabled();
    expect(screen.queryByText(/Monthly supporter/)).not.toBeInTheDocument();
  });
});
