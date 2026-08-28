import { afterEach, describe, expect, it, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MembershipSection } from "./MembershipSection";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";

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
  total_net_cents: 1500,
  payments: [
    { amount_cents: 500, refunded_amount_cents: 0, kind: "monthly", currency: "usd", paid_at: "2026-08-15T12:00:00.000Z" },
    { amount_cents: 2000, refunded_amount_cents: 1000, kind: "one_time", currency: "usd", paid_at: "2026-07-01T12:00:00.000Z" },
  ],
};

function renderSection(search = "") {
  return renderRoutes(
    [
      { path: "/me/settings", element: <MembershipSection /> },
      { path: "/terms", element: <p /> },
    ],
    { pathname: "/me/settings", search }
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.mocked(navigateExternal).mockReset();
});

describe("MembershipSection", () => {
  it("renders nothing when Stripe is not configured", async () => {
    const fetchMock = stubApiRoutes({ "/api/me/membership": { body: { enabled: false } } });
    const { container } = renderSection();

    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    await waitFor(() => expect(container).toBeEmptyDOMElement());
    expect(screen.queryByRole("heading", { name: "Support Elections Simplified" })).not.toBeInTheDocument();
  });

  it("shows the pitch and both payment forms to a non-member", async () => {
    stubApiRoutes({ "/api/me/membership": { body: NOT_MEMBER } });
    renderSection();

    expect(await screen.findByRole("heading", { name: "Support Elections Simplified" })).toBeInTheDocument();
    expect(screen.getByText(/not any candidate, campaign, committee, party, or charity/)).toBeInTheDocument();
    expect(screen.getByLabelText(/Become a supporting member/)).toHaveValue(10);
    expect(screen.getByLabelText(/Make a one-time contribution/)).toHaveValue(10);
    expect(screen.getByRole("button", { name: "Support monthly" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Support once" })).toBeEnabled();
    expect(screen.getByRole("link", { name: "Terms of Use" })).toHaveAttribute("href", "/terms");
    expect(screen.queryByText("Payment history")).not.toBeInTheDocument();
    expect(screen.queryByText(/Total support to date/)).not.toBeInTheDocument();
  });

  it("rejects amounts under the $5 minimum with a visible message", async () => {
    const user = userEvent.setup();
    const fetchMock = stubApiRoutes({ "/api/me/membership": { body: NOT_MEMBER } });
    renderSection();

    const input = await screen.findByLabelText(/Become a supporting member/);
    await user.clear(input);
    await user.type(input, "4");

    expect(screen.getByText("The minimum is $5.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Support monthly" })).toBeDisabled();
    // No checkout call reaches the server for an invalid amount.
    expect(fetchMock.mock.calls.map(([url]) => new URL(String(url), "http://localhost").pathname)).not.toContain(
      "/api/me/membership/checkout"
    );
  });

  it("rejects amounts over the $1,000 cap with a visible message", async () => {
    const user = userEvent.setup();
    stubApiRoutes({ "/api/me/membership": { body: NOT_MEMBER } });
    renderSection();

    const input = await screen.findByLabelText(/Make a one-time contribution/);
    await user.clear(input);
    await user.type(input, "1001");

    expect(screen.getByText("The maximum is $1,000 per payment.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Support once" })).toBeDisabled();

    // The boundary itself is accepted.
    await user.clear(input);
    await user.type(input, "1000");
    expect(screen.getByRole("button", { name: "Support once" })).toBeEnabled();
  });

  it("posts the amount in cents and redirects to the Checkout URL", async () => {
    const user = userEvent.setup();
    let checkoutBody: unknown = null;
    stubApiRoutes({
      "/api/me/membership": { body: NOT_MEMBER },
      "/api/me/membership/checkout": (_url, init) => {
        checkoutBody = JSON.parse(String(init?.body));
        return { body: { url: "https://checkout.stripe.com/c/pay/cs_test_123" } };
      },
    });
    renderSection();

    const input = await screen.findByLabelText(/Become a supporting member/);
    await user.clear(input);
    await user.type(input, "12");
    await user.click(screen.getByRole("button", { name: "Support monthly" }));

    await waitFor(() =>
      expect(navigateExternal).toHaveBeenCalledWith("https://checkout.stripe.com/c/pay/cs_test_123")
    );
    expect(checkoutBody).toEqual({ kind: "monthly", amount_cents: 1200 });
    // Both buttons stay locked until the browser actually leaves the page.
    expect(screen.getByRole("button", { name: "Support monthly" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Support once" })).toBeDisabled();
  });

  it("surfaces a checkout rejection (e.g. already a member) without redirecting", async () => {
    const user = userEvent.setup();
    stubApiRoutes({
      "/api/me/membership": { body: NOT_MEMBER },
      "/api/me/membership/checkout": apiError(409, "membership_exists", "You already have an active membership"),
    });
    renderSection();

    await user.click(await screen.findByRole("button", { name: "Support monthly" }));

    expect(await screen.findByText("You already have an active membership")).toBeInTheDocument();
    expect(navigateExternal).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "Support monthly" })).toBeEnabled();
  });

  it("shows an active member their plan, net total, and history, and opens the portal", async () => {
    const user = userEvent.setup();
    stubApiRoutes({
      "/api/me/membership": { body: ACTIVE_MEMBER },
      "/api/me/membership/portal": { body: { url: "https://billing.stripe.com/p/session/test_abc" } },
    });
    renderSection();

    expect(await screen.findByText(/Monthly supporter: \$5\.00\/month since August 15, 2026/)).toBeInTheDocument();
    expect(screen.getByText(/Total support to date/)).toHaveTextContent("$15.00");
    expect(screen.getByText("Payment history")).toBeInTheDocument();
    expect(screen.getByText(/August 15, 2026 · Monthly/)).toBeInTheDocument();
    expect(screen.getByText(/July 1, 2026 · One-time/)).toBeInTheDocument();
    expect(screen.getByText(/\$10\.00 refunded/)).toBeInTheDocument();
    // Members don't see the sign-up forms; the amount changes via cancel + resubscribe.
    expect(screen.queryByRole("button", { name: "Support monthly" })).not.toBeInTheDocument();
    expect(screen.queryByText(/ends /)).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Manage membership" }));
    await waitFor(() => expect(navigateExternal).toHaveBeenCalledWith("https://billing.stripe.com/p/session/test_abc"));
  });

  it("tells a canceling member when the membership ends", async () => {
    stubApiRoutes({
      "/api/me/membership": {
        body: { ...ACTIVE_MEMBER, membership: { ...ACTIVE_MEMBER.membership, cancel_at_period_end: true } },
      },
    });
    renderSection();

    expect(await screen.findByText("Your membership ends September 15, 2026.")).toBeInTheDocument();
  });

  it("nudges a past-due member to fix their card", async () => {
    stubApiRoutes({
      "/api/me/membership": {
        body: { ...ACTIVE_MEMBER, membership: { ...ACTIVE_MEMBER.membership, stripe_status: "past_due" } },
      },
    });
    renderSection();

    expect(await screen.findByText(/Your last payment didn't go through/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Manage membership" })).toBeEnabled();
  });

  it("thanks the user returning from Checkout and strips the query param", async () => {
    stubApiRoutes({ "/api/me/membership": { body: NOT_MEMBER } });
    const { router } = renderSection("?membership=success");

    expect(await screen.findByText(/Thank you for your support!/)).toBeInTheDocument();
    await waitFor(() => expect(router.state.location.search).toBe(""));
    // The banner survives the param removal (read once into state).
    expect(screen.getByText(/Thank you for your support!/)).toBeInTheDocument();
  });

  it("keeps the payment forms locked after a successful Checkout until the webhook has landed", async () => {
    // The status endpoint can still say "not a member" for a moment after
    // the redirect back; a second click here would be a second charge.
    stubApiRoutes({ "/api/me/membership": { body: NOT_MEMBER } });
    renderSection("?membership=success");

    expect(await screen.findByText(/Thank you for your support!/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Support monthly" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Support once" })).toBeDisabled();
  });

  it("shows an incomplete first payment as pending, not as a supporter", async () => {
    stubApiRoutes({
      "/api/me/membership": {
        body: {
          ...ACTIVE_MEMBER,
          total_net_cents: 0,
          payments: [],
          membership: { ...ACTIVE_MEMBER.membership, stripe_status: "incomplete" },
        },
      },
    });
    renderSection();

    expect(await screen.findByText("Monthly membership pending: $5.00/month")).toBeInTheDocument();
    expect(screen.getByText(/Your first payment is still being confirmed/)).toBeInTheDocument();
    expect(screen.queryByText(/Monthly supporter/)).not.toBeInTheDocument();
    // The backend 409s a second monthly checkout while this row exists, so
    // the forms stay hidden; the portal is where the open invoice gets paid.
    expect(screen.queryByRole("button", { name: "Support monthly" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Manage membership" })).toBeEnabled();
  });

  it("notes a canceled Checkout without alarm", async () => {
    stubApiRoutes({ "/api/me/membership": { body: NOT_MEMBER } });
    renderSection("?membership=canceled");

    expect(await screen.findByText("Checkout was canceled. Nothing was charged.")).toBeInTheDocument();
  });
});
