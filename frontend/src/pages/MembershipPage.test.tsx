import { afterEach, describe, expect, it, vi } from "vitest";
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MembershipPage, projectedAmountStart } from "./MembershipPage";
import { renderRoutes } from "../test/render";
import { apiError, stubApiRoutes } from "../test/mockApi";
import { ME_UNVERIFIED, ME_VERIFIED } from "../test/fixtures";
import type { MembershipMembership } from "@voteapp/api-client";

vi.mock("../lib/externalNavigation", () => ({ navigateExternal: vi.fn() }));
import { navigateExternal } from "../lib/externalNavigation";

const DAY_MS = 86_400_000;
// 20 days out: inside the 7–30 day notice window whenever the test runs, so
// the "starts on" helper names this renewal.
const PERIOD_END = new Date(Date.now() + 20 * DAY_MS).toISOString();
const STARTED_AT = new Date(Date.now() - 10 * DAY_MS).toISOString();

function fmt(iso: string): string {
  return new Date(iso).toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric" });
}
const END_TEXT = fmt(PERIOD_END);

const MEMBERSHIP: MembershipMembership = {
  stripe_status: "active",
  monthly_amount_cents: 1000,
  cancel_at_period_end: false,
  current_period_end: PERIOD_END,
  started_at: STARTED_AT,
  pending_amount_change: null,
};
const PAYMENTS = [
  { amount_cents: 1000, refunded_amount_cents: 0, kind: "monthly", currency: "usd", paid_at: STARTED_AT },
  { amount_cents: 2000, refunded_amount_cents: 500, kind: "one_time", currency: "usd", paid_at: "2026-07-01T12:00:00.000Z" },
];
const ACTIVE = { enabled: true, membership: MEMBERSHIP, total_net_cents: 2500, payments: PAYMENTS };
const NOT_MEMBER = { enabled: true, membership: null, total_net_cents: 0, payments: [] };

function withMembership(patch: Partial<MembershipMembership>, rest: Partial<typeof ACTIVE> = {}) {
  return { ...ACTIVE, ...rest, membership: { ...MEMBERSHIP, ...patch } };
}

function renderPage() {
  return renderRoutes(
    [
      { path: "/me/membership", element: <MembershipPage /> },
      { path: "/login", element: <p /> },
      { path: "/me/settings", element: <p /> },
      { path: "/support/member", element: <p /> },
      { path: "/support/once", element: <p /> },
    ],
    "/me/membership"
  );
}

function renderMember(status: unknown, routes: Parameters<typeof stubApiRoutes>[0] = {}) {
  return stubApiRoutes({ "/api/me": { body: ME_VERIFIED }, "/api/me/membership": { body: status }, ...routes });
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.mocked(navigateExternal).mockReset();
});

describe("projectedAmountStart", () => {
  const base = { stripe_status: "active", monthly_amount_cents: 1000, cancel_at_period_end: false, pending_amount_change: null } as const;

  it("names this period's end while the 7-day notice still fits", () => {
    const membership = { ...base, current_period_end: "2026-10-04T21:46:36.000Z", started_at: "2026-09-04T21:46:36.000Z" };
    expect(projectedAmountStart(membership, new Date("2026-09-27T21:46:36.000Z"))?.toISOString()).toBe(
      "2026-10-04T21:46:36.000Z"
    );
  });

  it("skips to the renewal after when under 7 days remain, on the anchor day clamped to the month", () => {
    // A Jan 31 member: the Feb 28 renewal is too close, so Mar 31 — not Mar 28.
    const membership = { ...base, current_period_end: "2027-02-28T10:00:00.000Z", started_at: "2027-01-31T10:00:00.000Z" };
    expect(projectedAmountStart(membership, new Date("2027-02-25T10:00:00.000Z"))?.toISOString()).toBe(
      "2027-03-31T10:00:00.000Z"
    );
  });

  it("is null without a known period end", () => {
    expect(projectedAmountStart({ ...base, current_period_end: null, started_at: STARTED_AT })).toBeNull();
  });
});

describe("MembershipPage", () => {
  it("asks logged-out visitors to log in and returns them here", async () => {
    stubApiRoutes({ "/api/me": apiError(401, "unauthorized", "Not logged in") });
    renderPage();

    expect(await screen.findByText("Log in to manage your membership.")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Log in" })).toHaveAttribute("href", "/login?next=%2Fme%2Fmembership");
  });

  it("asks unverified accounts to verify", async () => {
    stubApiRoutes({ "/api/me": { body: ME_UNVERIFIED } });
    renderPage();

    expect(await screen.findByRole("heading", { name: "Verify your email" })).toBeInTheDocument();
  });

  it("shows the plan, the thank-you, the controls, and the closed payment history", async () => {
    renderMember(ACTIVE);
    renderPage();

    expect(await screen.findByText(/Because of supporters like you/)).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Your membership" })).toBeInTheDocument();
    expect(screen.getByText(`$10.00 per month · renews ${END_TEXT}`)).toBeInTheDocument();
    expect(screen.getByLabelText("New monthly amount")).toHaveValue(10);
    expect(screen.getByRole("button", { name: "Cancel membership…" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Update payment method" })).toBeEnabled();
    expect(screen.queryByRole("button", { name: "Keep membership" })).not.toBeInTheDocument();

    // History is there but folded away; the total lives inside it.
    const details = screen.getByText("Recent payments").closest("details");
    expect(details).not.toHaveAttribute("open");
    expect(within(details as HTMLElement).getByText(/Total support to date/)).toHaveTextContent("$25.00");
    expect(within(details as HTMLElement).getByText(/\$5\.00 refunded/)).toBeInTheDocument();
    expect(screen.getByText(/not any candidate, campaign, committee, party, or charity/)).toBeInTheDocument();
  });

  it("omits the date rather than inventing one", async () => {
    renderMember(withMembership({ current_period_end: null }));
    renderPage();

    expect(await screen.findByText("$10.00 per month")).toBeInTheDocument();
    expect(screen.getByText(/Your new amount starts at a later renewal\. Nothing is charged today\./)).toBeInTheDocument();
  });

  it("shows a pending amount change on the plan line", async () => {
    renderMember(withMembership({ pending_amount_change: { new_amount_cents: 2000, starts_at: PERIOD_END, applied: true } }));
    renderPage();

    expect(await screen.findByText(`$10.00 per month · $20.00 from ${END_TEXT}`)).toBeInTheDocument();
    // Re-saving the current amount now withdraws the change, so it is allowed.
    expect(screen.getByRole("button", { name: "Save new amount" })).toBeEnabled();
  });

  it("keeps Save disabled until the amount actually changes, and validates the minimum", async () => {
    const user = userEvent.setup();
    const fetchMock = renderMember(ACTIVE);
    renderPage();

    const input = await screen.findByLabelText("New monthly amount");
    expect(screen.getByRole("button", { name: "Save new amount" })).toBeDisabled();
    expect(screen.getByText(`Your new amount starts on ${END_TEXT}. Nothing is charged today.`)).toBeInTheDocument();

    await user.clear(input);
    await user.type(input, "4");
    expect(screen.getByText("The minimum is $5.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Save new amount" })).toBeDisabled();
    expect(fetchMock.mock.calls.map(([url]) => new URL(String(url), "http://localhost").pathname)).not.toContain(
      "/api/me/membership/amount"
    );
  });

  it("posts the new amount in cents and confirms when it starts", async () => {
    const user = userEvent.setup();
    let body: unknown = null;
    renderMember(ACTIVE, {
      "/api/me/membership/amount": (_url, init) => {
        body = JSON.parse(String(init?.body));
        return { body: withMembership({ pending_amount_change: { new_amount_cents: 2000, starts_at: PERIOD_END, applied: true } }) };
      },
    });
    renderPage();

    const input = await screen.findByLabelText("New monthly amount");
    await user.clear(input);
    await user.type(input, "20");
    await user.click(screen.getByRole("button", { name: "Save new amount" }));

    expect(await screen.findByText(`Saved. $20.00 per month starts ${END_TEXT}.`)).toBeInTheDocument();
    expect(body).toEqual({ amount_cents: 2000 });
    expect(screen.getByText(`$10.00 per month · $20.00 from ${END_TEXT}`)).toBeInTheDocument();
    expect(navigateExternal).not.toHaveBeenCalled();
  });

  it("surfaces a refused amount change without changing the plan line", async () => {
    const user = userEvent.setup();
    renderMember(ACTIVE, {
      "/api/me/membership/amount": apiError(409, "membership_pending", "Your new amount of $20.00 is already set for your next renewal."),
    });
    renderPage();

    const input = await screen.findByLabelText("New monthly amount");
    await user.clear(input);
    await user.type(input, "20");
    await user.click(screen.getByRole("button", { name: "Save new amount" }));

    expect(await screen.findByText(/already set for your next renewal/)).toBeInTheDocument();
    expect(screen.getByText(`$10.00 per month · renews ${END_TEXT}`)).toBeInTheDocument();
  });

  it("cancels only after an inline confirmation, then offers Keep membership", async () => {
    const user = userEvent.setup();
    let cancelCalls = 0;
    renderMember(ACTIVE, {
      "/api/me/membership/cancel": () => {
        cancelCalls += 1;
        return { body: withMembership({ cancel_at_period_end: true }) };
      },
    });
    renderPage();

    await user.click(await screen.findByRole("button", { name: "Cancel membership…" }));
    expect(screen.getByText(`Your membership stays active until ${END_TEXT} and will not renew after that.`)).toBeInTheDocument();

    // Backing out posts nothing.
    await user.click(screen.getByRole("button", { name: "Never mind" }));
    expect(screen.queryByRole("button", { name: "Never mind" })).not.toBeInTheDocument();
    expect(cancelCalls).toBe(0);

    await user.click(screen.getByRole("button", { name: "Cancel membership…" }));
    await user.click(screen.getByRole("button", { name: "Cancel membership" }));

    expect(await screen.findByText(`$10.00 per month · will not renew after ${END_TEXT}`)).toBeInTheDocument();
    expect(cancelCalls).toBe(1);
    expect(screen.getByRole("status", { name: "" })).toHaveTextContent(`Your membership will not renew after ${END_TEXT}.`);
    expect(screen.getByRole("button", { name: "Keep membership" })).toBeEnabled();
    // Nothing to change or cancel while it is ending.
    expect(screen.queryByLabelText("New monthly amount")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Cancel membership…" })).not.toBeInTheDocument();
  });

  it("Keep membership posts resume and welcomes the member back", async () => {
    const user = userEvent.setup();
    let resumeCalls = 0;
    renderMember(withMembership({ cancel_at_period_end: true }), {
      "/api/me/membership/resume": () => {
        resumeCalls += 1;
        return { body: ACTIVE };
      },
    });
    renderPage();

    await user.click(await screen.findByRole("button", { name: "Keep membership" }));

    expect(await screen.findByText("Welcome back — your membership continues.")).toBeInTheDocument();
    expect(resumeCalls).toBe(1);
    expect(screen.getByText(`$10.00 per month · renews ${END_TEXT}`)).toBeInTheDocument();
    expect(screen.getByLabelText("New monthly amount")).toBeInTheDocument();
  });

  it("opens the portal's card screen and stays locked until the browser leaves", async () => {
    const user = userEvent.setup();
    let body: unknown = null;
    renderMember(ACTIVE, {
      "/api/me/membership/portal": (_url, init) => {
        body = JSON.parse(String(init?.body));
        return { body: { url: "https://billing.stripe.com/p/session/test_abc" } };
      },
    });
    renderPage();

    await user.click(await screen.findByRole("button", { name: "Update payment method" }));

    await waitFor(() => expect(navigateExternal).toHaveBeenCalledWith("https://billing.stripe.com/p/session/test_abc"));
    expect(body).toEqual({ flow: "payment_method_update" });
    expect(screen.getByRole("button", { name: "Update payment method" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Cancel membership…" })).toBeDisabled();
  });

  it("places the failed-payment notice with Update payment method and keeps the amount form for past_due", async () => {
    renderMember(withMembership({ stripe_status: "past_due" }));
    renderPage();

    const notice = await screen.findByText(/Your last payment didn't go through/);
    expect(within(notice.parentElement as HTMLElement).getByRole("button", { name: "Update payment method" })).toBeEnabled();
    expect(screen.getByLabelText("New monthly amount")).toBeInTheDocument();
    expect(screen.queryByText(/Because of supporters like you/)).not.toBeInTheDocument();
  });

  it("hides the amount form and cancel for an incomplete first payment", async () => {
    renderMember(withMembership({ stripe_status: "incomplete", current_period_end: null }, { payments: [], total_net_cents: 0 }));
    renderPage();

    expect(await screen.findByText(/Your first payment is still being confirmed/)).toBeInTheDocument();
    expect(screen.queryByLabelText("New monthly amount")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Cancel membership…" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Update payment method" })).toBeEnabled();
    expect(screen.queryByText("Recent payments")).not.toBeInTheDocument();
  });

  it("hides the amount form for an unpaid subscription", async () => {
    renderMember(withMembership({ stripe_status: "unpaid" }));
    renderPage();

    expect(await screen.findByText(/Your last payment didn't go through/)).toBeInTheDocument();
    expect(screen.queryByLabelText("New monthly amount")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Cancel membership…" })).toBeEnabled();
  });

  it("shows a non-member the support links, with history when there is any", async () => {
    renderMember({ ...NOT_MEMBER, total_net_cents: 1500, payments: [PAYMENTS[1]] });
    renderPage();

    expect(await screen.findByText(/You don't have a monthly membership right now/)).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Become an honorary member" })).toHaveAttribute("href", "/support/member");
    expect(screen.getByRole("link", { name: "Support once" })).toHaveAttribute("href", "/support/once");
    expect(screen.getByText("Recent payments")).toBeInTheDocument();
    expect(screen.getByText(/Total support to date/)).toHaveTextContent("$15.00");
    expect(screen.queryByRole("button", { name: "Update payment method" })).not.toBeInTheDocument();
  });

  it("shows a non-member without payments only the support links", async () => {
    renderMember(NOT_MEMBER);
    renderPage();

    expect(await screen.findByRole("link", { name: "Become an honorary member" })).toBeInTheDocument();
    expect(screen.queryByText("Recent payments")).not.toBeInTheDocument();
  });

  it("says payments are unavailable when Stripe is not configured", async () => {
    renderMember({ enabled: false });
    renderPage();

    expect(await screen.findByText(/Payments are temporarily unavailable/)).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Become an honorary member" })).not.toBeInTheDocument();
  });

  it("drops a delayed answer once the member has left the page, so it never lands in the next account's cache", async () => {
    const user = userEvent.setup();
    let releaseCancel: (() => void) | null = null;
    renderMember(ACTIVE, {
      "/api/me/membership/cancel": () =>
        new Promise((resolve) => {
          releaseCancel = () => resolve({ body: withMembership({ cancel_at_period_end: true }) });
        }),
    });
    const { queryClient } = renderPage();

    await user.click(await screen.findByRole("button", { name: "Cancel membership…" }));
    await user.click(screen.getByRole("button", { name: "Cancel membership" }));

    // Account A signs out while the cancel is in flight; account B's status
    // then occupies the shared cache.
    queryClient.setQueryData(["me"], null);
    expect(await screen.findByText("Log in to manage your membership.")).toBeInTheDocument();
    queryClient.setQueryData(["me", "membership"], NOT_MEMBER);

    (releaseCancel as (() => void) | null)?.();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(queryClient.getQueryData(["me", "membership"])).toEqual(NOT_MEMBER);
  });

  it("does not let an in-flight status GET overwrite a mutation's result", async () => {
    const user = userEvent.setup();
    let releaseStale: (() => void) | null = null;
    let statusCalls = 0;
    renderMember(ACTIVE, {
      "/api/me/membership": () => {
        statusCalls += 1;
        if (statusCalls === 1) {
          return { body: ACTIVE };
        }
        // The second GET (a refetch) hangs until the test releases it —
        // with the pre-change snapshot.
        return new Promise((resolve) => {
          releaseStale = () => resolve({ body: ACTIVE });
        });
      },
      "/api/me/membership/cancel": { body: withMembership({ cancel_at_period_end: true }) },
    });
    const { queryClient } = renderPage();

    await screen.findByRole("button", { name: "Cancel membership…" });
    void queryClient.refetchQueries({ queryKey: ["me", "membership"] });
    await waitFor(() => expect(statusCalls).toBe(2));

    await user.click(screen.getByRole("button", { name: "Cancel membership…" }));
    await user.click(screen.getByRole("button", { name: "Cancel membership" }));
    expect(await screen.findByRole("button", { name: "Keep membership" })).toBeInTheDocument();

    // The stale answer lands after the mutation installed its result.
    (releaseStale as (() => void) | null)?.();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(screen.getByRole("button", { name: "Keep membership" })).toBeInTheDocument();
    expect(screen.getByText(`$10.00 per month · will not renew after ${END_TEXT}`)).toBeInTheDocument();
  });
});
