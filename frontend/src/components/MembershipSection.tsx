import { useEffect, useState } from "react";
import { Link, useSearchParams } from "react-router";
import { useMutation, useQuery } from "@tanstack/react-query";
import { apiRequest, APP_NAME } from "@voteapp/api-client";
import type { MembershipKind, MembershipPayment, MembershipStatus } from "@voteapp/api-client";
import { ErrorNotice } from "./Status";
import { navigateExternal } from "../lib/externalNavigation";

// "Support Elections Simplified" settings section: voluntary monthly or
// one-time payments through Stripe Checkout (full-page redirect, no Stripe
// SDK here). Hidden entirely when the backend reports Stripe unconfigured.
// Not a nonprofit: copy says the money runs the service and never implies
// a candidate, campaign, or charity — see docs/plans/membership-contributions.md.

// Whole dollars; mirrored from the backend's checkout validation
// (MEMBERSHIP_CHECKOUT_MIN/MAX_AMOUNT_CENTS). The server re-checks both.
const MIN_DOLLARS = 5;
const MAX_DOLLARS = 1000;

const buttonClass =
  "rounded-lg bg-rausch px-4 py-2 text-sm font-semibold text-white transition hover:bg-rausch-dark disabled:cursor-not-allowed disabled:bg-line";
const secondaryButtonClass =
  "rounded-lg border border-line bg-white px-4 py-2 text-sm font-semibold text-ink transition hover:border-rausch disabled:cursor-not-allowed disabled:text-ink-soft";
const inputClass =
  "mt-1 w-full rounded-md border border-line px-3 py-2 shadow-sm focus:border-ink focus:outline-none";

const currency = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" });

function formatCents(cents: number): string {
  return currency.format(cents / 100);
}

// Timestamps (instants), unlike election dates — Date parsing is correct here.
function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric" });
}

/** Validates a whole-dollar input. Returns the cents to charge, or the
 * message to show. Empty input is neither (the button just stays disabled). */
function parseDollars(raw: string): { cents: number } | { message: string } | null {
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }
  const dollars = Number(trimmed);
  if (!Number.isInteger(dollars)) {
    return { message: "Enter a whole-dollar amount." };
  }
  if (dollars < MIN_DOLLARS) {
    return { message: `The minimum is $${MIN_DOLLARS}.` };
  }
  if (dollars > MAX_DOLLARS) {
    return { message: `The maximum is $${MAX_DOLLARS.toLocaleString("en-US")} per payment.` };
  }
  return { cents: dollars * 100 };
}

function AmountForm({
  kind,
  label,
  buttonLabel,
  initialDollars,
  disabled,
  onSubmit,
}: {
  kind: MembershipKind;
  label: string;
  buttonLabel: string;
  initialDollars: string;
  disabled: boolean;
  onSubmit: (input: { kind: MembershipKind; amountCents: number }) => void;
}) {
  const [raw, setRaw] = useState(initialDollars);
  const parsed = parseDollars(raw);
  const message = parsed && "message" in parsed ? parsed.message : null;
  const cents = parsed && "cents" in parsed ? parsed.cents : null;
  const inputId = `membership-${kind}-dollars`;

  return (
    <form
      onSubmit={(event) => {
        event.preventDefault();
        if (cents !== null && !disabled) {
          onSubmit({ kind, amountCents: cents });
        }
      }}
      className="flex items-end gap-3"
    >
      <div className="grow">
        <label htmlFor={inputId} className="block text-sm font-medium text-ink">
          {label}
        </label>
        <div className="relative">
          <span className="pointer-events-none absolute inset-y-0 left-3 top-1 flex items-center text-ink-soft">
            $
          </span>
          <input
            id={inputId}
            type="number"
            inputMode="numeric"
            min={MIN_DOLLARS}
            max={MAX_DOLLARS}
            step={1}
            value={raw}
            onChange={(event) => setRaw(event.target.value)}
            aria-describedby={message ? `${inputId}-message` : undefined}
            aria-invalid={message ? true : undefined}
            className={`${inputClass} pl-7`}
          />
        </div>
        {/* Always mounted so the message appears inside an existing live
            region, the reliably-announced case. */}
        <p id={`${inputId}-message`} role="status" className="mt-1 min-h-4 text-xs text-rausch-dark">
          {message ?? ""}
        </p>
      </div>
      <button type="submit" disabled={cents === null || disabled} className={`${buttonClass} mb-5`}>
        {buttonLabel}
      </button>
    </form>
  );
}

function PaymentHistory({ payments }: { payments: MembershipPayment[] }) {
  if (payments.length === 0) {
    return null;
  }
  return (
    <div className="mt-4">
      <h3 className="text-subheading font-semibold text-ink">Payment history</h3>
      <ul className="mt-1 divide-y divide-line text-sm">
        {payments.map((payment, index) => (
          <li key={`${payment.paid_at}-${index}`} className="flex flex-wrap justify-between gap-x-3 py-1.5">
            <span className="text-ink-soft">
              {formatDate(payment.paid_at)} · {payment.kind === "monthly" ? "Monthly" : "One-time"}
            </span>
            <span>
              {formatCents(payment.amount_cents)}
              {payment.refunded_amount_cents > 0 ? (
                <span className="text-ink-soft"> ({formatCents(payment.refunded_amount_cents)} refunded)</span>
              ) : null}
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}

export function MembershipSection() {
  // Checkout returns here with ?membership=success|canceled. Read once, then
  // strip the param so a reload doesn't re-announce an old outcome.
  const [searchParams, setSearchParams] = useSearchParams();
  const [outcome] = useState(() => searchParams.get("membership"));
  useEffect(() => {
    if (searchParams.has("membership")) {
      setSearchParams(
        (current) => {
          current.delete("membership");
          return current;
        },
        { replace: true }
      );
    }
  }, [searchParams, setSearchParams]);

  // staleTime 0 (the app default is 60s): the webhook that records a payment
  // can land after the return from Checkout, so every mount must ask again
  // rather than reuse a pre-webhook snapshot — no polling, just no caching.
  const status = useQuery({
    queryKey: ["me", "membership"],
    queryFn: () => apiRequest<MembershipStatus>("/api/me/membership"),
    staleTime: 0,
  });
  const checkout = useMutation({
    mutationFn: (input: { kind: MembershipKind; amountCents: number }) =>
      apiRequest<{ url: string }>("/api/me/membership/checkout", {
        method: "POST",
        body: { kind: input.kind, amount_cents: input.amountCents },
      }),
    onSuccess: (response) => {
      navigateExternal(response.url);
    },
  });
  const portal = useMutation({
    mutationFn: () => apiRequest<{ url: string }>("/api/me/membership/portal", { method: "POST", body: {} }),
    onSuccess: (response) => {
      navigateExternal(response.url);
    },
  });

  if (status.isPending || (status.data && !status.data.enabled)) {
    return null;
  }

  // Both redirects stay "in flight" until the browser leaves the page, so
  // the buttons stay locked after success too (double-click guard). The
  // return from a successful Checkout locks them as well: the webhook may
  // not have recorded the payment yet, and a second monthly checkout in
  // that gap is a real second charge the backend can only cancel forward
  // (its first month needs a manual refund).
  const busy =
    outcome === "success" || checkout.isPending || checkout.isSuccess || portal.isPending || portal.isSuccess;

  return (
    // id: anchor target for the Profile box's "Manage membership" link.
    <section id="support" className="rounded-xl border border-line bg-white p-4">
      <h2 className="text-heading font-semibold">Support {APP_NAME}</h2>
      {outcome === "success" ? (
        <p role="status" className="mt-2 rounded-lg border border-green-700/40 bg-green-50 px-3 py-2 text-sm text-green-900">
          Thank you for your support! Your payment may take a moment to appear below.
        </p>
      ) : null}
      {outcome === "canceled" ? (
        <p role="status" className="mt-2 rounded-lg border border-line bg-surface px-3 py-2 text-sm text-ink-soft">
          Checkout was canceled — nothing was charged.
        </p>
      ) : null}
      {status.isError ? (
        <div className="mt-2">
          <ErrorNotice error={status.error} />
        </div>
      ) : null}
      {status.data ? (
        <>
          <p className="mt-1 text-sm text-ink-soft">
            {APP_NAME} is independently operated. Optional payments support operating the service — not any
            candidate, campaign, committee, party, or charity. Payments provide no additional content or influence
            and are not eligible for a charitable-contribution receipt.
          </p>

          {status.data.membership ? (
            // Any nonterminal subscription takes this branch, paid or not:
            // the backend answers 409 to a monthly checkout while one exists,
            // so the signup forms would only fail. `incomplete` (first payment
            // not confirmed yet) gets its own copy instead of "supporter".
            <div className="mt-3 space-y-2 text-sm">
              <p className="font-medium text-ink">
                {status.data.membership.stripe_status === "incomplete"
                  ? `Monthly membership pending — ${formatCents(status.data.membership.monthly_amount_cents)}/month`
                  : `Monthly supporter — ${formatCents(status.data.membership.monthly_amount_cents)}/month since ${formatDate(status.data.membership.started_at)}`}
              </p>
              {status.data.membership.stripe_status === "incomplete" ? (
                // Cards-only Checkout confirms the payment before the session
                // completes, so `incomplete` here is the seconds-wide gap
                // between the subscription.created poke and the activation
                // poke. Refreshing is the whole remedy; nothing here promises
                // the portal can pay the invoice, because that's unverified.
                <p className="text-ink-soft">
                  Your first payment is still being confirmed. This usually takes a moment — refresh this page to
                  check again.
                </p>
              ) : status.data.membership.stripe_status === "past_due" ||
                status.data.membership.stripe_status === "unpaid" ? (
                <p className="rounded-lg border border-rausch/40 bg-rausch/5 px-3 py-2 text-rausch-dark">
                  Your last payment didn&apos;t go through. Update your card under Manage membership to keep your
                  membership.
                </p>
              ) : status.data.membership.stripe_status !== "active" ? (
                <p className="text-ink-soft">Status: {status.data.membership.stripe_status.replace(/_/g, " ")}</p>
              ) : null}
              {status.data.membership.cancel_at_period_end && status.data.membership.current_period_end ? (
                <p className="text-ink-soft">
                  Your membership ends {formatDate(status.data.membership.current_period_end)}.
                </p>
              ) : null}
              <button type="button" disabled={busy} onClick={() => portal.mutate()} className={secondaryButtonClass}>
                {portal.isPending ? "Opening…" : "Manage membership"}
              </button>
              <p className="text-xs text-ink-soft">
                Update your card or cancel anytime. To change the amount, cancel and start a new membership.
              </p>
            </div>
          ) : (
            <div className="mt-3 space-y-4">
              <AmountForm
                kind="monthly"
                label="Become a monthly supporter ($5/month minimum)"
                buttonLabel="Support monthly"
                initialDollars="5"
                disabled={busy}
                onSubmit={(input) => checkout.mutate(input)}
              />
              <AmountForm
                kind="one_time"
                label="One-time support ($5 minimum)"
                buttonLabel="Support once"
                initialDollars="10"
                disabled={busy}
                onSubmit={(input) => checkout.mutate(input)}
              />
              <p className="text-xs text-ink-soft">
                Monthly memberships renew until you cancel, which you can do anytime from this page. See the{" "}
                <Link to="/terms" className="underline hover:text-ink">
                  Terms of Use
                </Link>
                .
              </p>
            </div>
          )}

          {checkout.isError ? (
            <div className="mt-2">
              <ErrorNotice error={checkout.error} />
            </div>
          ) : null}
          {portal.isError ? (
            <div className="mt-2">
              <ErrorNotice error={portal.error} />
            </div>
          ) : null}

          {status.data.total_net_cents > 0 ? (
            <p className="mt-4 text-sm text-ink">
              Total support to date: <strong>{formatCents(status.data.total_net_cents)}</strong>
            </p>
          ) : null}
          <PaymentHistory payments={status.data.payments} />
        </>
      ) : null}
    </section>
  );
}
