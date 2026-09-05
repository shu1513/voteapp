import { useEffect, useState } from "react";
import { Link, useSearchParams } from "react-router";
import { useMutation } from "@tanstack/react-query";
import { apiRequest, APP_NAME } from "@voteapp/api-client";
import type { MembershipKind, MembershipMembership } from "@voteapp/api-client";
import { ErrorNotice } from "./Status";
import { navigateExternal } from "../lib/externalNavigation";
import { trackSettled } from "../lib/usage";
import { useMembershipStatus } from "../lib/useMembershipStatus";

// Membership payments through Stripe Checkout (full-page redirect, no Stripe
// SDK here). Two surfaces share the pieces in this file, and the membership
// page (pages/MembershipPage.tsx) reuses the form, hooks, and formatters:
//   - SupportCheckout: the single-kind checkout on /support/member and
//     /support/once (the visitor already chose, so only that form shows),
//   - MembershipThanks: the compact member acknowledgement on /mission.
// The status query itself lives in lib/useMembershipStatus (the My Draft
// membership ask shares it). Managing an existing membership (amount,
// cancel, card) lives on /me/membership — docs/plans/membership-manage-page.md.
// Not a nonprofit: copy says the money runs the service and never implies
// a candidate, campaign, or charity — see docs/plans/membership-contributions.md.

// Whole dollars; mirrored from the backend's checkout validation
// (MEMBERSHIP_CHECKOUT_MIN/MAX_AMOUNT_CENTS). The server re-checks both.
const MIN_DOLLARS = 5;
const MAX_DOLLARS = 1000;

export const buttonClass =
  "rounded-lg bg-rausch px-4 py-2 text-sm font-semibold text-white transition hover:bg-rausch-dark disabled:cursor-not-allowed disabled:bg-line";
// The member checkout CTA: inviting green (the app's affirmative color)
// instead of rausch, which reads as destructive/alert elsewhere.
const memberButtonClass =
  "rounded-lg bg-green-700 px-4 py-2 text-sm font-semibold text-white transition hover:bg-green-800 disabled:cursor-not-allowed disabled:bg-line";
export const secondaryButtonClass =
  "rounded-lg border border-line bg-white px-4 py-2 text-sm font-semibold text-ink transition hover:border-rausch disabled:cursor-not-allowed disabled:text-ink-soft";
const inputClass =
  "mt-1 w-full rounded-md border border-line px-3 py-2 shadow-sm focus:border-ink focus:outline-none";
const linkClass = "font-medium underline hover:text-ink";

const currency = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" });

export function formatCents(cents: number): string {
  return currency.format(cents / 100);
}

// Timestamps (instants), unlike election dates — Date parsing is correct here.
export function formatDate(iso: string): string {
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

export function AmountForm({
  inputId,
  label,
  buttonLabel,
  buttonClassName = buttonClass,
  initialDollars,
  disabled,
  unchangedCents = null,
  onSubmit,
  children,
}: {
  inputId: string;
  label: string;
  buttonLabel: string;
  buttonClassName?: string;
  initialDollars: string;
  disabled: boolean;
  /** The amount already in force: submitting it would change nothing, so
   * the button stays disabled while the input equals it. */
  unchangedCents?: number | null;
  onSubmit: (amountCents: number) => void;
  /** Helper text rendered under the input (the membership page's "starts
   * on" line). */
  children?: React.ReactNode;
}) {
  const [raw, setRaw] = useState(initialDollars);
  const parsed = parseDollars(raw);
  const message = parsed && "message" in parsed ? parsed.message : null;
  const cents = parsed && "cents" in parsed ? parsed.cents : null;
  const submittable = cents !== null && cents !== unchangedCents && !disabled;

  return (
    <form
      onSubmit={(event) => {
        event.preventDefault();
        if (submittable) {
          onSubmit(cents);
        }
      }}
    >
      <div className="flex items-end gap-3">
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
        <button type="submit" disabled={!submittable} className={`${buttonClassName} mb-5`}>
          {buttonLabel}
        </button>
      </div>
      {children}
    </form>
  );
}

/** Checkout returns with ?membership=success|canceled. Read once, then strip
 * the param so a reload doesn't re-announce an old outcome. */
function useCheckoutOutcome(): string | null {
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
  return outcome;
}

function useCheckoutMutation() {
  return useMutation({
    mutationFn: (input: { kind: MembershipKind; amountCents: number }) => {
      const request = apiRequest<{ url: string }>("/api/me/membership/checkout", {
        method: "POST",
        body: { kind: input.kind, amount_cents: input.amountCents },
      });
      // Usage: a Checkout session was requested (not proof of payment);
      // the kind only, never the amount.
      trackSettled(request, "checkout_start", { kind: input.kind });
      return request;
    },
    onSuccess: (response) => {
      navigateExternal(response.url);
    },
  });
}

function OutcomeBanners({ outcome }: { outcome: string | null }) {
  return (
    <>
      {outcome === "success" ? (
        <p role="status" className="mt-2 rounded-lg border border-green-700/40 bg-green-50 px-3 py-2 text-sm text-green-900">
          Thank you for your support! Your payment may take a moment to appear on your{" "}
          <Link to="/me/membership" className={linkClass}>
            membership page
          </Link>
          .
        </p>
      ) : null}
      {outcome === "canceled" ? (
        <p role="status" className="mt-2 rounded-lg border border-line bg-surface px-3 py-2 text-sm text-ink-soft">
          Checkout was canceled. Nothing was charged.
        </p>
      ) : null}
    </>
  );
}

export function Disclaimer() {
  return (
    <p className="mt-1 text-sm text-ink-soft">
      {APP_NAME} is independently operated. Optional payments support operating the service, not any
      candidate, campaign, committee, party, or charity. Payments provide no influence over our
      content and are not eligible for a charitable-contribution receipt.
    </p>
  );
}

/** What an existing subscription holder sees instead of the monthly form:
 * the backend answers 409 to a second monthly checkout while any nonterminal
 * subscription exists, so the form would only fail. `incomplete` (first
 * payment not confirmed yet) is never called a member. */
function ExistingMembership({ membership }: { membership: MembershipMembership }) {
  const status = membership.stripe_status;
  return (
    <p className="mt-3 text-sm text-ink">
      {status === "incomplete"
        ? "Your membership is being set up."
        : status === "active" || status === "past_due"
          ? "You're already an honorary member — thank you."
          : "You already have a membership."}{" "}
      <Link to="/me/membership" className={linkClass}>
        Manage membership
      </Link>
    </p>
  );
}

/** Single-kind checkout for the dedicated support pages: the visitor already
 * chose monthly or one-time on the way in, so only that form shows.
 * - monthly: an existing member sees a link to their membership page (a
 *   second subscription would only 409) instead of the form.
 * - one_time: the form shows even for members — a one-time gift on top of a
 *   membership is allowed. */
export function SupportCheckout({ kind }: { kind: MembershipKind }) {
  const outcome = useCheckoutOutcome();
  const status = useMembershipStatus();
  const checkout = useCheckoutMutation();

  if (status.isPending) {
    return null;
  }
  // This page's copy tells the visitor to pick an amount below — silence
  // here would read as broken.
  if (status.data && !status.data.enabled) {
    return (
      <p className="rounded-xl border border-line bg-surface p-4 text-sm text-ink-soft">
        Payments are temporarily unavailable. Please check back later.
      </p>
    );
  }

  // The redirect stays "in flight" until the browser leaves the page, so the
  // button stays locked after success too (double-click guard). The return
  // from a successful Checkout locks it as well: the webhook may not have
  // recorded the payment yet, and a second monthly checkout in that gap is
  // a real second charge the backend can only cancel forward (its first
  // month needs a manual refund).
  const busy = outcome === "success" || checkout.isPending || checkout.isSuccess;

  return (
    <section className="rounded-xl border border-line bg-white p-4">
      <OutcomeBanners outcome={outcome} />
      {status.isError ? (
        <div className="mt-2">
          <ErrorNotice error={status.error} />
        </div>
      ) : null}
      {status.data ? (
        <>
          <Disclaimer />

          {kind === "monthly" && status.data.membership ? (
            <ExistingMembership membership={status.data.membership} />
          ) : (
            <div className="mt-3 space-y-4">
              <AmountForm
                inputId={`membership-${kind}-dollars`}
                label={
                  kind === "monthly"
                    ? "Monthly amount ($5/month minimum)"
                    : "One-time support ($5 minimum)"
                }
                buttonLabel={kind === "monthly" ? "Become an honorary member" : "Support once"}
                buttonClassName={kind === "monthly" ? memberButtonClass : undefined}
                initialDollars="10"
                disabled={busy}
                onSubmit={(amountCents) => checkout.mutate({ kind, amountCents })}
              />
              <p className="text-xs text-ink-soft">
                {kind === "monthly"
                  ? "Monthly memberships renew until you cancel, which you can do anytime from your account. "
                  : "You will be charged once. "}
                See the{" "}
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

          {/* The other kind, one line, so neither page is a dead end for a
              visitor who would rather give the other way. */}
          <p className="mt-4 text-sm text-ink-soft">
            {kind === "monthly" ? (
              <>
                Prefer a one-time contribution?{" "}
                <Link to="/support/once" className={linkClass}>
                  Support once
                </Link>
              </>
            ) : (
              <>
                Prefer to become an honorary member?{" "}
                <Link to="/support/member" className={linkClass}>
                  Support monthly
                </Link>
              </>
            )}
          </p>
        </>
      ) : null}
    </section>
  );
}

// Compact member acknowledgement for the Mission page: renders only for an
// existing member (any nonterminal subscription) so the pitch buttons above
// aren't the page's last word to someone who already pays. Acquisition lives
// on the /support pages; management on /me/membership.
export function MembershipThanks() {
  const status = useMembershipStatus();

  // Only a paid-up membership earns the thanks (matches the Settings profile
  // line): pending/past-due states would make this line a lie, and the pitch
  // buttons above already lead to the page that explains the real state.
  if (!status.data?.enabled || status.data.membership?.stripe_status !== "active") {
    return null;
  }

  return (
    <div className="rounded-xl border border-line bg-white p-4 text-sm">
      <p className="font-medium text-ink">You are a supporting member. Thank you!</p>
      <Link to="/me/membership" className={`${secondaryButtonClass} mt-2 inline-block`}>
        Manage membership
      </Link>
    </div>
  );
}
