import { useState } from "react";
import { Link } from "react-router";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { apiRequest, APP_NAME, useMe } from "@voteapp/api-client";
import type { MembershipMembership, MembershipPayment, MembershipStatus } from "@voteapp/api-client";
import { ErrorNotice, LoadingNotice } from "../components/Status";
import { AmountForm, Disclaimer, formatCents, formatDate, secondaryButtonClass } from "../components/SupportCheckout";
import { VerifyPrompt } from "../components/VerifyPrompt";
import { navigateExternal } from "../lib/externalNavigation";
import { useDocumentTitle } from "../lib/useDocumentTitle";
import { MEMBERSHIP_QUERY_KEY, useMembershipStatus } from "../lib/useMembershipStatus";

// /me/membership: the one place a member manages their plan — change the
// monthly amount, cancel or keep the membership, update the card (Stripe's
// portal, deep-linked to the card screen), and read the payment history.
// docs/plans/membership-manage-page.md. Amount changes never charge today:
// the new amount bills at a renewal at least 7 days out (the CA BPC
// §17602(g)(2) notice window the backend enforces).

const linkClass = "font-medium underline hover:text-ink";
const confirmButtonClass =
  "rounded-lg bg-rausch-dark px-4 py-2 text-sm font-semibold text-white transition hover:bg-rausch disabled:cursor-not-allowed disabled:bg-line";

const DAY_MS = 86_400_000;
// Mirrors the backend's NOTICE_WINDOW_MIN_DAYS: a renewal closer than this
// cannot carry a new amount, so the change waits for the one after.
const NOTICE_WINDOW_MIN_DAYS = 7;

/** The month after `periodEnd` on the subscription's anchor day, clamped to
 * the shorter month the way Stripe clamps (anchor 31 → Feb 28 → Mar 31).
 * Mirrors the backend's nextRenewalAfter; UTC like Stripe's epochs. */
function nextRenewalAfter(periodEnd: Date, anchorDay: number): Date {
  const next = new Date(periodEnd.getTime());
  next.setUTCDate(1);
  next.setUTCMonth(next.getUTCMonth() + 1);
  const lastDay = new Date(Date.UTC(next.getUTCFullYear(), next.getUTCMonth() + 1, 0)).getUTCDate();
  next.setUTCDate(Math.min(anchorDay, lastDay));
  return next;
}

/** The renewal a change saved now would first bill: this period's end while
 * the notice can still go out 7 days ahead of it, else the renewal after.
 * A projection for the helper line; the backend records the firm date. */
export function projectedAmountStart(membership: MembershipMembership, now: Date = new Date()): Date | null {
  if (!membership.current_period_end) {
    return null;
  }
  const periodEnd = new Date(membership.current_period_end);
  const days = (periodEnd.getTime() - now.getTime()) / DAY_MS;
  return days >= NOTICE_WINDOW_MIN_DAYS
    ? periodEnd
    : nextRenewalAfter(periodEnd, new Date(membership.started_at).getUTCDate());
}

/** "$10 per month · renews October 4, 2026", with the pending change or the
 * scheduled end in the second half. Never invents a date. */
function planLine(membership: MembershipMembership): string {
  const base = `${formatCents(membership.monthly_amount_cents)} per month`;
  const end = membership.current_period_end ? formatDate(membership.current_period_end) : null;
  if (membership.cancel_at_period_end) {
    return end ? `${base} · will not renew after ${end}` : `${base} · will not renew`;
  }
  const pending = membership.pending_amount_change;
  if (pending) {
    const amount = formatCents(pending.new_amount_cents);
    return pending.starts_at ? `${base} · ${amount} from ${formatDate(pending.starts_at)}` : `${base} · ${amount} pending`;
  }
  return end ? `${base} · renews ${end}` : base;
}

/** One POST that answers with the fresh status. The in-flight GET (if any)
 * is canceled before the result is installed, so a slower GET that started
 * before the change cannot overwrite it. */
function useMembershipAction<TVars>(input: {
  request: (vars: TVars) => Promise<MembershipStatus>;
  message: (status: MembershipStatus) => string;
  setNotice: (notice: string | null) => void;
}) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: input.request,
    onMutate: () => input.setNotice(null),
    onSuccess: async (status) => {
      await queryClient.cancelQueries({ queryKey: MEMBERSHIP_QUERY_KEY });
      queryClient.setQueryData(MEMBERSHIP_QUERY_KEY, status);
      input.setNotice(input.message(status));
    },
  });
}

function membershipOf(status: MembershipStatus): MembershipMembership | null {
  return status.enabled ? status.membership : null;
}

function savedMessage(status: MembershipStatus): string {
  const membership = membershipOf(status);
  const pending = membership?.pending_amount_change;
  if (!membership) {
    return "Saved.";
  }
  if (!pending) {
    return `Saved. Your amount stays ${formatCents(membership.monthly_amount_cents)} per month.`;
  }
  const amount = `${formatCents(pending.new_amount_cents)} per month`;
  return pending.starts_at ? `Saved. ${amount} starts ${formatDate(pending.starts_at)}.` : `Saved. Your new amount is ${amount}.`;
}

function canceledMessage(status: MembershipStatus): string {
  const end = membershipOf(status)?.current_period_end;
  return end ? `Your membership will not renew after ${formatDate(end)}.` : "Your membership will not renew.";
}

function CancelControls({ membership, disabled, onCancel }: { membership: MembershipMembership; disabled: boolean; onCancel: () => void }) {
  const [confirming, setConfirming] = useState(false);
  if (!confirming) {
    return (
      <button type="button" disabled={disabled} onClick={() => setConfirming(true)} className={secondaryButtonClass}>
        Cancel membership…
      </button>
    );
  }
  return (
    <div className="rounded-lg border border-line bg-surface p-3 text-sm">
      <p className="text-ink">
        {membership.current_period_end
          ? `Your membership stays active until ${formatDate(membership.current_period_end)} and will not renew after that.`
          : "Your membership will not renew."}
      </p>
      <div className="mt-3 flex gap-3">
        <button type="button" disabled={disabled} onClick={onCancel} className={confirmButtonClass}>
          Cancel membership
        </button>
        <button type="button" disabled={disabled} onClick={() => setConfirming(false)} className={secondaryButtonClass}>
          Never mind
        </button>
      </div>
    </div>
  );
}

function PaymentHistory({ payments, totalNetCents }: { payments: MembershipPayment[]; totalNetCents: number }) {
  if (payments.length === 0) {
    return null;
  }
  return (
    // Closed by default (user decision): the list is context, not the task.
    <details className="rounded-xl border border-line bg-white p-4">
      <summary className="cursor-pointer text-heading font-semibold">Recent payments</summary>
      <p className="mt-2 text-sm text-ink">
        Total support to date: <strong>{formatCents(totalNetCents)}</strong>
      </p>
      <ul className="mt-2 divide-y divide-line text-sm">
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
    </details>
  );
}

function SupportLinks() {
  return (
    <p className="flex flex-wrap gap-3 text-sm">
      <Link
        to="/support/member"
        className="inline-block rounded-lg bg-green-700 px-4 py-2 font-semibold text-white transition hover:bg-green-800"
      >
        Become an honorary member
      </Link>
      <Link to="/support/once" className={`${secondaryButtonClass} inline-block`}>
        Support once
      </Link>
    </p>
  );
}

function MemberPanel({ membership }: { membership: MembershipMembership }) {
  const [notice, setNotice] = useState<string | null>(null);
  const changeAmount = useMembershipAction({
    request: (amountCents: number) =>
      apiRequest<MembershipStatus>("/api/me/membership/amount", { method: "POST", body: { amount_cents: amountCents } }),
    message: savedMessage,
    setNotice,
  });
  const cancel = useMembershipAction<void>({
    request: () => apiRequest<MembershipStatus>("/api/me/membership/cancel", { method: "POST", body: {} }),
    message: canceledMessage,
    setNotice,
  });
  const resume = useMembershipAction<void>({
    request: () => apiRequest<MembershipStatus>("/api/me/membership/resume", { method: "POST", body: {} }),
    message: () => "Welcome back — your membership continues.",
    setNotice,
  });
  const portal = useMutation({
    mutationFn: () =>
      apiRequest<{ url: string }>("/api/me/membership/portal", {
        method: "POST",
        body: { flow: "payment_method_update" },
      }),
    onSuccess: (response) => {
      navigateExternal(response.url);
    },
  });

  // Every control locks while any change is in flight; the portal redirect
  // stays "in flight" until the browser actually leaves the page.
  const busy = changeAmount.isPending || cancel.isPending || resume.isPending || portal.isPending || portal.isSuccess;
  const status = membership.stripe_status;
  const canceling = membership.cancel_at_period_end;
  const paymentFailed = status === "past_due" || status === "unpaid";
  // Matches the backend's refusals: no amount change while the first payment
  // is unconfirmed, after a failed payment that Stripe gave up on, or while
  // the membership is set to end (Keep membership first).
  const canChangeAmount = (status === "active" || status === "past_due") && !canceling;
  const projectedStart = projectedAmountStart(membership);

  return (
    <>
      <section className="rounded-xl border border-line bg-white p-4">
        {status === "active" ? (
          <p className="text-sm text-ink">
            Thank you. Because of supporters like you, {APP_NAME} stays independent and free for every voter.
          </p>
        ) : null}
        <p className="mt-2 font-medium text-ink">{planLine(membership)}</p>
        {status === "incomplete" ? (
          // Cards-only Checkout confirms the payment before the session
          // completes, so `incomplete` is the seconds-wide gap between the
          // subscription.created poke and the activation poke. Refreshing is
          // the whole remedy.
          <p className="mt-2 text-sm text-ink-soft">
            Your first payment is still being confirmed. This usually takes a moment; refresh this page to
            check again.
          </p>
        ) : null}
        {/* Always-mounted live region: confirmations appear inside an
            existing region, the reliably-announced case. */}
        <p role="status" className="mt-2 min-h-5 text-sm font-medium text-green-900">
          {notice ?? ""}
        </p>
        {canceling ? (
          <div className="mt-1">
            <button type="button" disabled={busy} onClick={() => resume.mutate()} className={secondaryButtonClass}>
              {resume.isPending ? "Saving…" : "Keep membership"}
            </button>
            {resume.isError ? (
              <div className="mt-2">
                <ErrorNotice error={resume.error} />
              </div>
            ) : null}
          </div>
        ) : null}
      </section>

      {canChangeAmount ? (
        <section className="rounded-xl border border-line bg-white p-4">
          <h2 className="text-heading font-semibold">Change amount</h2>
          <div className="mt-2">
            <AmountForm
              // Remount when the amount in force changes (a renewal billed the
              // new price), so the field never shows a stale prefill.
              key={membership.monthly_amount_cents}
              inputId="membership-amount-dollars"
              label="New monthly amount"
              buttonLabel={changeAmount.isPending ? "Saving…" : "Save new amount"}
              initialDollars={String(membership.monthly_amount_cents / 100)}
              disabled={busy}
              // Re-saving the current amount is a no-op — unless a change is
              // pending, when it withdraws that change.
              unchangedCents={membership.pending_amount_change ? null : membership.monthly_amount_cents}
              onSubmit={(amountCents) => changeAmount.mutate(amountCents)}
            >
              <p className="text-xs text-ink-soft">
                {projectedStart
                  ? `Your new amount starts on ${formatDate(projectedStart.toISOString())}. `
                  : "Your new amount starts at a later renewal. "}
                Nothing is charged today.
              </p>
            </AmountForm>
          </div>
          {changeAmount.isError ? (
            <div className="mt-2">
              <ErrorNotice error={changeAmount.error} />
            </div>
          ) : null}
        </section>
      ) : null}

      <section className="space-y-3 rounded-xl border border-line bg-white p-4">
        {paymentFailed ? (
          <p className="rounded-lg border border-rausch/40 bg-rausch/5 px-3 py-2 text-sm text-rausch-dark">
            Your last payment didn&apos;t go through.
          </p>
        ) : null}
        <div>
          <button type="button" disabled={busy} onClick={() => portal.mutate()} className={secondaryButtonClass}>
            {portal.isPending ? "Opening…" : "Update payment method"}
          </button>
          {portal.isError ? (
            <div className="mt-2">
              <ErrorNotice error={portal.error} />
            </div>
          ) : null}
        </div>
        {status !== "incomplete" && !canceling ? (
          <div>
            <CancelControls membership={membership} disabled={busy} onCancel={() => cancel.mutate()} />
            {cancel.isError ? (
              <div className="mt-2">
                <ErrorNotice error={cancel.error} />
              </div>
            ) : null}
          </div>
        ) : null}
      </section>
    </>
  );
}

function MembershipManager() {
  const status = useMembershipStatus();

  if (status.isPending) {
    return <LoadingNotice text="Loading…" />;
  }
  if (status.isError) {
    return <ErrorNotice error={status.error} />;
  }
  if (!status.data.enabled) {
    return (
      <p className="rounded-xl border border-line bg-surface p-4 text-sm text-ink-soft">
        Payments are temporarily unavailable. Please check back later.
      </p>
    );
  }
  const { membership, payments, total_net_cents } = status.data;

  return (
    <>
      {membership ? (
        <MemberPanel membership={membership} />
      ) : (
        <section className="rounded-xl border border-line bg-white p-4">
          <p className="text-sm text-ink">You don&apos;t have a monthly membership right now.</p>
          <div className="mt-3">
            <SupportLinks />
          </div>
        </section>
      )}
      <PaymentHistory payments={payments} totalNetCents={total_net_cents} />
      <Disclaimer />
    </>
  );
}

export function MembershipPage() {
  useDocumentTitle("Your membership");
  const { me, isLoading } = useMe();

  if (isLoading || me === undefined) {
    return <LoadingNotice text="Loading…" />;
  }
  if (me === null) {
    return (
      <div className="mx-auto max-w-md px-4 py-10 text-center">
        <p className="text-ink-soft">Log in to manage your membership.</p>
        <p className="mt-4">
          {/* ?next: the emails link straight here. */}
          <Link
            to="/login?next=%2Fme%2Fmembership"
            className="rounded-lg bg-rausch px-4 py-2 font-semibold text-white transition hover:bg-rausch-dark"
          >
            Log in
          </Link>
        </p>
      </div>
    );
  }
  // The membership endpoints are verification-gated.
  if (!me.email_verified) {
    return <VerifyPrompt email={me.email} />;
  }

  return (
    <div className="mx-auto max-w-2xl space-y-4 px-4 py-8">
      <h1 className="text-title font-bold">Your membership</h1>
      <MembershipManager />
      <p className="text-sm">
        <Link to="/me/settings" className={linkClass}>
          Back to Settings
        </Link>
      </p>
    </div>
  );
}

export default MembershipPage;
