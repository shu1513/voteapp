import { Link } from "react-router";
import { useMembershipStatus } from "../lib/useMembershipStatus";

// The honorary-member ask at the one moment a registered user feels done:
// the same first visit that shows the finished-draft milestone (`show` is
// that visibility). Registered users who are not yet members, only —
// guests already get the sign-up ask, and members need nothing. Sits once
// below whichever view is on (cards or ballot sheets), never on both.
// Leads to the Mission page, which carries the member/one-time buttons.
// Nudge green (the "Set your address" tint), not the pick green and not
// the rausch of sign-up buttons: a distinct, quiet ask. Hidden entirely
// when Stripe is not configured on the deployment (`enabled: false`).

export function DraftMembershipCta({ show }: { show: boolean }) {
  const status = useMembershipStatus({ enabled: show });
  if (!show || !status.data?.enabled || status.data.membership !== null) {
    return null;
  }
  return (
    <section
      aria-label="Support this work"
      className="mt-6 rounded-xl border border-nudge-line bg-nudge px-4 py-3 text-sm text-ink"
    >
      <p>Your draft is done. Help us keep this research free and current for every voter.</p>
      <p className="mt-2">
        <Link
          to="/mission"
          className="inline-block rounded-lg bg-nudge-deep px-3 py-1.5 font-semibold text-white transition hover:bg-green-900"
        >
          See our mission and join us
        </Link>
      </p>
    </section>
  );
}
