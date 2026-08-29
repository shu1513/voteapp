import type { MetaFunction } from "react-router";
import { Link, useLocation } from "react-router";
import { APP_NAME, useMe } from "@voteapp/api-client";
import { SupportCheckout } from "../components/MembershipSection";
import { VerifyPrompt } from "../components/VerifyPrompt";
import { pageMeta } from "../lib/pageMeta";

export const meta: MetaFunction = () =>
  pageMeta({
    title: `One-time contribution · ${APP_NAME}`,
    description: `Make a one-time contribution to ${APP_NAME}.`,
    path: "/support/once",
  });

// One-time contribution only — no membership form competes for attention,
// and members can give here too (a one-time gift on top of a membership is
// allowed). Amount picked here, payment finished on Stripe Checkout.
export default function SupportOncePage() {
  const { me } = useMe();
  // ?next preserves the FULL url, query included: a Stripe return
  // (?membership=success) with an expired session must survive the login
  // round-trip, or the success banner and the double-charge lock are lost.
  // (The param-stripping effect lives in SupportCheckout, which does not
  // mount for logged-out visitors, so the query is still here to preserve.)
  const location = useLocation();
  const next = encodeURIComponent(location.pathname + location.search);

  return (
    <div className="mx-auto max-w-3xl space-y-6 px-4 py-8">
      <section className="space-y-3 text-body text-ink">
        <h1 className="text-title font-bold">Make a one-time contribution</h1>
        <p>
          A one-time contribution helps us keep bringing you higher-quality content, with no
          commitment.
        </p>
        <p>Choose your amount below; you will finish your payment securely on Stripe.</p>
      </section>

      {me?.email_verified ? (
        // Renders nothing when the backend reports payments unconfigured, so
        // the page never shows dead forms.
        <SupportCheckout kind="one_time" />
      ) : me ? (
        <VerifyPrompt email={me.email} />
      ) : (
        // Shown while /api/me is unresolved too; ?next returns the
        // contributor here after auth.
        <p className="text-sm">
          <Link to={`/login?next=${next}`} className="font-semibold underline hover:text-ink">
            Log in
          </Link>{" "}
          or{" "}
          <Link to={`/register?next=${next}`} className="font-semibold underline hover:text-ink">
            sign up
          </Link>{" "}
          to make a one-time contribution.
        </p>
      )}
    </div>
  );
}
