import type { MetaFunction } from "react-router";
import { Link } from "react-router";
import { APP_NAME, useMe } from "@voteapp/api-client";
import { SupportCheckout } from "../components/MembershipSection";
import { VerifyPrompt } from "../components/VerifyPrompt";
import { pageMeta } from "../lib/pageMeta";

export const meta: MetaFunction = () =>
  pageMeta({
    title: `Become an honorary member · ${APP_NAME}`,
    description: `Become an honorary member of ${APP_NAME}.`,
    path: "/support/member",
  });

// Monthly membership only — the visitor chose this on the way in, so no
// one-time form competes for attention. The amount is picked here because
// Stripe's hosted Checkout cannot take a customer-chosen recurring amount;
// the button hands off to Stripe to pay. Verified login required by the
// membership endpoints; an existing member sees their plan and the portal.
export default function SupportMemberPage() {
  const { me } = useMe();

  return (
    <div className="mx-auto max-w-3xl space-y-6 px-4 py-8">
      <section className="space-y-3 text-body text-ink">
        <h1 className="text-title font-bold">Become an honorary member</h1>
        <p>
          For a small monthly contribution, less than a cup of coffee, you can become an honorary
          member and help us keep bringing you higher-quality content.
        </p>
        <p>Choose your monthly amount below; you will finish your payment securely on Stripe.</p>
      </section>

      {me?.email_verified ? (
        // Renders nothing when the backend reports payments unconfigured, so
        // the page never shows dead forms.
        <SupportCheckout kind="monthly" />
      ) : me ? (
        <VerifyPrompt email={me.email} />
      ) : (
        // Shown while /api/me is unresolved too; ?next returns the
        // prospective member here after auth.
        <p className="text-sm">
          <Link to="/login?next=%2Fsupport%2Fmember" className="font-semibold underline hover:text-ink">
            Log in
          </Link>{" "}
          or{" "}
          <Link to="/register?next=%2Fsupport%2Fmember" className="font-semibold underline hover:text-ink">
            sign up
          </Link>{" "}
          to become a member.
        </p>
      )}
    </div>
  );
}
