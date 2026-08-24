import type { MetaFunction } from "react-router";
import { Link } from "react-router";
import { APP_NAME, useMe } from "@voteapp/api-client";
import { MembershipSection } from "../components/MembershipSection";
import { VerifyPrompt } from "../components/VerifyPrompt";
import { pageMeta } from "../lib/pageMeta";

export const meta: MetaFunction = () =>
  pageMeta({
    title: `Our mission · ${APP_NAME}`,
    description: `Why ${APP_NAME} exists, and how to support it.`,
    path: "/mission",
  });

// Public mission page: the pitch reads without an account; the payment forms
// (MembershipSection) additionally need a verified login because the
// membership endpoints are verified-only — logged-out readers get the
// login/signup path instead, and unverified accounts a verify nudge.
// Acquisition lives here; management stays on Settings, where the same
// section shows a member their plan and the portal button.
export default function MissionPage() {
  const { me } = useMe();

  return (
    <div className="mx-auto max-w-3xl space-y-6 px-4 py-8">
      <section>
        <h1 className="text-2xl font-bold">Our mission</h1>
        {/* Placeholder statement — the founder's own words replace this. */}
        <div className="mt-3 space-y-3 text-sm text-ink">
          <p>
            {APP_NAME} exists so that anyone can walk into their polling place knowing what is on their
            ballot and where every candidate actually stands — without wading through ads, spin, or a
            dozen county websites.
          </p>
          <p>
            The service is independently operated and free to use. It runs on research time and server
            bills, not on selling attention or data.
          </p>
        </div>
      </section>

      <section>
        <h2 className="text-lg font-semibold">Support {APP_NAME}</h2>
        <p className="mt-2 text-sm text-ink-soft">
          If it helps you vote with confidence, you can help keep it running — as a monthly member or
          with a one-time contribution. Payments support operating the service — not any candidate,
          campaign, committee, party, or charity.
        </p>
      </section>

      {me?.email_verified ? (
        // Renders nothing when the backend reports payments unconfigured, so
        // the page never shows dead forms.
        <MembershipSection />
      ) : me ? (
        // The standard unverified interstitial: names the address and offers
        // a real resend (nothing else on this page can).
        <VerifyPrompt email={me.email} />
      ) : (
        // Shown while /api/me is unresolved too, same tradeoff as the header
        // nav: a self-correcting logged-out CTA beats an invisible one, and
        // warm navigation has the session cached anyway.
        // ?next brings the prospective supporter back here after auth
        // instead of into normal onboarding.
        <p className="text-sm">
          <Link to="/login?next=%2Fmission" className="font-semibold underline hover:text-ink">
            Log in
          </Link>{" "}
          or{" "}
          <Link to="/register?next=%2Fmission" className="font-semibold underline hover:text-ink">
            sign up
          </Link>{" "}
          to become a member or make a one-time contribution.
        </p>
      )}
    </div>
  );
}
