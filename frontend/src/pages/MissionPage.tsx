import type { MetaFunction } from "react-router";
import { Link } from "react-router";
import { APP_NAME, useMe } from "@voteapp/api-client";
import { EmailPreferenceToggles } from "../components/EmailPreferenceToggles";
import { MembershipSection } from "../components/MembershipSection";
import { VerifyPrompt } from "../components/VerifyPrompt";
import { pageMeta } from "../lib/pageMeta";

export const meta: MetaFunction = () =>
  pageMeta({
    title: `Mission · ${APP_NAME}`,
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
      <section className="space-y-3 text-body text-ink">
        <h1 className="font-sans text-title font-bold">Mission</h1>

        <h2 className="pt-2 font-sans text-heading font-semibold">Why do we do what we do?</h2>
        <p>When we go voting, two problems show up immediately:</p>
        <ol className="list-decimal space-y-1 pl-6">
          <li>We don’t know if our votes actually matter.</li>
          <li>We don’t know who most of these candidates are.</li>
        </ol>

        <h2 className="pt-2 font-sans text-heading font-semibold">Do our votes matter?</h2>
        <p>
          In a presidential election, unless we live in a key district in a swing state, our vote is
          one among more than 150 million. But in a city council or school board race, a few hundred
          votes can decide the outcome. Ironically, these local offices affect our daily lives far
          more — our schools, our street safety, our water quality, our local taxes.
        </p>

        <h2 className="pt-2 font-sans text-heading font-semibold">Who are these candidates?</h2>
        <p>
          The bigger the election — think presidential — the more media coverage it gets. Smaller
          elections, the local races where our vote is most powerful, usually get almost none. And
          what little information we do get is usually marketing written by the campaigns themselves.
        </p>

        <h2 className="pt-2 font-sans text-heading font-semibold">What {APP_NAME} does</h2>
        <p>
          The purpose of {APP_NAME} is to give real track records of candidates based on what they
          actually did, so we can see more clearly who these candidates are, and make a decision on
          who to pick based on the issues that matter to us — not on their ads.
        </p>

        <h2 className="pt-2 font-sans text-heading font-semibold">How we do it</h2>
        <p>
          We use AI (from American companies only) to research public sources, validate everything
          against multiple methods before it’s written, and run quality passes with both humans and
          AI. Keeping this information current — new elections, new candidates, new records — takes
          constant effort from our staff and money for AI usage.
        </p>

        <p>To literally keep us alive, you can help in three ways.</p>
        <ol className="list-decimal space-y-2 pl-6">
          <li>
            <span className="font-semibold">Become a supporting member</span> — a small monthly
            contribution, less than a cup of coffee, helps us keep our operations going.
          </li>
          <li>
            <span className="font-semibold">Make a one-time contribution</span> — if you’d rather
            help once.
          </li>
          <li>
            <span className="font-semibold">Subscribe to our emails</span> — free. We will keep you
            updated on the elections and issues most important to you. Our emails are very
            occasional, and we will never spam.
            {me?.email_verified ? (
              // The two subscription opt-ins this pitch is about, editable in
              // place (Settings still carries the full set). Verified-only,
              // like the endpoint behind them.
              <EmailPreferenceToggles only={["email_digest", "email_issue_updates"]} />
            ) : null}
          </li>
        </ol>

        <p>
          We understand that life is hard. And life has been hard. So we don’t expect this
          contribution from anyone, and we will keep this site running for as long as we can
          financially keep it, regardless. But if you believe as we believe, join us.
        </p>

        <p className="text-ink-soft">
          Payments support operating the service — not any candidate, campaign, committee, party, or
          charity.
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
