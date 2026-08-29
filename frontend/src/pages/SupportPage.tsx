import type { MetaFunction } from "react-router";
import { Link } from "react-router";
import { APP_NAME } from "@voteapp/api-client";
import { pageMeta } from "../lib/pageMeta";

export const meta: MetaFunction = () =>
  pageMeta({
    title: `Support · ${APP_NAME}`,
    description: `Become a supporting member of ${APP_NAME} or make a one-time contribution.`,
    path: "/support",
  });

const ctaClass =
  "inline-block rounded-lg bg-rausch px-4 py-2 text-sm font-semibold text-white transition hover:bg-rausch-dark";

// Chooser for direct visits (old links, ?next after auth, type-ins). The
// Mission pitch deep-links straight to /support/member and /support/once,
// where the visitor has already made the choice and sees only that form.
export default function SupportPage() {
  return (
    <div className="mx-auto max-w-3xl space-y-6 px-4 py-8">
      <section className="space-y-3 text-body text-ink">
        <h1 className="text-title font-bold">Support {APP_NAME}</h1>
        <p>
          For a small monthly contribution, less than a cup of coffee, you can become an honorary
          member and help us keep bringing you higher-quality content. As an honorary member, you
          will get our private analysis reports on the important issues that could affect you.
        </p>
        <p>
          <Link to="/support/member" className={ctaClass}>
            Become an honorary member
          </Link>
        </p>
        <p>If you’d rather make a one-time contribution:</p>
        <p>
          <Link to="/support/once" className={ctaClass}>
            Contribute once
          </Link>
        </p>
      </section>
    </div>
  );
}
