import { data, Link } from "react-router";
import type { MetaFunction } from "react-router";
import { APP_NAME } from "@voteapp/api-client";
import { pageMeta } from "../lib/pageMeta";
import { useDocumentTitle } from "../lib/useDocumentTitle";

// The splat route answered 200 for unknown URLs, so crawlers could index them
// as soft 404s. Returning data() with a 404 status (not throwing) keeps this
// page rendering and leaves the root error boundary — which reports to
// Sentry and says "Something went wrong" — out of it.
export function loader() {
  return data(null, { status: 404 });
}

export const meta: MetaFunction = () => pageMeta({ title: `Page not found · ${APP_NAME}` });

export function NotFoundPage() {
  useDocumentTitle("Page not found");
  return (
    <div className="mx-auto max-w-3xl px-4 py-16 text-center">
      <h1 className="text-title font-bold">Page not found</h1>
      <p className="mt-2 text-ink-soft">That page doesn't exist or may have moved.</p>
      <Link
        to="/"
        className="mt-6 inline-block rounded-lg bg-rausch px-4 py-2 font-semibold text-white transition hover:bg-rausch-dark"
      >
        Find your ballot
      </Link>
    </div>
  );
}

export default NotFoundPage;
