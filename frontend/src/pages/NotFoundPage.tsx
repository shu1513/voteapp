import { Link } from "react-router";
import { useDocumentTitle } from "../lib/useDocumentTitle";

export function NotFoundPage() {
  useDocumentTitle("Page not found");
  return (
    <div className="mx-auto max-w-3xl px-4 py-16 text-center">
      <h1 className="text-2xl font-bold">Page not found</h1>
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
