import { Link, useRouteError } from "react-router-dom";
import { useDocumentTitle } from "../lib/useDocumentTitle";

// errorElement for the router root: a render error anywhere in the tree
// otherwise unmounts the app into a blank page. Rendered outside <App/>, so
// it carries its own minimal shell.
export function RouteError() {
  useDocumentTitle("Something went wrong");
  const error = useRouteError();
  if (import.meta.env.DEV) {
    console.error(error);
  }
  return (
    <div className="mx-auto max-w-3xl px-4 py-16 text-center text-ink">
      <h1 className="text-2xl font-bold">Something went wrong</h1>
      <p className="mt-2 text-ink-soft">The page hit an unexpected error. Reloading usually fixes it.</p>
      <div className="mt-6 flex items-center justify-center gap-4">
        <button
          type="button"
          onClick={() => window.location.reload()}
          className="rounded-lg bg-rausch px-4 py-2 font-semibold text-white transition hover:bg-rausch-dark"
        >
          Reload
        </button>
        <Link to="/" reloadDocument className="text-ink-soft underline hover:text-ink">
          Go to the home page
        </Link>
      </div>
    </div>
  );
}
