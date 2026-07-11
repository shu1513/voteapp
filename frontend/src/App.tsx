import { useEffect, useRef } from "react";
import { Link, Outlet, ScrollRestoration, useLocation, useNavigate } from "react-router";
import { RouteError } from "./components/RouteError";
import { useLogout, useMe } from "@voteapp/api-client";

function AccountNav() {
  const { me, isLoading } = useMe();
  const logout = useLogout();
  const navigate = useNavigate();

  if (isLoading) {
    return null;
  }
  if (!me) {
    return (
      <span className="flex items-center gap-4">
        <Link to="/login" className="text-ink-soft hover:text-ink">
          Log in
        </Link>
        <Link
          to="/register"
          className="rounded-lg bg-rausch px-3 py-1.5 font-semibold text-white transition hover:bg-rausch-dark"
        >
          Sign up
        </Link>
      </span>
    );
  }
  return (
    <span className="flex items-center gap-4">
      <span className="text-ink">Hi {me.first_name}</span>
      <Link to="/me/ballot" className="text-ink-soft hover:text-ink">
        My ballot
      </Link>
      <Link to="/me/follows" className="text-ink-soft hover:text-ink">
        Following
      </Link>
      <Link to="/me/settings" className="text-ink-soft hover:text-ink">
        Settings
      </Link>
      <button
        type="button"
        className="text-ink-soft hover:text-ink"
        onClick={() =>
          logout.mutate(undefined, {
            onSuccess: () => {
              navigate("/");
            },
          })
        }
      >
        Log out
      </button>
    </span>
  );
}

export function App() {
  const location = useLocation();
  const mainRef = useRef<HTMLElement>(null);
  const lastPathname = useRef(location.pathname);

  // In-app navigation keeps focus wherever it was in the old page; move it
  // to the new page's content so keyboard and screen-reader users land where
  // sighted users look. Compared against the previous pathname (not a
  // first-render flag) so the initial load keeps the browser's default focus
  // even under StrictMode's double effect run.
  useEffect(() => {
    if (lastPathname.current === location.pathname) {
      return;
    }
    lastPathname.current = location.pathname;
    mainRef.current?.focus();
  }, [location.pathname]);

  return (
    <div className="min-h-screen bg-white text-ink">
      <a
        href="#main"
        className="sr-only focus:not-sr-only focus:absolute focus:left-4 focus:top-4 focus:z-20 focus:rounded-lg focus:bg-white focus:px-3 focus:py-2 focus:shadow-md"
      >
        Skip to content
      </a>
      <header className="border-b border-line bg-white">
        <div className="mx-auto flex max-w-3xl items-center justify-between px-4 py-4">
          <Link to="/" className="text-xl font-extrabold tracking-tight text-rausch">
            VoteApp
          </Link>
          <nav className="flex items-center gap-4 text-sm">
            <AccountNav />
          </nav>
        </div>
      </header>
      <main id="main" ref={mainRef} tabIndex={-1} className="outline-none">
        <Outlet />
      </main>
      <ScrollRestoration />
      <footer className="mt-16 border-t border-line py-8 text-center text-xs text-ink-soft">
        <p>
          Independent, nonpartisan, AI-assisted election research. Not an official election source —{" "}
          <Link to="/disclaimer" className="underline hover:text-ink">
            read the Disclaimer
          </Link>
          .
        </p>
      </footer>
    </div>
  );
}

export function ErrorBoundary() {
  return <RouteError />;
}

export default App;
