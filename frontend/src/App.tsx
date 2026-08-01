import { useEffect, useRef } from "react";
import { Link, Outlet, ScrollRestoration, useLocation, useNavigate } from "react-router";
import { Menu, MenuButton, MenuItem, MenuItems } from "@headlessui/react";
import { RouteError } from "./components/RouteError";
import { TermsRenewalGate } from "./components/TermsRenewalGate";
import { APP_NAME, VERIFY_WITH_OFFICIALS_NOTE, useLogout, useMe } from "@voteapp/api-client";

/**
 * The greeting lives beside the logo, not in the nav: sitting between
 * clickable links made it read as a link itself. Dark navy + semibold — a
 * weight and color no link in the header uses — plus no hover state keep it
 * visibly inert.
 */
function Greeting() {
  const { me } = useMe();
  if (!me) {
    return null;
  }
  return <span className="text-sm font-semibold text-navy">Hi {me.first_name}</span>;
}

function AccountNav() {
  const { me } = useMe();
  const logout = useLogout();
  const navigate = useNavigate();

  // While /api/me is unresolved (SSR, or a cold-started API taking tens of
  // seconds), default to the logged-out links rather than an empty header —
  // an invisible Log in/Sign up costs signups, while a signed-in visitor
  // briefly seeing them is harmless and self-corrects when /api/me lands.
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

  function signOut() {
    logout.mutate(undefined, {
      onSuccess: () => {
        navigate("/");
      },
    });
  }

  // Five inline items wrap into a broken two-line header on phones, so the
  // signed-in nav is inline links on sm+ and a menu below that breakpoint.
  return (
    <>
      <span className="hidden items-center gap-4 sm:flex">
        <Link to="/me/ballot" className="text-ink-soft hover:text-ink">
          My Elections
        </Link>
        <Link to="/me/follows" className="text-ink-soft hover:text-ink">
          My Candidates
        </Link>
        <Link to="/me/settings" className="text-ink-soft hover:text-ink">
          Settings
        </Link>
        <button type="button" className="text-ink-soft hover:text-ink" onClick={signOut}>
          Log out
        </button>
      </span>
      <Menu as="div" className="relative sm:hidden">
        <MenuButton className="rounded-lg border border-line px-3 py-1.5 font-medium text-ink">
          Menu <span aria-hidden="true">▾</span>
        </MenuButton>
        <MenuItems className="absolute right-0 z-20 mt-2 w-44 rounded-xl border border-line bg-white py-1 shadow-lg focus:outline-none">
          <MenuItem>
            <Link to="/me/ballot" className="block px-4 py-2 text-ink data-[focus]:bg-surface">
              My Elections
            </Link>
          </MenuItem>
          <MenuItem>
            <Link to="/me/follows" className="block px-4 py-2 text-ink data-[focus]:bg-surface">
              My Candidates
            </Link>
          </MenuItem>
          <MenuItem>
            <Link to="/me/settings" className="block px-4 py-2 text-ink data-[focus]:bg-surface">
              Settings
            </Link>
          </MenuItem>
          <MenuItem>
            <button
              type="button"
              onClick={signOut}
              className="block w-full px-4 py-2 text-left text-ink data-[focus]:bg-surface"
            >
              Log out
            </button>
          </MenuItem>
        </MenuItems>
      </Menu>
    </>
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
          <span className="flex items-baseline gap-3">
            <Link to="/" className="text-xl font-extrabold tracking-tight text-rausch">
              {APP_NAME}
            </Link>
            <Greeting />
          </span>
          <nav className="flex items-center gap-4 text-sm">
            <AccountNav />
          </nav>
        </div>
      </header>
      <main id="main" ref={mainRef} tabIndex={-1} className="outline-none">
        <Outlet />
      </main>
      <TermsRenewalGate />
      <ScrollRestoration />
      {/* The old footer repeated the home page's pitch line and buried the
          Disclaimer inside it. The pitch now sits in the hero where it is
          read, and the footer does the job a footer is for: reaching every
          document from every page, which is also what keeps the clickwrap's
          named documents permanently available rather than only at the gate. */}
      <footer className="mt-16 border-t border-line py-8 text-center text-xs text-ink-soft">
        {/* Reaches everyone who sees results, including the people the
            clickwrap never reached — a shared computer, someone else's phone,
            a link from a text message. For a reliance claim this line carries
            more weight than the agreement does, because it does not depend on
            the reader having accepted anything. It used to sit above the
            election list, where it read as an interruption; the footer keeps
            it on every page instead of only the ballot, and beside the
            Disclaimer link it already points at. Non-blocking by design. */}
        <p className="mb-3 px-4">{VERIFY_WITH_OFFICIALS_NOTE}</p>
        <nav aria-label="Legal" className="flex flex-wrap justify-center gap-x-5 gap-y-2">
          <Link to="/terms" className="underline hover:text-ink">
            Terms of Use
          </Link>
          <Link to="/privacy" className="underline hover:text-ink">
            Privacy Policy
          </Link>
          <Link to="/disclaimer" className="underline hover:text-ink">
            Disclaimer
          </Link>
        </nav>
      </footer>
    </div>
  );
}

export function ErrorBoundary() {
  return <RouteError />;
}

export default App;
