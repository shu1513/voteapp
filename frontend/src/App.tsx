import { useEffect, useRef } from "react";
import { Link, Outlet, ScrollRestoration, useLocation } from "react-router";
import { Menu, MenuButton, MenuItem, MenuItems } from "@headlessui/react";
import { ChatWidget } from "./components/chatbot/ChatWidget";
import { RouteError } from "./components/RouteError";
import { TermsRenewalGate } from "./components/TermsRenewalGate";
import { APP_NAME, VERIFY_WITH_OFFICIALS_NOTE, useMe } from "@voteapp/api-client";
import { useFlushBallotDraft } from "./lib/useFlushBallotDraft";
import { useGuestDraftNav, useMyPicksProgress } from "./lib/usePickProgress";

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
  // min-w-0 + truncate: first names run to 80 chars, and at 375px the row
  // has ~78px to spare — an unbounded greeting rewraps the logo onto two
  // lines. The greeting absorbs all the shrink and ellipsizes instead.
  return <span className="min-w-0 truncate text-sm font-semibold text-navy">Hi {me.first_name}</span>;
}

function AccountNav() {
  const { me } = useMe();
  // Header collection mechanic, both sides of the session: guests get a
  // "My Ballot Draft" link to /draft once they've seen a ballot or made a
  // pick (null — no link — before that, so the address-search landing stays
  // clean), signed-in users get the same counter on the My Picks link. Both
  // flip to a ✓ when every race on the nearest election day is decided. On
  // the SSR pass the guest link is absent (server snapshot is an empty
  // draft), so the cached anonymous document stays draft-free.
  const guestDraftNav = useGuestDraftNav();
  const picksProgress = useMyPicksProgress();

  // While /api/me is unresolved (SSR, or a cold-started API taking tens of
  // seconds), default to the logged-out links rather than an empty header —
  // an invisible Log in/Sign up costs signups, while a signed-in visitor
  // briefly seeing them is harmless and self-corrects when /api/me lands.
  if (!me) {
    return (
      <span className="flex items-center gap-4">
        {guestDraftNav ? (
          <Link
            to={guestDraftNav.to}
            className={
              guestDraftNav.complete
                ? "whitespace-nowrap font-semibold text-green-800 hover:text-green-900"
                : "whitespace-nowrap font-medium text-ink-soft hover:text-ink"
            }
          >
            {guestDraftNav.label}
          </Link>
        ) : null}
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

  // "My Draft" mirrors the guest label rules: plain until the first pick
  // (no homework-flavored "0/8", and no counter while the queries haven't
  // settled — a counter that flashes in later is fine, a wrong one is not),
  // then counting up, then the earned name "My Picks ✓" when every race on
  // the nearest election day is decided.
  const myDraftLabel =
    picksProgress && picksProgress.picked > 0
      ? picksProgress.complete
        ? "My Picks ✓"
        : `My Draft ${picksProgress.picked}/${picksProgress.total}`
      : "My Draft";

  // Log out lives in Settings (Sessions section), not the header — it kept a
  // rarely-used action in premium header space. "My Draft" sits last so the
  // working document is the item nearest the reader's thumb.
  // Inline items wrap into a broken two-line header on phones, so the
  // signed-in nav is inline links on sm+ and a menu below that breakpoint.
  return (
    <>
      <span className="hidden items-center gap-4 sm:flex">
        <Link to="/me/ballot" className="text-ink-soft hover:text-ink">
          My Elections
        </Link>
        <Link to="/me/follows" className="whitespace-nowrap text-ink-soft hover:text-ink">
          My Candidates
        </Link>
        <Link to="/me/settings" className="text-ink-soft hover:text-ink">
          Settings
        </Link>
        <Link to="/me/picks" className="whitespace-nowrap text-ink-soft hover:text-ink">
          {myDraftLabel}
        </Link>
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
            <Link to="/me/picks" className="block px-4 py-2 text-ink data-[focus]:bg-surface">
              {myDraftLabel}
            </Link>
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
  // Replays a guest ballot draft into the account on login/registration.
  useFlushBallotDraft();

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
        {/* flex-wrap: when logo + nav outgrow the row (guest nav with the
            draft link at phone widths) the nav drops to its own line instead
            of the shrink-0 logo painting over it. ml-auto keeps the wrapped
            nav right-aligned. */}
        <div className="mx-auto flex max-w-3xl flex-wrap items-center justify-between gap-x-4 gap-y-2 px-4 py-4">
          {/* min-w-0 here and shrink-0 on the logo let the greeting (not the
              two-word logo text) give way when the row runs out of room. */}
          <span className="flex min-w-0 items-baseline gap-3">
            <Link to="/" className="shrink-0 text-xl font-extrabold tracking-tight text-rausch">
              {APP_NAME}
            </Link>
            <Greeting />
          </span>
          {/* shrink-0: without it a long greeting squeezes the nav and wraps
              the Menu button's label onto two lines. */}
          <nav className="ml-auto flex shrink-0 items-center gap-4 text-sm">
            <AccountNav />
          </nav>
        </div>
      </header>
      <main id="main" ref={mainRef} tabIndex={-1} className="outline-none">
        <Outlet />
      </main>
      <TermsRenewalGate />
      {/* Flag-guarded chatbot widget (docs/plans/chatbot-rag.md): floating
          lower-right bubble on most pages; the component owns its own
          per-route visibility and auth-wall rules. */}
      {import.meta.env.VITE_CHATBOT_ENABLED === "true" && <ChatWidget />}
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
