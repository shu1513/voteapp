import { useEffect, useRef } from "react";
import { Link } from "react-router";
import type { BackTo } from "../lib/detailNavContext";

/** One rail row: the sibling's detail path plus the label the list showed. */
export type RailEntry = { id: string; label: string; path: string };

/**
 * The desktop master–detail rail: the sibling list the visitor arrived with,
 * rendered beside the detail content so walking the list never loses their
 * place. Same data contract as DetailPager (which stays for narrow screens,
 * where this rail is display: none) — the page builds `entries` from its
 * validated nav state and the rail renders it verbatim, no fetching.
 *
 * The back link doubles as the "leave split screen" control: it returns to
 * the full-width list page, delivering `backToState` exactly as the pager's
 * back slot does. Sibling links pass `siblingState` verbatim — the page's own
 * incoming nav state — so a walk down the rail keeps the whole context chain.
 *
 * The current entry is text, not a link (aria-current on its row), and is
 * scrolled into view on arrival: ballots run to 40+ contests, longer than
 * the rail's own scroll viewport.
 */
export function DetailRail({
  ariaLabel,
  entries,
  currentId,
  backTo,
  backToState,
  siblingState,
}: {
  ariaLabel: string;
  entries: RailEntry[];
  currentId: string;
  backTo: BackTo;
  backToState?: unknown;
  siblingState?: unknown;
}) {
  const currentRef = useRef<HTMLLIElement | null>(null);
  useEffect(() => {
    // block: "nearest" scrolls only what's needed — the rail's own scroll
    // container when the row is off-screen, nothing when it's visible.
    // Guarded because jsdom elements have no scrollIntoView.
    if (typeof currentRef.current?.scrollIntoView === "function") {
      currentRef.current.scrollIntoView({ block: "nearest" });
    }
  }, [currentId]);
  // hidden lg:block — narrow screens keep the pager bar instead. self-start
  // keeps the grid from stretching the nav to the content's height, which
  // would leave sticky nothing to do; max-h + overflow give long ballots
  // their own scrollbar. truncate + title on every row: contest titles run
  // legal-length, and the rail must stay a rail.
  return (
    <nav
      aria-label={ariaLabel}
      className="sticky top-4 hidden max-h-[calc(100vh-2rem)] min-w-0 self-start overflow-y-auto lg:block"
    >
      <Link
        to={backTo.path}
        state={backToState}
        aria-label={`Back to ${backTo.label}`}
        title={backTo.label}
        className="block truncate px-3 text-sm font-medium text-ink transition hover:text-rausch"
      >
        <span aria-hidden="true">← </span>
        {backTo.label}
      </Link>
      <ul className="mt-3 space-y-1 border-t border-line pt-3">
        {entries.map((entry) =>
          entry.id === currentId ? (
            <li
              key={entry.id}
              ref={currentRef}
              aria-current="page"
              title={entry.label}
              className="truncate rounded-lg bg-surface px-3 py-1.5 text-sm font-medium text-ink"
            >
              {entry.label}
            </li>
          ) : (
            <li key={entry.id}>
              <Link
                to={entry.path}
                state={siblingState}
                title={entry.label}
                className="block truncate rounded-lg px-3 py-1.5 text-sm text-ink-soft transition hover:bg-surface hover:text-ink"
              >
                {entry.label}
              </Link>
            </li>
          )
        )}
      </ul>
    </nav>
  );
}
