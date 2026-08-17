import { useEffect, useRef, type ReactNode } from "react";
import { Link } from "react-router";
import type { BackTo } from "../lib/detailNavContext";

/** One rail row: the sibling's detail path plus the label the list showed.
 * picked renders the green "decided" check before the label. */
export type RailEntry = { id: string; label: string; path: string; picked?: boolean };

// Filled green circle with a white check — the rail's "you decided this
// race" marker. The sr-only text in the row carries it for screen readers.
function PickedCheck() {
  return (
    <svg aria-hidden="true" viewBox="0 0 16 16" className="h-4 w-4 shrink-0 text-green-700">
      <circle cx="8" cy="8" r="8" fill="currentColor" />
      <path
        d="M4.5 8.5 7 10.5l4.5-5"
        fill="none"
        stroke="white"
        strokeWidth="1.8"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

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
  headerSlot,
}: {
  ariaLabel: string;
  entries: RailEntry[];
  currentId: string;
  backTo: BackTo;
  backToState?: unknown;
  siblingState?: unknown;
  /** Rendered between the back link and the list — the election rail's
   * race-type tabs live here. The rail stays presentation-only: whatever
   * the slot controls re-slices `entries` in the caller. */
  headerSlot?: ReactNode;
}) {
  const currentRef = useRef<HTMLLIElement | null>(null);
  useEffect(() => {
    // block: "nearest" scrolls only what's needed — the rail's own scroll
    // container when the row is off-screen, nothing when it's visible.
    // Guarded because jsdom elements have no scrollIntoView.
    //
    // Keyed on currentId ONLY, deliberately: a header-slot control (sort,
    // tab) reordering `entries` must NOT snap the scroll back to the
    // current row. The controls sit at the top of this same scroll
    // container — the reader is already looking at the top when they
    // engage one, and the top of the new order is what they asked to see
    // ("My issues first" = show me who ranks highest). A tab switch can
    // even remove the current row entirely. Arrivals and sibling walks
    // still center the current row: those change currentId.
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
      {/* One divider, right under the back link: everything below it — the
          header slot's label/controls and the rows — reads as one panel.
          Without a header slot the divider moves down to keep separating
          the back link from the rows. */}
      {headerSlot ? (
        <div className="mt-3 border-t border-line px-3 pt-3">{headerSlot}</div>
      ) : null}
      <ul className={`mt-3 space-y-1 ${headerSlot ? "" : "border-t border-line pt-3"}`}>
        {entries.map((entry) =>
          entry.id === currentId ? (
            <li
              key={entry.id}
              ref={currentRef}
              aria-current="page"
              title={entry.label}
              className="flex items-center gap-1.5 rounded-lg bg-surface px-3 py-1.5 text-sm font-medium text-ink"
            >
              {entry.picked ? <PickedCheck /> : null}
              <span className="truncate">
                {entry.label}
                {/* Suffix, not prefix: the label must stay the leading text
                    of the accessible name so rows read (and match queries)
                    by their race title first. */}
                {entry.picked ? <span className="sr-only"> (decided)</span> : null}
              </span>
            </li>
          ) : (
            <li key={entry.id}>
              <Link
                to={entry.path}
                state={siblingState}
                title={entry.label}
                className="flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-sm text-ink-soft transition hover:bg-surface hover:text-ink"
              >
                {entry.picked ? <PickedCheck /> : null}
                <span className="truncate">
                  {entry.label}
                  {entry.picked ? <span className="sr-only"> (decided)</span> : null}
                </span>
              </Link>
            </li>
          )
        )}
      </ul>
    </nav>
  );
}
