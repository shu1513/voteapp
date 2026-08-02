import { Link } from "react-router";
import type { BackTo } from "../lib/detailNavContext";

type PagerLink = { path: string; label: string };

/**
 * The prev / up-level / next pager at the bottom of a detail page. Pages
 * render it only when a validated sibling list contains the current page
 * (pagerNeighbors) — a sequence end leaves that slot empty rather than
 * showing a dead control. Arrows are visual only; screen readers get
 * "Previous/Next: {label}" (same aria-label pattern as BackLink, since
 * adjacent-node text runs together in accessible-name computation).
 *
 * Layout: ballot titles run long, so narrow screens get prev|next on one
 * row with the full-width up-level link below; sm+ gets three columns.
 */
export function DetailPager({
  ariaLabel,
  prev,
  next,
  backTo,
  backToState,
  siblingState,
}: {
  ariaLabel: string;
  prev: PagerLink | null;
  next: PagerLink | null;
  backTo: BackTo;
  /** Delivered by the up-level link — e.g. the candidate context an
   * election's back destination restores (navState.backState). */
  backToState?: unknown;
  /** Delivered by prev/next links, verbatim — the page's own incoming nav
   * state, so the walk continues from every stop. */
  siblingState?: unknown;
}) {
  const linkClass = "font-medium text-ink-soft transition hover:text-ink";
  return (
    <nav
      aria-label={ariaLabel}
      className="mt-8 grid grid-cols-2 items-center gap-x-3 gap-y-2 border-t border-line pt-4 text-sm sm:grid-cols-3"
    >
      <p className="min-w-0">
        {prev ? (
          <Link to={prev.path} state={siblingState} aria-label={`Previous: ${prev.label}`} className={linkClass}>
            <span aria-hidden="true">← </span>
            {prev.label}
          </Link>
        ) : null}
      </p>
      {/* order-last drops the up-level link to its own full-width second row
          on narrow screens; sm+ restores the natural middle position. */}
      <p className="order-last col-span-2 min-w-0 text-center sm:order-none sm:col-span-1">
        <Link to={backTo.path} state={backToState} aria-label={`Back to ${backTo.label}`} className={linkClass}>
          {backTo.label}
        </Link>
      </p>
      <p className="min-w-0 text-right">
        {next ? (
          <Link to={next.path} state={siblingState} aria-label={`Next: ${next.label}`} className={linkClass}>
            {next.label}
            <span aria-hidden="true"> →</span>
          </Link>
        ) : null}
      </p>
    </nav>
  );
}
