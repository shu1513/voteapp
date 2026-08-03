import { Link } from "react-router";
import type { BackTo } from "../lib/detailNavContext";

type PagerLink = { path: string; label: string };

/**
 * The navigation bar at the TOP of a detail page — it replaced the separate
 * back link plus bottom pager, which read as two unrelated controls. Three
 * slots, each a small caption over its link: "Prev:" | "Go back to:" |
 * "Next:". The captions explain the slots so a reader lost mid-ballot knows
 * exactly what each label is (the labels alone — a race title next to a
 * list name — read as three unexplained links).
 *
 * The up-level slot always renders (it carries the deep-link fallback);
 * prev/next render only when a validated sibling sequence contains the
 * current page. With no siblings at all the bar collapses to the single
 * centered back slot. Arrows are visual only; screen readers get
 * "Previous/Next: {label}" and "Back to {label}" (aria-label, since
 * adjacent-node text runs together in accessible-name computation).
 *
 * Layout: ballot titles run long, so narrow screens put prev|next on one
 * row with the full-width up-level slot below; sm+ gets three columns.
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
  const captionClass = "text-xs text-ink-soft";
  const linkClass = "font-medium text-ink transition hover:text-rausch";
  // truncate / line-clamp: some labels are legal-length ("Elections in
  // Congressional District 1 (119th Congress), Alabama") — the bar must
  // stay a bar, not a paragraph. title= keeps the full text on hover; the
  // aria-label already carries it for screen readers.
  const backSlot = (
    <>
      <p className={captionClass}>Go back to:</p>
      <Link
        to={backTo.path}
        state={backToState}
        aria-label={`Back to ${backTo.label}`}
        title={backTo.label}
        className={`block truncate ${linkClass}`}
      >
        {backTo.label}
      </Link>
    </>
  );
  if (prev === null && next === null) {
    // No sequence to walk (deep link, single-entry list): just the back
    // slot, centered — a three-column grid would leave dead columns.
    return (
      <nav aria-label={ariaLabel} className="mb-6 border-b border-line pb-3 text-center text-sm">
        {backSlot}
      </nav>
    );
  }
  return (
    <nav
      aria-label={ariaLabel}
      className="mb-6 grid grid-cols-2 items-start gap-x-3 gap-y-2 border-b border-line pb-3 text-sm sm:grid-cols-3"
    >
      <div className="min-w-0">
        {prev ? (
          <>
            <p className={captionClass}>Prev:</p>
            <Link
              to={prev.path}
              state={siblingState}
              aria-label={`Previous: ${prev.label}`}
              title={prev.label}
              className={`line-clamp-2 ${linkClass}`}
            >
              <span aria-hidden="true">← </span>
              {prev.label}
            </Link>
          </>
        ) : null}
      </div>
      {/* order-last drops the back slot to its own full-width second row on
          narrow screens; sm+ restores the natural middle position. */}
      <div className="order-last col-span-2 min-w-0 text-center sm:order-none sm:col-span-1">{backSlot}</div>
      <div className="min-w-0 text-right">
        {next ? (
          <>
            <p className={captionClass}>Next:</p>
            <Link
              to={next.path}
              state={siblingState}
              aria-label={`Next: ${next.label}`}
              title={next.label}
              className={`line-clamp-2 ${linkClass}`}
            >
              {next.label}
              <span aria-hidden="true"> →</span>
            </Link>
          </>
        ) : null}
      </div>
    </nav>
  );
}
