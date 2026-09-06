import { Link } from "react-router";
import type { BackTo } from "../lib/detailNavContext";
import { track } from "../lib/usage";

type PagerLink = { path: string; label: string };

/**
 * The navigation bar at the TOP of a detail page — it replaced the separate
 * back link plus bottom pager, which read as two unrelated controls. Three
 * slots, each a small inline caption before its link: "Prev:" | "Back to:" |
 * "Next:". The captions explain the slots so a reader lost mid-ballot knows
 * exactly what each label is (the labels alone — a race title next to a
 * list name — read as three unexplained links). Captions sit on the SAME
 * line as their link: a caption-over-link stack doubled the bar's height,
 * and on phones the sparse two-row grid it produced read as broken
 * whitespace (an empty Prev cell left a blank half-row above the back
 * link).
 *
 * The up-level slot always renders (it carries the deep-link fallback);
 * prev/next render only when a validated sibling sequence contains the
 * current page. With no siblings at all the bar collapses to a single
 * left-aligned "← label" link, the same shape as the split-detail rail's
 * exit link: the "Back to:" caption earns its place only when Prev and
 * Next flank it, and one centered link floated alone in an empty strip.
 * Arrows are visual only; screen readers get
 * "Previous/Next: {label}" and "Back to {label}" (aria-label, since
 * adjacent-node text runs together in accessible-name computation).
 *
 * -mt-4 pulls the bar up into the page container's py-8: as a breadcrumb
 * strip it should hug the site header, not float a full text-block gap
 * below it (pages without the bar keep the untouched py-8).
 *
 * Layout: narrow screens stack the centered back slot over a single
 * prev|next flex row (a missing sibling collapses to nothing instead of
 * reserving a grid cell, and the survivor keeps its natural edge); sm+
 * turns the same markup into three columns via sm:contents.
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
    <p className="min-w-0 truncate text-center">
      <span className={captionClass}>Back to: </span>
      <Link
        to={backTo.path}
        state={backToState}
        aria-label={`Back to ${backTo.label}`}
        title={backTo.label}
        onClick={() => track("detail_control", { control: "pager_back", value: "none" })}
        className={linkClass}
      >
        {backTo.label}
      </Link>
    </p>
  );
  if (prev === null && next === null) {
    // No sequence to walk (deep link, single-entry list, the draft page):
    // one arrowed link at the left edge, where a back link is expected.
    return (
      <nav aria-label={ariaLabel} className="-mt-4 mb-6 border-b border-line pb-3 text-sm">
        <p className="min-w-0 truncate">
          <Link
            to={backTo.path}
            state={backToState}
            aria-label={`Back to ${backTo.label}`}
            title={backTo.label}
            onClick={() => track("detail_control", { control: "pager_back", value: "none" })}
            className={linkClass}
          >
            <span aria-hidden="true">← </span>
            {backTo.label}
          </Link>
        </p>
      </nav>
    );
  }
  return (
    <nav
      aria-label={ariaLabel}
      className="-mt-4 mb-6 border-b border-line pb-3 text-sm sm:grid sm:grid-cols-3 sm:items-start sm:gap-x-3"
    >
      {/* Back first on narrow screens (it matches "where you came from"
          reading order and stops Next floating alone above it); sm:order-2
          restores the middle column. Deliberate trade-off: the two
          breakpoints have different visual orders, so one DOM order can't
          match both — DOM follows the mobile layout (this bar's main
          audience; lg+ swaps in the rail), leaving sm-to-lg tab order
          Back -> Prev -> Next. Three links, meaning preserved. */}
      <div className="mb-1 min-w-0 sm:order-2 sm:mb-0">{backSlot}</div>
      {/* One flex row for the siblings on narrow screens; sm:contents
          promotes the two cells into the grid so the same markup serves
          both layouts. */}
      <div className="flex items-start justify-between gap-x-4 sm:contents">
        <p className="min-w-0 max-w-[50%] sm:order-1 sm:max-w-none">
          {prev ? (
            <Link
              to={prev.path}
              state={siblingState}
              aria-label={`Previous: ${prev.label}`}
              title={prev.label}
              onClick={() => track("detail_control", { control: "pager_prev", value: "none" })}
              className={`line-clamp-2 ${linkClass}`}
            >
              <span aria-hidden="true">← </span>
              <span className={captionClass}>Prev: </span>
              {prev.label}
            </Link>
          ) : null}
        </p>
        <p className="ml-auto min-w-0 max-w-[50%] text-right sm:order-3 sm:ml-0 sm:max-w-none">
          {next ? (
            <Link
              to={next.path}
              state={siblingState}
              aria-label={`Next: ${next.label}`}
              title={next.label}
              onClick={() => track("detail_control", { control: "pager_next", value: "none" })}
              className={`line-clamp-2 ${linkClass}`}
            >
              <span className={captionClass}>Next: </span>
              {next.label}
              <span aria-hidden="true"> →</span>
            </Link>
          ) : null}
        </p>
      </div>
    </nav>
  );
}
