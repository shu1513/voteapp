import { Fragment, useState, type ReactNode } from "react";

export const INLINE_LIST_CAP = 3;

/**
 * One labelled, comma-separated row that shows the first few items and folds
 * the rest behind an inline "+N more" toggle. Callers pass items already in
 * display order (personal matches first), so the cap always keeps the ones
 * that matter most to the reader. Separators are plain text nodes outside
 * the item markup so each item's own text stays exact.
 */
export function CappedInlineList({
  label,
  items,
  noun,
  cap = INLINE_LIST_CAP,
  className,
}: {
  /** Leading label ("Affects:"); omitted when a heading above carries it. */
  label?: string;
  items: { key: string; node: ReactNode }[];
  /** Plural noun for the overflow toggle ("issues" → "+4 more issues"). */
  noun: string;
  cap?: number;
  className?: string;
}) {
  const [expanded, setExpanded] = useState(false);
  const visible = expanded ? items : items.slice(0, cap);
  const hiddenCount = items.length - visible.length;
  const showToggle = items.length > cap;
  return (
    <p className={className}>
      {label ? (
        <>
          <span className="font-medium text-ink-soft">{label}</span>{" "}
        </>
      ) : null}
      {visible.map((item, index) => (
        <Fragment key={item.key}>
          {item.node}
          {index < visible.length - 1 || showToggle ? ", " : null}
        </Fragment>
      ))}
      {showToggle ? (
        // Link-styled (same navy underline as "See full profile →") with a
        // flipping chevron, so it reads as clickable next to the plain issue
        // names. relative z-10 lifts it above a card's stretched link so the
        // click expands the row instead of navigating; the padding widens the
        // tap target without moving the text.
        <button
          type="button"
          onClick={() => setExpanded((value) => !value)}
          aria-expanded={expanded}
          className="relative z-10 -my-1 inline-flex items-center gap-1 py-1 font-medium text-navy underline underline-offset-2 hover:text-rausch-deep"
        >
          {expanded ? "Show less" : `+${hiddenCount} more ${noun}`}
          <svg
            aria-hidden="true"
            viewBox="0 0 12 12"
            className={`h-3 w-3 transition-transform ${expanded ? "rotate-180" : ""}`}
          >
            <path d="M2.5 4.5 6 8l3.5-3.5" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        </button>
      ) : null}
    </p>
  );
}
