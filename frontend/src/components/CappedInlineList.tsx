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
  label: string;
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
      <span className="font-medium text-ink-soft">{label}</span>{" "}
      {visible.map((item, index) => (
        <Fragment key={item.key}>
          {item.node}
          {index < visible.length - 1 || showToggle ? ", " : null}
        </Fragment>
      ))}
      {showToggle ? (
        // relative z-10 lifts the toggle above a card's stretched link so the
        // click expands the row instead of navigating.
        <button
          type="button"
          onClick={() => setExpanded((value) => !value)}
          aria-expanded={expanded}
          className="relative z-10 font-medium text-ink-soft underline decoration-dotted underline-offset-2 hover:text-ink"
        >
          {expanded ? "Show less" : `+${hiddenCount} more ${noun}`}
        </button>
      ) : null}
    </p>
  );
}
