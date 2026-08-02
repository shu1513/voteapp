import type { ReactNode } from "react";

/**
 * Quiet toolbar trigger for the ballot pages' inline disclosure panels
 * ("Filters", "How to vote"). Styled to sit level with the "Sort by" select —
 * same border, radius, height, and type — so the controls row reads as one
 * toolbar; the chevron carries the open/closed affordance instead of a pill
 * that looked like a primary action. Shared so the two triggers can't drift.
 */
export function DisclosureTrigger({
  open,
  panelId,
  onClick,
  children,
}: {
  open: boolean;
  panelId: string;
  onClick: () => void;
  children: ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-expanded={open}
      aria-controls={panelId}
      className={`flex items-center gap-1.5 rounded-md border bg-white px-2 py-1.5 text-sm text-ink transition hover:bg-surface ${
        open ? "border-ink" : "border-line"
      }`}
    >
      {children}
      <svg
        aria-hidden="true"
        viewBox="0 0 12 12"
        className={`h-3 w-3 text-ink-soft transition-transform ${open ? "rotate-180" : ""}`}
      >
        <path d="M2.5 4.5 6 8l3.5-3.5" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    </button>
  );
}
