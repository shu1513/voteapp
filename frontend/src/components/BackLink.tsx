import { Link } from "react-router";
import type { BackTo } from "../lib/detailNavContext";

/**
 * The labeled back link at the top of a detail page. The arrow is visual
 * only; screen readers get "Back to {label}". `state` (optional) is
 * delivered to the destination — the candidate page uses it to restore the
 * election page's own ballot context on the back hop.
 */
export function BackLink({ backTo, state }: { backTo: BackTo; state?: unknown }) {
  return (
    <p className="mb-3 text-sm">
      <Link
        to={backTo.path}
        state={state}
        // aria-label, not an sr-only prefix span: adjacent-span text runs
        // together in accessible-name computation ("Back toAll elections").
        aria-label={`Back to ${backTo.label}`}
        className="font-medium text-ink-soft transition hover:text-ink"
      >
        <span aria-hidden="true">← </span>
        {backTo.label}
      </Link>
    </p>
  );
}
