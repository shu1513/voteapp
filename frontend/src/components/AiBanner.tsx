import { Link } from "react-router-dom";
import { AI_BANNER } from "../legal/copy";

/** Rendered at the top of every ballot, election, and candidate view. */
export function AiBanner() {
  return (
    <p className="mb-4 rounded border border-line bg-surface px-3 py-2 text-xs text-ink-soft">
      {AI_BANNER}{" "}
      <Link to="/disclaimer" className="underline hover:text-ink">
        Learn more
      </Link>
    </p>
  );
}
