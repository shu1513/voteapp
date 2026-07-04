import { Link } from "react-router-dom";
import { AI_BANNER } from "../legal/copy";

/** Rendered at the top of every ballot, election, and candidate view. */
export function AiBanner() {
  return (
    <p className="mb-4 rounded border border-gray-300 bg-gray-50 px-3 py-2 text-xs text-gray-600">
      {AI_BANNER}{" "}
      <Link to="/disclaimer" className="underline">
        Learn more
      </Link>
    </p>
  );
}
