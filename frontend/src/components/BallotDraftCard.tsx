import { Link } from "react-router";
import { draftProgress, useBallotDraft } from "../lib/ballotDraft";

/**
 * Guest-only progress card at the top of the anonymous ballot: the shopping-
 * cart read of the ballot draft ("4 of 13 races decided" plus a bar), turning
 * into "My Election Picks ✓" when every race on the nearest election day has
 * a pick. Renders nothing until the draft has a target (set by BallotPage on
 * each successful load). The signup CTA appears once at least one pick
 * exists — the draft itself is the conversion pitch, so the copy names what
 * signing up preserves.
 */
export function BallotDraftCard({ registerNext }: { registerNext: string }) {
  const draft = useBallotDraft();
  const progress = draftProgress(draft);
  if (!progress) {
    return null;
  }
  const { picked, total, complete } = progress;
  const countLine = `${picked} of ${total} race${total === 1 ? "" : "s"} decided`;
  return (
    <section className="mt-4 rounded-xl border border-line bg-white p-4">
      <div className="flex flex-wrap items-baseline justify-between gap-x-3 gap-y-1">
        <h2 className="text-lg font-semibold text-ink">
          {complete ? (
            <>
              My Election Picks{" "}
              <span aria-hidden="true" className="text-green-700">
                ✓
              </span>
            </>
          ) : (
            "My Ballot Draft"
          )}
        </h2>
        <span className={`text-sm ${complete ? "font-medium text-green-900" : "text-ink-soft"}`}>
          {countLine}
        </span>
      </div>
      <div
        role="progressbar"
        aria-label="Ballot draft progress"
        aria-valuemin={0}
        aria-valuemax={total}
        aria-valuenow={picked}
        aria-valuetext={countLine}
        className="mt-2 h-2 overflow-hidden rounded-full bg-surface"
      >
        <div
          className={`h-full rounded-full transition-all ${complete ? "bg-green-700" : "bg-rausch"}`}
          style={{ width: `${total === 0 ? 0 : Math.round((picked / total) * 100)}%` }}
        />
      </div>
      {picked === 0 ? (
        <p className="mt-2 text-sm text-ink-soft">
          Tap &ldquo;Make my pick&rdquo; on each race below to build your ballot — no account needed.
        </p>
      ) : (
        <p className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-sm">
          <Link
            to={`/register?next=${encodeURIComponent(registerNext)}`}
            className="rounded-lg bg-rausch px-3 py-1.5 font-semibold text-white transition hover:bg-rausch-dark"
          >
            Sign up free to save your picks
          </Link>
          <span className="text-xs text-ink-soft">
            {complete
              ? `All ${total} races decided — your draft lives only on this device until you sign up.`
              : "Your draft lives only on this device until you sign up."}
          </span>
        </p>
      )}
    </section>
  );
}
