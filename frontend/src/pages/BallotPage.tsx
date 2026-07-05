import { Link, useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { apiRequest } from "../api/client";
import { BALLOT_SORT_DESCRIPTIONS, PUBLIC_BALLOT_SORTS, type BallotSort, type BallotSummary } from "../api/types";
import { AiBanner } from "../components/AiBanner";
import { ElectionCard } from "../components/ElectionCard";
import { EmptyNotice, ErrorNotice, LoadingNotice } from "../components/Status";

// Public page: only the sorts the anonymous endpoint can honor. A my_areas
// value (typed into the URL or copied from a signed-in session) falls back to
// vote_power so the subtitle never claims an ordering the backend cannot do.
const SORT_VALUES: readonly string[] = PUBLIC_BALLOT_SORTS.map((option) => option.value);

export function BallotPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const districtIds = (searchParams.get("d") ?? "")
    .split(",")
    .map((id) => id.trim())
    .filter((id) => id.length > 0);
  const rawSort = searchParams.get("sort") ?? "";
  const sort: BallotSort = SORT_VALUES.includes(rawSort) ? (rawSort as BallotSort) : "vote_power";

  const ballot = useQuery({
    queryKey: ["ballot", districtIds.join(","), sort],
    queryFn: () =>
      apiRequest<BallotSummary>(
        `/api/ballot?district_ids=${encodeURIComponent(districtIds.join(","))}&sort=${sort}`
      ),
    enabled: districtIds.length > 0,
  });

  function onSortChange(nextSort: string) {
    setSearchParams(
      (previous) => {
        const next = new URLSearchParams(previous);
        next.set("sort", nextSort);
        return next;
      },
      { replace: true }
    );
  }

  if (districtIds.length === 0) {
    return (
      <div className="mx-auto max-w-3xl px-4 py-10">
        <EmptyNotice text="No districts selected." />
        <p className="text-center">
          <Link to="/" className="text-ink underline hover:text-rausch">
            Start with your address
          </Link>
        </p>
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <AiBanner />
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-2xl font-bold">Your ballot</h1>
        <label className="flex items-center gap-2 text-sm text-ink-soft">
          Sort by
          <select
            value={sort}
            onChange={(event) => onSortChange(event.target.value)}
            className="rounded-md border border-line bg-white px-2 py-1.5 text-sm text-ink focus:border-ink focus:outline-none"
          >
            {PUBLIC_BALLOT_SORTS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </label>
      </div>

      {ballot.isPending ? <LoadingNotice text="Loading your elections…" /> : null}
      {ballot.isError ? (
        <div className="mt-4">
          <ErrorNotice error={ballot.error} />
        </div>
      ) : null}

      {ballot.isSuccess ? (
        <>
          <p className="mt-1 text-sm text-ink-soft">
            {ballot.data.elections.length} election{ballot.data.elections.length === 1 ? "" : "s"} across{" "}
            {ballot.data.districts.length} district{ballot.data.districts.length === 1 ? "" : "s"},{" "}
            {BALLOT_SORT_DESCRIPTIONS[sort]}
          </p>
          <details className="mt-2 text-xs text-ink-soft">
            <summary className="cursor-pointer select-none underline">What do these labels mean?</summary>
            <div className="mt-2 space-y-2 rounded-lg border border-line bg-surface p-3">
              <p>
                <strong className="text-ink">Vote power</strong> estimates how much weight one vote carries in
                an election, based on district population and how decisive the contest is expected to be. It is
                an estimate for comparing elections — it does not measure the value, importance, or likely
                effect of your individual vote.
              </p>
              <p>
                <strong className="text-ink">Competitiveness</strong> labels reflect the margin of past results
                for the same contest and may be outdated after redistricting.
              </p>
              <p>
                Details and limitations:{" "}
                <Link to="/disclaimer" className="underline">
                  Disclaimer
                </Link>
                , section 8.
              </p>
            </div>
          </details>
          {ballot.data.elections.length === 0 ? (
            <EmptyNotice text="No upcoming elections found for these districts yet. Check back — new elections are added as they are announced." />
          ) : (
            <div className="mt-4 space-y-3">
              {ballot.data.elections.map((election) => (
                <ElectionCard key={election.id} election={election} />
              ))}
            </div>
          )}
        </>
      ) : null}
    </div>
  );
}
