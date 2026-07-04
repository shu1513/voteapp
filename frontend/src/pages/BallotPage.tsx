import { Link, useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { apiRequest } from "../api/client";
import { BALLOT_SORTS, type BallotSort, type BallotSummary, type ElectionSummary } from "../api/types";
import { AiBanner } from "../components/AiBanner";
import { EmptyNotice, ErrorNotice, LoadingNotice } from "../components/Status";
import { formatDistrictType, formatElectionDate, formatVotePowerLabel } from "../lib/format";

const SORT_VALUES: readonly string[] = BALLOT_SORTS.map((option) => option.value);

const SORT_DESCRIPTIONS: Record<BallotSort, string> = {
  vote_power: "ordered by where your vote carries the most weight.",
  soonest: "ordered by election date, soonest first.",
  district_size: "ordered by district population, biggest first.",
  district_size_smallest: "ordered by district population, smallest first.",
};

function ElectionCard({ election }: { election: ElectionSummary }) {
  return (
    <Link
      to={`/elections/${election.id}`}
      className="block rounded-xl border border-line bg-white p-4 shadow-sm transition hover:shadow-md"
    >
      <div className="flex items-start justify-between gap-3">
        <h3 className="font-semibold text-ink">{election.official_ballot_title}</h3>
        <span className="shrink-0 text-sm text-ink-soft">{formatElectionDate(election.election_date)}</span>
      </div>
      <p className="mt-1 text-sm text-ink-soft">
        {election.district.name} · {formatDistrictType(election.district.district_type)}
        {election.office ? <> · {election.office.canonical_name}</> : null}
      </p>
      <div className="mt-2 flex flex-wrap items-center gap-2 text-xs">
        {election.race_type === "ballot_measure" ? (
          <span className="rounded bg-ink/10 px-2 py-0.5 text-ink">Ballot measure</span>
        ) : (
          <span className="rounded bg-surface px-2 py-0.5 text-ink-soft">
            {election.candidate_count} candidate{election.candidate_count === 1 ? "" : "s"}
          </span>
        )}
        {election.vote_power.label !== "unknown" ? (
          <span className="rounded bg-rausch/10 px-2 py-0.5 text-rausch-dark">
            Vote power: {formatVotePowerLabel(election.vote_power.label)}
          </span>
        ) : null}
        {election.historical_competitiveness ? (
          <span className="rounded bg-surface px-2 py-0.5 text-ink-soft">
            {election.historical_competitiveness.display_label}
          </span>
        ) : null}
        {election.has_results ? (
          <span className="rounded bg-surface px-2 py-0.5 text-ink">
            {election.current_result_outcome
              ? `Result: ${election.current_result_outcome}`
              : "Results available"}
          </span>
        ) : null}
        {election.research_areas.map((area) => (
          <span key={area.id} className="rounded bg-surface px-2 py-0.5 text-ink-soft">
            {area.name}
          </span>
        ))}
      </div>
    </Link>
  );
}

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
            {BALLOT_SORTS.map((option) => (
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
            {SORT_DESCRIPTIONS[sort]}
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
