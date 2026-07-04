import { Link, useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { apiRequest } from "../api/client";
import type { BallotSummary, ElectionSummary } from "../api/types";
import { AiBanner } from "../components/AiBanner";
import { EmptyNotice, ErrorNotice, LoadingNotice } from "../components/Status";
import { formatDistrictType, formatElectionDate, formatVotePowerLabel } from "../lib/format";

function ElectionCard({ election }: { election: ElectionSummary }) {
  return (
    <Link
      to={`/elections/${election.id}`}
      className="block rounded-lg border border-line bg-white p-4 shadow-sm transition hover:border-rausch"
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
          <span className="rounded bg-surface px-2 py-0.5 text-ink">Results available</span>
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
  const [searchParams] = useSearchParams();
  const districtIds = (searchParams.get("d") ?? "")
    .split(",")
    .map((id) => id.trim())
    .filter((id) => id.length > 0);

  const ballot = useQuery({
    queryKey: ["ballot", districtIds.join(",")],
    queryFn: () =>
      apiRequest<BallotSummary>(`/api/ballot?district_ids=${encodeURIComponent(districtIds.join(","))}`),
    enabled: districtIds.length > 0,
  });

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
      <h1 className="text-2xl font-bold">Your ballot</h1>

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
            {ballot.data.districts.length} district{ballot.data.districts.length === 1 ? "" : "s"}, ordered by
            where your vote carries the most weight.
          </p>
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
