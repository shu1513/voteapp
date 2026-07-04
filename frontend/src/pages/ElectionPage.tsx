import { Link, useParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { apiRequest } from "../api/client";
import type { ElectionDetail } from "../api/types";
import { AiBanner } from "../components/AiBanner";
import { ErrorNotice, LoadingNotice } from "../components/Status";
import { SourceLine } from "../components/SourceLine";
import { formatDistrictType, formatElectionDate, formatMoney, formatOutcome, formatVotePowerLabel } from "../lib/format";

export function ElectionPage() {
  const { electionId } = useParams();

  const election = useQuery({
    queryKey: ["election", electionId],
    queryFn: () => apiRequest<ElectionDetail>(`/api/elections/${electionId}`),
    enabled: Boolean(electionId),
  });

  if (election.isPending) {
    return <LoadingNotice text="Loading election…" />;
  }
  if (election.isError) {
    return (
      <div className="mx-auto max-w-3xl px-4 py-8">
        <ErrorNotice error={election.error} />
      </div>
    );
  }

  const data = election.data;
  const measure = data.ballot_measure;

  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <AiBanner />
      <h1 className="text-2xl font-bold">{data.official_ballot_title}</h1>
      <p className="mt-1 text-sm text-ink-soft">
        {formatElectionDate(data.election_date)} · {data.district.name} ·{" "}
        {formatDistrictType(data.district.district_type)}
        {data.election_stage ? <> · {data.election_stage}</> : null}
      </p>
      <div className="mt-2 flex flex-wrap gap-2 text-xs">
        {data.vote_power.label !== "unknown" ? (
          <span className="rounded bg-rausch/10 px-2 py-0.5 text-rausch-dark">
            Vote power: {formatVotePowerLabel(data.vote_power.label)}
          </span>
        ) : null}
        {data.historical_competitiveness ? (
          <span className="rounded bg-surface px-2 py-0.5 text-ink-soft">
            {data.historical_competitiveness.display_label}
          </span>
        ) : null}
      </div>

      {measure ? (
        <section className="mt-6 rounded-xl border border-line bg-white p-4">
          <h2 className="text-lg font-semibold">Ballot measure</h2>
          {measure.summary ? <p className="mt-2 text-sm text-ink">{measure.summary}</p> : null}
          <div className="mt-3 grid gap-3 sm:grid-cols-2">
            <div className="rounded border border-green-200 bg-green-50 p-3">
              <h3 className="text-sm font-semibold text-green-900">A YES vote means</h3>
              <p className="mt-1 text-sm text-green-900">{measure.what_yes_means}</p>
            </div>
            <div className="rounded border border-red-200 bg-red-50 p-3">
              <h3 className="text-sm font-semibold text-red-900">A NO vote means</h3>
              <p className="mt-1 text-sm text-red-900">{measure.what_no_means}</p>
            </div>
          </div>
          {measure.result ? (
            <p className="mt-3 text-sm font-medium">
              Result: <span className={measure.result === "passed" ? "text-green-700" : "text-red-700"}>{measure.result}</span>
            </p>
          ) : null}
          {(measure.official_measure_url ? [measure.official_measure_url] : measure.source_urls.slice(0, 1)).map(
            (url) => (
              <SourceLine key={url} url={url} />
            )
          )}
        </section>
      ) : null}

      {data.candidates.length > 0 ? (
        <section className="mt-6">
          <h2 className="text-lg font-semibold">Candidates</h2>
          <div className="mt-3 space-y-3">
            {data.candidates.map((candidate) => (
              <Link
                key={candidate.candidate_id}
                to={`/candidates/${candidate.candidate_id}`}
                className="block rounded-xl border border-line bg-white p-4 shadow-sm transition hover:shadow-md"
              >
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <h3 className="font-semibold">{candidate.display_name}</h3>
                    <p className="text-sm text-ink-soft">
                      {candidate.party}
                      {candidate.is_incumbent ? " · Incumbent" : ""}
                      {candidate.status !== "active" ? ` · ${candidate.status}` : ""}
                    </p>
                  </div>
                  {candidate.finance_summary?.direct_campaign.total_raised != null ? (
                    <span className="shrink-0 text-sm text-ink-soft">
                      Raised {formatMoney(candidate.finance_summary.direct_campaign.total_raised)}
                    </span>
                  ) : null}
                </div>
                {candidate.summary ? (
                  <p className="mt-2 line-clamp-3 text-sm text-ink">{candidate.summary}</p>
                ) : null}
              </Link>
            ))}
          </div>
        </section>
      ) : null}

      {data.results.length > 0 ? (
        <section className="mt-6 rounded-xl border border-line bg-white p-4">
          <h2 className="text-lg font-semibold">Results</h2>
          <p className="mt-1 text-xs text-ink-soft">
            Unofficial until certified by the relevant election authority.
          </p>
          <ul className="mt-2 space-y-3">
            {data.results.map((result) => (
              <li key={result.id} className="text-sm">
                <p className="text-ink">
                  <span className="font-medium">{formatOutcome(result.outcome)}</span>
                  {result.result_status ? (
                    <span className="text-ink-soft"> · {formatOutcome(result.result_status)}</span>
                  ) : null}
                </p>
                {result.winners.length > 0 ? (
                  <p className="text-ink-soft">
                    Winner{result.winners.length === 1 ? "" : "s"}:{" "}
                    {result.winners
                      .map((winner) =>
                        winner.party ? `${winner.candidate_name ?? "Unknown"} (${winner.party})` : winner.candidate_name ?? "Unknown"
                      )
                      .join(", ")}
                  </p>
                ) : null}
                <SourceLine url={result.source_url} researchedDate={result.retrieved_at.slice(0, 10)} />
              </li>
            ))}
          </ul>
        </section>
      ) : null}

      {data.sources.length > 0 ? (
        <section className="mt-6">
          <h2 className="text-sm font-semibold text-ink">Election sources</h2>
          {data.sources.map((url) => (
            <SourceLine key={url} url={url} />
          ))}
        </section>
      ) : null}
    </div>
  );
}
