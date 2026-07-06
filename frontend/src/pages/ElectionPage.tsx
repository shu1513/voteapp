import { useState } from "react";
import { Link, useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { apiRequest } from "../api/client";
import type { ElectionDetail } from "../api/types";
import { AiBanner } from "../components/AiBanner";
import { ErrorNotice, LoadingNotice } from "../components/Status";
import { SourceLine } from "../components/SourceLine";
import { FollowButton } from "../components/FollowButton";
import { ReportContentButton } from "../components/ReportContentButton";
import { formatDistrictType, formatElectionDate, formatMoney, formatOutcome, formatVotePowerLabel } from "../lib/format";
import { useFollows } from "../lib/useFollows";
import { useMe } from "../lib/useMe";
import { useMyResearchAreas } from "../lib/useMyResearchAreas";
import { aggregateRecordAreaStances, scoreStanceDirection } from "../lib/researchAreaScoring";
import { useDocumentTitle } from "../lib/useDocumentTitle";
import { useJsonLd } from "../lib/useJsonLd";

type CandidateSort = "ballot" | "for_mine" | "against_mine";

export function ElectionPage() {
  const { electionId } = useParams();
  const { me } = useMe();
  // Election payload candidates carry no follow state; derive it from the
  // follows list (only fetched for verified users).
  const { follows, canFollow } = useFollows();
  const followedIds = new Set((follows ?? []).map((follow) => follow.candidate_id));
  const { savedAreaIds, weights, hasSaved } = useMyResearchAreas();
  const [candidateSort, setCandidateSort] = useState<CandidateSort>("ballot");

  const election = useQuery({
    queryKey: ["election", electionId],
    queryFn: () => apiRequest<ElectionDetail>(`/api/elections/${electionId}`),
    enabled: Boolean(electionId),
  });
  useDocumentTitle(
    election.data?.official_ballot_title,
    election.data
      ? `${election.data.official_ballot_title} — ${election.data.district.name} election on ${election.data.election_date}: candidates, campaign finance, and issue research.`
      : undefined
  );
  useJsonLd(
    election.data
      ? {
          "@type": "Event",
          name: election.data.official_ballot_title,
          startDate: election.data.election_date,
          location: { "@type": "AdministrativeArea", name: election.data.district.name },
        }
      : undefined
  );

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
      <div className="mt-2">
        <ReportContentButton
          entityType="election"
          entityId={data.id}
          contextLabel="election"
          reporterEmail={me?.email}
        />
      </div>
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
          {measure.research_area_tags.length > 0 ? (
            <div className="mt-2 flex flex-wrap gap-2 text-xs">
              {measure.research_area_tags.map((tag) => (
                <span
                  key={tag.research_area_id}
                  className={
                    savedAreaIds.has(tag.research_area_id)
                      ? "rounded border border-rausch/40 bg-rausch/10 px-2 py-0.5 font-medium text-rausch-dark"
                      : "rounded bg-surface px-2 py-0.5 text-ink-soft"
                  }
                >
                  {tag.name}
                </span>
              ))}
            </div>
          ) : null}
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
          <div className="mt-3">
            <ReportContentButton
              entityType="ballot_measure"
              entityId={measure.id}
              contextLabel="ballot measure"
              reporterEmail={me?.email}
            />
          </div>
        </section>
      ) : null}

      {data.candidates.length > 0 ? (
        <section className="mt-6">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <h2 className="text-lg font-semibold">Candidates</h2>
            {hasSaved && data.candidates.length > 1 ? (
              <label className="flex items-center gap-2 text-sm text-ink-soft">
                Sort by
                <select
                  value={candidateSort}
                  onChange={(event) => setCandidateSort(event.target.value as CandidateSort)}
                  className="rounded-md border border-line bg-white px-2 py-1.5 text-sm text-ink focus:border-ink focus:outline-none"
                >
                  <option value="ballot">Ballot order</option>
                  <option value="for_mine">For my issues first</option>
                  <option value="against_mine">Against my issues first</option>
                </select>
              </label>
            ) : null}
          </div>
          <div className="mt-3 space-y-3">
            {sortCandidatesByStance(data.candidates, candidateSort, weights).map(({ candidate, stances }) => (
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
                  <div className="flex shrink-0 items-center gap-3">
                    {candidate.finance_summary?.direct_campaign.total_raised != null ? (
                      <span className="text-sm text-ink-soft">
                        Raised {formatMoney(candidate.finance_summary.direct_campaign.total_raised)}
                      </span>
                    ) : null}
                    {canFollow ? (
                      <span
                        onClick={(event) => {
                          // The card is a Link; the follow toggle must not navigate.
                          event.preventDefault();
                          event.stopPropagation();
                        }}
                      >
                        <FollowButton
                          candidateId={candidate.candidate_id}
                          isFollowing={followedIds.has(candidate.candidate_id)}
                          size="sm"
                        />
                      </span>
                    ) : null}
                  </div>
                </div>
                {candidate.summary ? (
                  <p className="mt-2 line-clamp-3 text-sm text-ink">{candidate.summary}</p>
                ) : null}
                {stances.length > 0 ? (
                  <div className="mt-2 flex flex-wrap gap-2 text-xs">
                    {stances.map((stance) => (
                      <span
                        key={stance.research_area_id}
                        className={
                          savedAreaIds.has(stance.research_area_id)
                            ? "rounded border border-rausch/40 bg-rausch/10 px-2 py-0.5 font-medium text-rausch-dark"
                            : "rounded bg-surface px-2 py-0.5 text-ink-soft"
                        }
                      >
                        {stance.name} ·{" "}
                        {[
                          stance.for_count > 0 ? `${stance.for_count} for` : null,
                          stance.against_count > 0 ? `${stance.against_count} against` : null,
                        ]
                          .filter(Boolean)
                          .join(", ")}
                      </span>
                    ))}
                  </div>
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

export default ElectionPage;

// Client-side "for/against my issues" candidate ordering: weighted unique
// matched areas dominate, matching record volume breaks ties, and candidates
// that tie completely (including all zero-scores) keep their ballot order —
// the sort is stable over the payload's original sequence.
function sortCandidatesByStance(
  candidates: ElectionDetail["candidates"],
  sort: CandidateSort,
  weights: ReturnType<typeof useMyResearchAreas>["weights"]
): Array<{ candidate: ElectionDetail["candidates"][number]; stances: ReturnType<typeof aggregateRecordAreaStances> }> {
  const entries = candidates.map((candidate) => ({
    candidate,
    stances: aggregateRecordAreaStances(candidate.records),
  }));
  if (sort === "ballot") {
    return entries;
  }
  const direction = sort === "for_mine" ? ("for" as const) : ("against" as const);
  return entries
    .map((entry, index) => ({ entry, index, score: scoreStanceDirection(entry.stances, weights, direction) }))
    .sort(
      (a, b) =>
        b.score.score - a.score.score || b.score.recordCount - a.score.recordCount || a.index - b.index
    )
    .map(({ entry }) => entry);
}
