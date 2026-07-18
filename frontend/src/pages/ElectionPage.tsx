import { useState } from "react";
import { isRouteErrorResponse, Link, useLoaderData, useRouteError } from "react-router";
import type { LoaderFunctionArgs, MetaFunction } from "react-router";
import type { ElectionDetail } from "@voteapp/api-client";
import { AiBanner } from "../components/AiBanner";
import { JsonLdScript } from "../components/JsonLdScript";
import { NotFoundNotice } from "../components/NotFoundNotice";
import { RouteError } from "../components/RouteError";
import { SourceLine } from "../components/SourceLine";
import { FinanceSummaryCard, hasFinanceContent } from "../components/FinanceSummaryCard";
import { ReportContentButton } from "../components/ReportContentButton";
import {
  formatDistrictType,
  formatElectionDate,
  formatMoney,
  formatOutcome,
  formatRosterStatus,
  formatVotePowerLabel,
} from "@voteapp/api-client";
import { loadFromApi } from "../lib/loadFromApi";
import { useMe } from "@voteapp/api-client";
import { useMyResearchAreas } from "@voteapp/api-client";
import { aggregateRecordAreaStances, scoreStanceDirection } from "@voteapp/api-client";

type CandidateSort = "ballot" | "for_mine" | "against_mine";

// Server loader: the election subject arrives in the document HTML so
// non-JS crawlers can read it. Anonymous by design — see loadFromApi.
export async function loader({ params, request }: LoaderFunctionArgs) {
  return loadFromApi<ElectionDetail>(`/api/elections/${params.electionId}`, request);
}

// Replaces useDocumentTitle here: a leaf meta export fully overrides the
// root's, so it must carry both title and description.
export const meta: MetaFunction<typeof loader> = ({ data, error }) => {
  if (!data) {
    // "Not found" only for real 404s; a 429/502/504 render must not tell
    // crawlers the page doesn't exist.
    const isNotFound = isRouteErrorResponse(error) && error.status === 404;
    return [{ title: isNotFound ? "Not found · VoteApp" : "Something went wrong · VoteApp" }];
  }
  return [
    { title: `${data.official_ballot_title} · VoteApp` },
    {
      name: "description",
      content: `${data.official_ballot_title} — ${data.district.name} election on ${data.election_date}: candidates, campaign finance, and issue research.`,
    },
  ];
};

export function ErrorBoundary() {
  const error = useRouteError();
  if (isRouteErrorResponse(error) && error.status === 404) {
    return <NotFoundNotice subject="Election" />;
  }
  return <RouteError />;
}

export function ElectionPage() {
  const { me } = useMe();
  const { savedAreaIds, weights, hasSaved } = useMyResearchAreas();
  const [candidateSort, setCandidateSort] = useState<CandidateSort>("ballot");

  const data = useLoaderData<typeof loader>();
  const measure = data.ballot_measure;

  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <JsonLdScript
        data={{
          "@type": "Event",
          name: data.official_ballot_title,
          startDate: data.election_date,
          location: { "@type": "AdministrativeArea", name: data.district.name },
        }}
      />
      <AiBanner />
      <h1 className="text-2xl font-bold">{data.official_ballot_title}</h1>
      <p className="mt-1 text-sm text-ink-soft">
        {formatElectionDate(data.election_date)} · {data.district.name} ·{" "}
        {formatDistrictType(data.district.district_type)}
        {data.election_stage ? <> · {data.election_stage}</> : null}
        {data.seats_to_fill != null && data.seats_to_fill > 1 ? <> · {data.seats_to_fill} seats</> : null}
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
      {data.vote_power.label !== "unknown" && data.vote_power.explanation ? (
        <details className="mt-2 text-sm">
          <summary className="cursor-pointer text-xs font-medium text-ink-soft underline decoration-dotted underline-offset-2 hover:text-ink">
            How do we calculate vote power?
          </summary>
          <div className="mt-2 rounded-xl border border-line bg-white p-4">
            <p className="text-ink">{data.vote_power.explanation.how}</p>
            {/* One row per graded measure, formula-style: title, grade, this
                election's actual numbers, then a one-line why. */}
            <div className="mt-3 space-y-2">
              {data.vote_power.explanation.parts.map((part) => (
                <div key={part.title} className="rounded-lg bg-surface p-3">
                  <p className="text-ink">
                    <span className="font-semibold">{part.title}:</span>{" "}
                    <span className="font-medium">{part.grade}</span>
                    {part.stat ? <span className="text-ink-soft"> · {part.stat}</span> : null}
                  </p>
                  <p className="mt-1 text-xs text-ink-soft">{part.detail}</p>
                  {part.formula ? (
                    <p className="mt-1 break-words font-mono text-[11px] leading-relaxed text-ink-soft">
                      {part.formula}
                    </p>
                  ) : null}
                </div>
              ))}
            </div>
            <p className="mt-3 font-medium text-ink">{data.vote_power.explanation.result}</p>
            {data.vote_power.explanation.caveat ? (
              <p className="mt-2 text-xs text-ink-soft">{data.vote_power.explanation.caveat}</p>
            ) : null}
          </div>
        </details>
      ) : null}

      {measure ? (
        <section className="mt-6 rounded-xl border border-line bg-white p-4">
          <h2 className="text-lg font-semibold text-dem-blue">Ballot Measure</h2>
          {measure.research_area_tags.length > 0 ? (
            <div className="mt-2 flex flex-wrap gap-2 text-xs">
              {measure.research_area_tags.map((tag) => (
                <span
                  key={tag.research_area_id}
                  className={
                    savedAreaIds.has(tag.research_area_id)
                      ? "rounded border border-green-600/40 bg-green-600/10 px-2 py-0.5 font-medium text-green-900"
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
          {measure.results.length > 0 ? (
            <div className="mt-3">
              <h3 className="text-sm font-semibold">Results</h3>
              {hasCertifiedRow(measure.results) ? null : (
                <p className="mt-1 text-xs text-ink-soft">
                  Unofficial until certified by the relevant election authority.
                </p>
              )}
              <ul className="mt-2 space-y-3">
                {measure.results.map((result) => (
                  <li key={result.id} className="text-sm">
                    <p className="text-ink">
                      <span className="font-medium">{formatOutcome(result.outcome)}</span>
                      {result.result_status ? (
                        <span className="text-ink-soft"> · {formatOutcome(result.result_status)}</span>
                      ) : null}
                    </p>
                    <SourceLine url={result.source_url} researchedDate={result.retrieved_at.slice(0, 10)} />
                  </li>
                ))}
              </ul>
            </div>
          ) : measure.result ? (
            // Legacy canonical outcome kept as a fallback for measures whose
            // result predates the per-pass results rows.
            <p className="mt-3 text-sm font-medium">
              Result: <span className={measure.result === "passed" ? "text-green-700" : "text-red-700"}>{measure.result}</span>
            </p>
          ) : null}
          {measure.official_measure_url ? (
            <p className="mt-3 text-sm">
              <a
                href={measure.official_measure_url}
                target="_blank"
                rel="noopener noreferrer"
                className="font-medium text-rausch-dark underline hover:text-rausch"
              >
                {isGovernmentUrl(measure.official_measure_url)
                  ? `Read the official measure text${isPdfUrl(measure.official_measure_url) ? " (PDF)" : ""}`
                  : "More about this measure"}
              </a>
            </p>
          ) : null}
          {[...new Set(measure.source_urls)]
            .filter((url) => url !== measure.official_measure_url)
            .map((url) => (
              <SourceLine key={url} url={url} />
            ))}
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
              // The finance <details> is interactive content and may not
              // nest inside an anchor. The whole-card click target survives
              // via a stretched link: the name Link's ::after overlays the
              // wrapper, and the interactive sibling sits above it (z-10) so
              // it receives its own clicks. Following happens on the
              // candidate profile page, not here.
              <div
                key={candidate.candidate_id}
                className="relative rounded-xl border border-line bg-white shadow-sm transition hover:shadow-md"
              >
                <div className="p-4">
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <h3 className="font-semibold">
                        <Link
                          to={`/candidates/${candidate.candidate_id}`}
                          className="after:absolute after:inset-0"
                        >
                          {candidate.display_name}
                        </Link>
                      </h3>
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
                  {stances.length > 0 ? (
                    <div className="mt-2 flex flex-wrap gap-2 text-xs">
                      {stances.map((stance) => (
                        <span
                          key={stance.research_area_id}
                          className={
                            savedAreaIds.has(stance.research_area_id)
                              ? "rounded border border-green-600/40 bg-green-600/10 px-2 py-0.5 font-medium text-green-900"
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
                </div>
                {hasFinanceContent(candidate.finance_summary) ? (
                  <details className="relative z-10 border-t border-line px-4 py-3">
                    {/* Every card repeats this toggle; the aria-label keeps
                        repeated disclosures distinguishable for screen-reader
                        users (an sr-only span would glue words together in
                        the computed accessible name). */}
                    <summary
                      className="cursor-pointer text-sm font-medium text-ink"
                      aria-label={`Campaign finance for ${candidate.display_name}`}
                    >
                      Campaign finance
                    </summary>
                    <div className="mt-2">
                      <FinanceSummaryCard summary={candidate.finance_summary} />
                    </div>
                  </details>
                ) : null}
              </div>
            ))}
          </div>
        </section>
      ) : data.candidate_roster_status ? (
        // Empty office roster: say WHY instead of hiding the section (roster
        // awaiting certification, profiles being prepared, or unavailable).
        <section className="mt-6">
          <h2 className="text-lg font-semibold">Candidates</h2>
          <p className="mt-3 rounded-xl border border-line bg-white p-4 text-sm text-ink-soft">
            {formatRosterStatus(data.candidate_roster_status).long}
          </p>
        </section>
      ) : null}

      {data.results.length > 0 ? (
        <section className="mt-6 rounded-xl border border-line bg-white p-4">
          <h2 className="text-lg font-semibold">Results</h2>
          {hasCertifiedRow(data.results) ? null : (
            <p className="mt-1 text-xs text-ink-soft">
              Unofficial until certified by the relevant election authority.
            </p>
          )}
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
          {/* Research passes can record the same source twice; showing the
              repeat reads as a rendering bug. */}
          {[...new Set(data.sources)].map((url) => (
            <SourceLine key={url} url={url} />
          ))}
        </section>
      ) : null}
    </div>
  );
}

export default ElectionPage;

// The blanket pre-certification notice contradicts a row already labeled
// "Certified"; show it only while everything listed is pre-certification.
function hasCertifiedRow(results: readonly { result_status: string }[]): boolean {
  return results.some((result) => result.result_status === "certified");
}

function isPdfUrl(url: string): boolean {
  return /\.pdf($|[?#])/i.test(url);
}

// "Official" is a claim, not a style: the pipeline intends
// official_measure_url to be an official full-text page, but real rows point
// at Wikipedia/Ballotpedia. Only .gov links get the official label; anything
// else keeps neutral wording. .us is deliberately excluded — it is an open
// registry (individuals and businesses register ordinary .us domains), so it
// is not evidence of government hosting.
function isGovernmentUrl(url: string): boolean {
  try {
    return new URL(url).hostname.toLowerCase().endsWith(".gov");
  } catch {
    return false;
  }
}

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
