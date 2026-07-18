import { useState } from "react";
import { isRouteErrorResponse, Link, useLoaderData, useRouteError } from "react-router";
import type { LoaderFunctionArgs, MetaFunction } from "react-router";
import { useQuery } from "@tanstack/react-query";
import type {
  CandidateDetail,
  CandidateElection,
  CandidateRecord,
  FinanceSummary,
  ResearchAreaPreference,
} from "@voteapp/api-client";
import { JsonLdScript } from "../components/JsonLdScript";
import { NotFoundNotice } from "../components/NotFoundNotice";
import { RouteError } from "../components/RouteError";
import { SourceLine } from "../components/SourceLine";
import { FollowButton } from "../components/FollowButton";
import { FinanceSummaryCard, hasFinanceContent } from "../components/FinanceSummaryCard";
import { ReportContentButton } from "../components/ReportContentButton";
import { apiRequest, formatElectionDate } from "@voteapp/api-client";
import { loadFromApi } from "../lib/loadFromApi";
import { useFollows } from "@voteapp/api-client";
import { useMe } from "@voteapp/api-client";
import { useMyResearchAreas } from "@voteapp/api-client";
import { UNRANKED_RESEARCH_AREA_RANK } from "@voteapp/api-client";

type RecordView = "by_issue" | "my_issues" | "newest";

// A researched incumbent can carry 50+ records; rendering everything open
// made the profile a 10,000px wall. Grouped views open the first few issue
// groups and collapse the rest behind per-group counts; the flat newest
// view cuts off with an explicit "show all".
const INITIAL_OPEN_GROUPS = 3;
const INITIAL_NEWEST_RECORDS = 20;

type RecordGroup = {
  /** null for the untagged "Other records" pseudo-group. */
  areaId: string | null;
  areaName: string;
  records: CandidateRecord[];
};

// Records grouped by research area (a record with several tags appears under
// each; untagged records fall into "Other records"). Groups key on the
// stable research_area_id — display names are presentation, not identity.
function groupRecords(records: CandidateRecord[]): RecordGroup[] {
  const groups = new Map<string | null, RecordGroup>();
  for (const record of records) {
    const areas = record.research_area_tags.length
      ? record.research_area_tags.map((tag) => ({ areaId: tag.research_area_id, areaName: tag.name }))
      : [{ areaId: null, areaName: "Other records" }];
    for (const area of areas) {
      const group = groups.get(area.areaId) ?? { ...area, records: [] };
      group.records.push(record);
      groups.set(area.areaId, group);
    }
  }
  return [...groups.values()].sort((a, b) =>
    a.areaId === null ? 1 : b.areaId === null ? -1 : a.areaName.localeCompare(b.areaName)
  );
}

// "My issues first": saved-area groups move to the front ordered by the
// user's rank (unranked saved areas after ranked ones), everything else
// keeps the alphabetical order groupRecords produced.
function orderGroupsByPreference(
  groups: RecordGroup[],
  preferences: readonly ResearchAreaPreference[]
): RecordGroup[] {
  const rankByAreaId = new Map(
    preferences.map((preference) => [preference.research_area_id, preference.rank ?? UNRANKED_RESEARCH_AREA_RANK])
  );
  return groups
    .map((group, index) => ({
      group,
      index,
      rank: (group.areaId !== null ? rankByAreaId.get(group.areaId) : undefined) ?? Number.POSITIVE_INFINITY,
    }))
    .sort((a, b) => a.rank - b.rank || a.index - b.index)
    .map(({ group }) => group);
}

// Server loader: the candidate subject arrives in the document HTML so
// non-JS crawlers can read it. Anonymous by design — see loadFromApi.
export async function loader({ params, request }: LoaderFunctionArgs) {
  return loadFromApi<CandidateDetail>(`/api/candidates/${params.candidateId}`, request);
}

// Election dates are YYYY-MM-DD calendar strings; "today" is the last US
// clock still on a given date — Pacific/Honolulu, UTC-10, no DST — mirroring
// the backend's US_LATEST_LOCAL_DATE_SQL (usLocalDate.ts): an election
// counts as past only once the entire United States has finished that day.
// Fixing the timezone also makes the value identical on the SSR host and in
// the viewer's browser, so ongoing/past classification cannot flip during
// hydration. en-CA formats as YYYY-MM-DD.
function usLatestLocalDate(): string {
  return new Intl.DateTimeFormat("en-CA", { timeZone: "Pacific/Honolulu" }).format(new Date());
}

// Election-specific finance stays off the candidate detail payload (see
// backend candidateDetailReader.ts) — the profile fetches this candidate's
// summary for one election from the narrow finance endpoint, instead of the
// full election detail with every opponent's records.
function useElectionFinance(electionId: string, candidateId: string, enabled: boolean) {
  const query = useQuery({
    queryKey: ["election-finance", electionId, candidateId],
    queryFn: () =>
      apiRequest<{ finance_summary: FinanceSummary | null }>(
        `/api/elections/${electionId}/candidates/${candidateId}/finance`
      ),
    enabled,
    staleTime: 60_000,
  });
  return { summary: query.data?.finance_summary ?? null, isPending: query.isPending, isError: query.isError };
}

// Eager finance for an election the candidate is currently in. Renders its
// own section so there is no orphan heading while the fetch is in flight or
// when the election has no finance coverage — a fetch failure also just
// leaves the profile without the section.
function OngoingElectionFinance({ election, candidateId }: { election: CandidateElection; candidateId: string }) {
  const { summary } = useElectionFinance(election.election_id, candidateId, true);
  if (!hasFinanceContent(summary)) {
    return null;
  }
  return (
    <section className="mt-6">
      {/* A candidate can be in two concurrent races, repeating this heading;
          the aria-label keeps heading navigation distinguishable (an sr-only
          span would glue words together in the computed accessible name). */}
      <h2 className="text-lg font-semibold" aria-label={`Campaign finance — ${election.official_ballot_title}`}>
        Campaign finance
      </h2>
      <p className="mt-1 text-sm text-ink-soft">
        {election.official_ballot_title} · {formatElectionDate(election.election_date)}
      </p>
      <div className="mt-2 rounded-xl border border-line bg-white p-4">
        <FinanceSummaryCard summary={summary} />
      </div>
    </section>
  );
}

// Lazy finance for a past election-history row: nothing is fetched until
// the user opens the disclosure (opening is the explicit ask, so unlike the
// ongoing section this one states it when there is nothing to show).
function PastElectionFinance({ election, candidateId }: { election: CandidateElection; candidateId: string }) {
  const [opened, setOpened] = useState(false);
  const { summary, isPending, isError } = useElectionFinance(election.election_id, candidateId, opened);
  return (
    <details
      className="mt-1"
      onToggle={(event) => {
        if (event.currentTarget.open) {
          setOpened(true);
        }
      }}
    >
      {/* Every past-election row repeats this toggle; the aria-label keeps
          them distinguishable for screen-reader users (an sr-only span would
          glue words together in the computed accessible name). */}
      <summary
        className="cursor-pointer text-xs text-ink-soft hover:text-ink"
        aria-label={`Campaign finance for ${election.official_ballot_title}, ${formatElectionDate(election.election_date)}`}
      >
        Campaign finance
      </summary>
      <div className="mt-2">
        {!opened || isPending ? (
          <p className="text-xs text-ink-soft">Loading…</p>
        ) : isError ? (
          <p className="text-xs text-ink-soft">Couldn’t load finance data for this election.</p>
        ) : hasFinanceContent(summary) ? (
          <FinanceSummaryCard summary={summary} />
        ) : (
          <p className="text-xs text-ink-soft">No finance data for this election.</p>
        )}
      </div>
    </details>
  );
}

// One record card, shared by the grouped and flat views (the flat view adds
// the area tags to the meta line since there is no group heading to carry
// them).
function RecordItem({
  record,
  showTags,
  reporterEmail,
}: {
  record: CandidateRecord;
  showTags: boolean;
  reporterEmail?: string | null;
}) {
  return (
    <li className="rounded-xl border border-line bg-white p-3">
      <p className="text-sm text-ink">{record.description}</p>
      <p className="mt-1 text-xs text-ink-soft">
        {formatElectionDate(record.event_date)}
        {showTags && record.research_area_tags.length > 0
          ? ` · ${record.research_area_tags.map((tag) => tag.name).join(", ")}`
          : ""}
      </p>
      <SourceLine url={record.source_url} researchedDate={record.created_at.slice(0, 10)} />
      <div className="mt-2">
        <ReportContentButton
          entityType="candidate_record"
          entityId={record.id}
          contextLabel="candidate record"
          reporterEmail={reporterEmail}
        />
      </div>
    </li>
  );
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
  const candidate = data.candidate;
  return [
    { title: `${candidate.display_name} · VoteApp` },
    {
      name: "description",
      content: `${candidate.display_name} (${candidate.party}, ${candidate.state}) — issue-tagged records with sources, election history, and campaign finance.`,
    },
  ];
};

export function ErrorBoundary() {
  const error = useRouteError();
  if (isRouteErrorResponse(error) && error.status === 404) {
    return <NotFoundNotice subject="Candidate" />;
  }
  return <RouteError />;
}

export function CandidatePage() {
  // The anonymous loader payload always carries is_following=false; derive
  // the real state from the follows list (only fetched for verified users),
  // same as ElectionPage. useSetFollow invalidates that list on toggle.
  // The button renders only once the list has loaded — before then a
  // followed candidate would briefly (or, on fetch failure, permanently)
  // show as unfollowed.
  const { follows, canFollow } = useFollows();
  const { me } = useMe();
  const { hasSaved, preferences } = useMyResearchAreas();
  const [recordView, setRecordView] = useState<RecordView>("by_issue");
  const [showAllNewest, setShowAllNewest] = useState(false);

  const detail = useLoaderData<typeof loader>();
  const candidate = detail.candidate;
  const isFollowing = (follows ?? []).some((follow) => follow.candidate_id === candidate.candidate_id);
  const baseGroups = groupRecords(candidate.records);
  const recordGroups =
    recordView === "my_issues" ? orderGroupsByPreference(baseGroups, preferences) : baseGroups;
  const today = usLatestLocalDate();
  const ongoingElections = candidate.elections.filter((election) => election.election_date >= today);

  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <JsonLdScript
        data={{
          "@type": "Person",
          name: candidate.display_name,
          ...(candidate.current_office ? { jobTitle: candidate.current_office } : {}),
          ...(candidate.official_website_url ? { url: candidate.official_website_url } : {}),
        }}
      />
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-2xl font-bold">{candidate.display_name}</h1>
        {canFollow && follows ? (
          <FollowButton candidateId={candidate.candidate_id} isFollowing={isFollowing} />
        ) : null}
      </div>
      <p className="mt-1 text-sm text-ink-soft">
        {candidate.party} · {candidate.state}
        {candidate.current_office ? <> · {candidate.current_office}</> : null}
      </p>
      {candidate.official_website_url ? (
        <p className="mt-1 text-sm">
          <a
            href={candidate.official_website_url}
            target="_blank"
            rel="noopener noreferrer"
            className="text-ink underline hover:text-rausch"
          >
            Official website
          </a>
        </p>
      ) : null}
      {candidate.summary ? <p className="mt-3 text-ink">{candidate.summary}</p> : null}
      <div className="mt-3">
        <ReportContentButton
          entityType="candidate"
          entityId={candidate.candidate_id}
          contextLabel="candidate profile"
          reporterEmail={me?.email}
        />
      </div>

      {ongoingElections.map((election) => (
        <OngoingElectionFinance
          key={election.candidate_election_id}
          election={election}
          candidateId={candidate.candidate_id}
        />
      ))}

      {recordGroups.length > 0 ? (
        <section className="mt-6">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <h2 className="text-lg font-semibold">Record</h2>
            <label className="flex items-center gap-2 text-sm text-ink-soft">
              View
              <select
                value={recordView}
                onChange={(event) => setRecordView(event.target.value as RecordView)}
                className="rounded-md border border-line bg-white px-2 py-1.5 text-sm text-ink focus:border-ink focus:outline-none"
              >
                <option value="by_issue">By issue</option>
                {hasSaved ? <option value="my_issues">My issues first</option> : null}
                <option value="newest">Newest first</option>
              </select>
            </label>
          </div>
          {recordView === "newest" ? (
            // Flat chronological view; the payload already arrives newest-first.
            <>
              <ul className="mt-2 space-y-3">
                {(showAllNewest ? candidate.records : candidate.records.slice(0, INITIAL_NEWEST_RECORDS)).map(
                  (record) => (
                    <RecordItem key={record.id} record={record} showTags reporterEmail={me?.email} />
                  )
                )}
              </ul>
              {!showAllNewest && candidate.records.length > INITIAL_NEWEST_RECORDS ? (
                <button
                  type="button"
                  onClick={() => setShowAllNewest(true)}
                  className="mt-3 rounded-lg border border-line bg-white px-3 py-1.5 text-sm font-medium text-ink transition hover:border-ink"
                >
                  Show all {candidate.records.length} records
                </button>
              ) : null}
            </>
          ) : (
            recordGroups.map((group, groupIndex) => (
              // The first few groups start open, the rest collapsed. `open`
              // only re-applies when a group's position changes (a view
              // switch reordering the groups), which just resets that
              // group's disclosure — user toggles otherwise stick.
              <details key={group.areaId ?? "other"} open={groupIndex < INITIAL_OPEN_GROUPS} className="mt-4">
                <summary className="cursor-pointer select-none">
                  <h3 className="inline text-sm font-semibold uppercase tracking-wide text-ink-soft">
                    {group.areaName}
                  </h3>{" "}
                  <span className="text-xs text-ink-soft">
                    · {group.records.length} record{group.records.length === 1 ? "" : "s"}
                  </span>
                </summary>
                <ul className="mt-2 space-y-3">
                  {group.records.map((record) => (
                    <RecordItem
                      key={`${group.areaId ?? "other"}-${record.id}`}
                      record={record}
                      showTags={false}
                      reporterEmail={me?.email}
                    />
                  ))}
                </ul>
              </details>
            ))
          )}
        </section>
      ) : (
        // An empty record list is ambiguous on its own: researched-and-none-
        // found and not-researched-yet must read differently or absence looks
        // like a completed (empty) record. "Verified", not "found": a search
        // can finish with every discovered record dropped for permanently
        // failing source checks, and the checkpoint still advances — the
        // array only proves nothing verifiable was kept.
        <p className="mt-6 text-sm text-ink-soft">
          {candidate.records_researched_through
            ? `No verified public records for this candidate — record history researched through ${formatElectionDate(candidate.records_researched_through)}.`
            : "This candidate's record history has not been researched yet."}
        </p>
      )}

      {candidate.elections.length > 0 ? (
        <section className="mt-6">
          <h2 className="text-lg font-semibold">Elections</h2>
          <ul className="mt-2 divide-y divide-line rounded-xl border border-line bg-white">
            {candidate.elections.map((election) => (
              <li key={election.candidate_election_id} className="px-3 py-2 text-sm">
                <Link to={`/elections/${election.election_id}`} className="text-ink underline hover:text-rausch">
                  {election.official_ballot_title}
                </Link>{" "}
                <span className="text-ink-soft">
                  · {formatElectionDate(election.election_date)} · {election.district.name}
                  {election.is_incumbent ? " · incumbent" : ""}
                </span>
                {election.election_date < today ? (
                  // Ongoing races already show finance eagerly above; past
                  // rows offer it on demand.
                  <PastElectionFinance election={election} candidateId={candidate.candidate_id} />
                ) : null}
              </li>
            ))}
          </ul>
        </section>
      ) : null}

      {candidate.last_researched ? (
        <p className="mt-6 text-xs text-ink-soft">
          Profile last researched {formatElectionDate(candidate.last_researched.slice(0, 10))}.
        </p>
      ) : null}
    </div>
  );
}

export default CandidatePage;
