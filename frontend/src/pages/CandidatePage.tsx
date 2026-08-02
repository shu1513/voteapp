import { Fragment, useState } from "react";
import { isRouteErrorResponse, Link, useLoaderData, useRouteError } from "react-router";
import type { LoaderFunctionArgs, MetaFunction } from "react-router";
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
import { RegisterToFollowButton } from "../components/RegisterToFollowButton";
import { ShareButton } from "../components/ShareButton";
import { CandidatePickRow } from "../components/ElectionChoiceControls";
import { RegisterToPickRow } from "../components/RegisterToPickControls";
import { useElectionChoices } from "@voteapp/api-client";
import { FinanceSummaryCard, hasFinanceContent } from "../components/FinanceSummaryCard";
import { ReportContentButton } from "../components/ReportContentButton";
import { formatDistrictName, formatElectionDate } from "@voteapp/api-client";
import { loadFromApi } from "../lib/loadFromApi";
import { pageMeta } from "../lib/pageMeta";
import { usLatestLocalDate } from "../lib/usLatestLocalDate";
import { compareByResearchAreaPriority } from "../lib/researchAreaPriority";
import { useFollows } from "@voteapp/api-client";
import { APP_NAME } from "@voteapp/api-client";
import { useMe } from "@voteapp/api-client";
import { useMyResearchAreas } from "@voteapp/api-client";
import { UNRANKED_RESEARCH_AREA_RANK } from "@voteapp/api-client";

type RecordView = "by_issue" | "my_issues" | "newest";

// A researched incumbent can carry 50+ records; rendering everything open
// made the profile a 10,000px wall. Grouped views start with EVERY issue
// group collapsed behind its per-group count, so the profile opens as a
// readable index of which issues the candidate has a record on and the
// reader picks what to expand; the flat newest view cuts off with an
// explicit "show all".
const INITIAL_NEWEST_RECORDS = 20;

type RecordGroup = {
  /** null for the untagged "Other records" pseudo-group. */
  areaId: string | null;
  /** null for "Other records"; drives the public-salience ordering. */
  areaSlug: string | null;
  areaName: string;
  records: CandidateRecord[];
};

// Records grouped by research area (a record with several tags appears under
// each; untagged records fall into "Other records"). Groups key on the
// stable research_area_id — display names are presentation, not identity.
// Groups order by public salience (same ranking as election-card chips), not
// alphabetically, so the issues voters care about most lead; "Other records"
// stays last.
function groupRecords(records: CandidateRecord[]): RecordGroup[] {
  const groups = new Map<string | null, RecordGroup>();
  for (const record of records) {
    const areas = record.research_area_tags.length
      ? record.research_area_tags.map((tag) => ({
          areaId: tag.research_area_id,
          areaSlug: tag.slug,
          areaName: tag.name,
        }))
      : [{ areaId: null, areaSlug: null, areaName: "Other records" }];
    for (const area of areas) {
      const group = groups.get(area.areaId) ?? { ...area, records: [] };
      group.records.push(record);
      groups.set(area.areaId, group);
    }
  }
  return [...groups.values()].sort((a, b) =>
    a.areaId === null || a.areaSlug === null
      ? 1
      : b.areaId === null || b.areaSlug === null
        ? -1
        : compareByResearchAreaPriority(
            { slug: a.areaSlug, name: a.areaName },
            { slug: b.areaSlug, name: b.areaName }
          )
  );
}

// "My issues first": saved-area groups move to the front ordered by the
// user's rank (unranked saved areas after ranked ones), everything else
// keeps the public-salience order groupRecords produced.
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

// Loader payload: the candidate detail plus this candidate's finance for
// each election they are currently in, keyed by candidate_election_id.
export type CandidateLoaderData = CandidateDetail & {
  ongoing_finance: Record<string, FinanceSummary | null>;
};

// Server loader: the candidate subject arrives in the document HTML so
// non-JS crawlers can read it. Anonymous by design — see loadFromApi.
//
// Ongoing-election finance rides along in the loader (not a client query):
// SSR never dehydrates query state, so a client-side fetch would leave the
// profile's only finance surface invisible to crawlers and no-JS readers.
// Election-specific finance still stays off the candidate detail payload
// (see backend candidateDetailReader.ts) — the loader hits the narrow
// per-candidate finance endpoint, instead of the full election detail with
// every opponent's records.
export async function loader({ params, request }: LoaderFunctionArgs): Promise<CandidateLoaderData> {
  const detail = await loadFromApi<CandidateDetail>(`/api/candidates/${params.candidateId}`, request);
  const today = usLatestLocalDate();
  const ongoingElections = detail.candidate.elections.filter(
    (election) => election.election_date >= today
  );
  const entries = await Promise.all(
    ongoingElections.map(async (election): Promise<[string, FinanceSummary | null]> => {
      try {
        const { finance_summary } = await loadFromApi<{ finance_summary: FinanceSummary | null }>(
          `/api/elections/${election.election_id}/candidates/${detail.candidate.candidate_id}/finance`,
          request
        );
        return [election.candidate_election_id, finance_summary];
      } catch {
        // A finance failure (404/429/5xx/timeout) must not take down the
        // whole profile; the section simply doesn't render — same degradation
        // the old client-side fetch had.
        return [election.candidate_election_id, null];
      }
    })
  );
  return { ...detail, ongoing_finance: Object.fromEntries(entries) };
}

// Finance for an election the candidate is currently in, server-fetched by
// the loader so crawlers see it. Renders its own section so there is no
// orphan heading when the election has no finance coverage.
function OngoingElectionFinance({ election, summary }: { election: CandidateElection; summary: FinanceSummary | null }) {
  if (!hasFinanceContent(summary)) {
    return null;
  }
  return (
    <section className="mt-6">
      {/* Collapsed by default: finance is reference material, and open it
          pushed the record — the page's main content — below the fold. The
          collapsed content still ships in the SSR HTML (details just hides
          it), so crawler readability is unaffected.

          The heading lives OUTSIDE the summary, sr-only: browsers map
          <summary> to a button, and a heading inside it can drop out of
          screen-reader heading navigation (the HTML content model also
          forbids mixing a heading with phrasing content there). The election
          name is in both — heading and visible line — because a candidate
          can be in two concurrent races, which would otherwise render two
          indistinguishable "Campaign finance" rows. */}
      <h2 className="sr-only">{`Campaign Finance Information — ${election.official_ballot_title}`}</h2>
      <details>
        <summary className="cursor-pointer select-none">
          <span className="text-lg font-semibold">Campaign Finance Information</span>{" "}
          <span className="text-sm text-ink-soft">
            · {election.official_ballot_title} · {formatElectionDate(election.election_date)}
          </span>
        </summary>
        <div className="mt-2 rounded-xl border border-line bg-white p-4">
          <FinanceSummaryCard summary={summary} />
        </div>
      </details>
    </section>
  );
}

// The stance-bearing tag this record card should claim in a group view: the
// group's area decides — the same record can be for one area and against
// another, so the other areas' stances must not leak into this group. The
// flat view has no single chip; it spells out per-tag stances in the meta
// line instead.
function recordStanceTag(record: CandidateRecord, areaId: string) {
  const tag = record.research_area_tags.find((t) => t.research_area_id === areaId);
  return tag?.stance === "for" || tag?.stance === "against" ? { ...tag, stance: tag.stance } : null;
}

// Judicial evaluative areas, where a for/against tag grades the EVIDENCE
// (favorable/unfavorable), not the candidate's advocacy — the label contract
// requires a stance on every non-neutral area, these two included. Advocacy
// verbs there would state an intent the data never claimed ("Opposes Legal
// Competence"), so they get evidence wording instead.
const EVALUATIVE_AREA_SLUGS = new Set(["legal_competence", "impartiality"]);

// The stance phrase names its topic ("Supports Gun Control", never a bare
// "For") because cards get read without their group heading — quoted,
// screenshotted, or far down an open group — and next to a "Voted no ..."
// description a bare "For" reads as the vote direction, the opposite of
// what it means.
function stanceLabel(stance: "for" | "against", slug: string, name: string): string {
  if (EVALUATIVE_AREA_SLUGS.has(slug)) {
    return stance === "for" ? `Favorable on ${name}` : `Unfavorable on ${name}`;
  }
  return stance === "for" ? `Supports ${name}` : `Opposes ${name}`;
}

// Small colored stance marker — direction as a quiet cue, not a whole-card
// color wash. Colored text only, no box: a bordered chip read as a button.
// Same palette as the stance text on the election page.
function StanceChip({ stance, label }: { stance: "for" | "against"; label: string }) {
  return (
    <span className={stance === "for" ? "font-medium text-green-900" : "font-medium text-red-900"}>
      {label}
    </span>
  );
}

// One record card, shared by the grouped and flat views (the flat view adds
// the area tags to the meta line since there is no group heading to carry
// them). `stanceAreaId` is the group's area in grouped views (null for the
// untagged "Other records" pseudo-group); undefined in the flat view, which
// has no single chip.
function RecordItem({
  record,
  showTags,
  reporterEmail,
  stanceAreaId,
}: {
  record: CandidateRecord;
  showTags: boolean;
  reporterEmail?: string | null;
  stanceAreaId?: string | null;
}) {
  const stanceTag = stanceAreaId != null ? recordStanceTag(record, stanceAreaId) : null;
  return (
    <li className="rounded-xl border border-line bg-white p-3">
      <p className="text-sm text-ink">{record.description}</p>
      <p className="mt-1 flex flex-wrap items-center gap-x-1.5 gap-y-1 text-xs text-ink-soft">
        <span>{formatElectionDate(record.event_date)}</span>
        {stanceTag ? (
          <StanceChip
            stance={stanceTag.stance}
            label={stanceLabel(stanceTag.stance, stanceTag.slug, stanceTag.name)}
          />
        ) : null}
        {showTags && record.research_area_tags.length > 0 ? (
          // Per-tag stance in the flat view, in the same colored verb
          // phrasing as the grouped chip: a record can be for one area and
          // against another, so each tag carries its own direction.
          <span>
            ·{" "}
            {record.research_area_tags.map((tag, index) => (
              <Fragment key={tag.research_area_id}>
                {index > 0 ? ", " : null}
                <span
                  className={
                    tag.stance === "for"
                      ? "font-medium text-green-900"
                      : tag.stance === "against"
                        ? "font-medium text-red-900"
                        : undefined
                  }
                >
                  {tag.stance === "for" || tag.stance === "against"
                    ? stanceLabel(tag.stance, tag.slug, tag.name)
                    : tag.name}
                </span>
              </Fragment>
            ))}
          </span>
        ) : null}
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

// "Name (Party, State)" built from the non-empty parts: party is typed
// string but the detail reader coalesces a missing value to ""
// (candidateDetailReader.ts), and "Jane Doe (, CA)" must not reach a share
// card or a share sheet. Mirrored on the mobile candidate screen.
function candidateShareText(candidate: { display_name: string; party: string; state: string }): string {
  const context = [candidate.party, candidate.state].filter(Boolean).join(", ");
  return context ? `${candidate.display_name} (${context})` : candidate.display_name;
}

// Replaces useDocumentTitle here: a leaf meta export fully overrides the
// root's, so it must carry the full pageMeta set — title alone would drop
// the og:*/twitter:* share-card tags on exactly the page people share.
export const meta: MetaFunction<typeof loader> = ({ data, error, location }) => {
  if (!data) {
    // "Not found" only for real 404s; a 429/502/504 render must not tell
    // crawlers the page doesn't exist.
    const isNotFound = isRouteErrorResponse(error) && error.status === 404;
    return [{ title: isNotFound ? `Not found · ${APP_NAME}` : `Something went wrong · ${APP_NAME}` }];
  }
  const candidate = data.candidate;
  return pageMeta({
    title: `${candidate.display_name} · ${APP_NAME}`,
    description: `${candidateShareText(candidate)} — issue-tagged records with sources, election history, and campaign finance.`,
    path: location.pathname,
  });
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
  // null = the user hasn't picked a view; default to "my issues first" once
  // saved areas exist (they load async, so this can't live in useState's
  // initial value). An explicit pick always wins — except a picked
  // "my_issues" is ignored while hasSaved is false (the user cleared their
  // areas in Settings; the shared query key syncs it here), because its
  // <option> is gated on hasSaved and a value without an option leaves the
  // select uncontrolled. The pick is kept, not cleared: re-saving areas
  // restores it.
  const [chosenRecordView, setChosenRecordView] = useState<RecordView | null>(null);
  const effectiveChosenView = chosenRecordView === "my_issues" && !hasSaved ? null : chosenRecordView;
  const recordView = effectiveChosenView ?? (hasSaved ? "my_issues" : "by_issue");
  const [showAllNewest, setShowAllNewest] = useState(false);

  const detail = useLoaderData<typeof loader>();
  const candidate = detail.candidate;
  // ?? {}: tolerates loader data from before this field existed (deploy skew
  // between a cached document and fresh code) by rendering no finance.
  const ongoingFinance = detail.ongoing_finance ?? {};
  const isFollowing = (follows ?? []).some((follow) => follow.candidate_id === candidate.candidate_id);
  const baseGroups = groupRecords(candidate.records);
  const recordGroups =
    recordView === "my_issues" ? orderGroupsByPreference(baseGroups, preferences) : baseGroups;
  const today = usLatestLocalDate();
  const ongoingElections = candidate.elections.filter((election) => election.election_date >= today);
  // "My choice" rows: one per ongoing OFFICE candidacy the candidate hasn't
  // withdrawn or lost — a candidate can be in several races at once (and
  // have past ones), so each row names its election and only pickable
  // candidacies get a button. Rendered only once the choices list is loaded
  // (no-flash rule, like the follow button).
  const { choiceByElectionId, canChoose } = useElectionChoices();
  const officeCandidacies = ongoingElections.filter(
    (election) => election.race_type === "office" && election.status !== "withdrawn" && election.status !== "lost"
  );
  const pickableElections = canChoose && choiceByElectionId !== undefined ? officeCandidacies : [];
  // Logged-out visitors see the same rows, but clicking prompts them to
  // register (mirrors RegisterToFollowButton). me is undefined while the
  // session loads — render nothing then to avoid a flash of the wrong row.
  const registerToPickElections = me === null ? officeCandidacies : [];

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
        <div className="flex items-center gap-2">
          <ShareButton
            path={`/candidates/${candidate.candidate_id}`}
            shareText={candidateShareText(candidate)}
          />
          {canFollow && follows ? (
            <FollowButton candidateId={candidate.candidate_id} isFollowing={isFollowing} />
          ) : me === null ? (
            // Logged-out visitors get a Follow button that prompts them to
            // register (me is undefined while the session is still loading —
            // render nothing then to avoid a flash of the wrong button).
            <RegisterToFollowButton candidateName={candidate.display_name} />
          ) : null}
        </div>
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

      {pickableElections.length > 0 ? (
        <div className="mt-4 space-y-2">
          {pickableElections.map((election) => (
            /* Always names the election: several concurrent races (and past
               ones) exist, and the pick must land on the right one. */
            <CandidatePickRow
              key={election.candidate_election_id}
              electionId={election.election_id}
              candidateId={candidate.candidate_id}
              candidateName={candidate.display_name}
              raceName={election.official_ballot_title}
              dateLabel={formatElectionDate(election.election_date)}
              choice={choiceByElectionId?.get(election.election_id)}
              seatsToFill={election.seats_to_fill ?? null}
            />
          ))}
        </div>
      ) : registerToPickElections.length > 0 ? (
        <div className="mt-4 space-y-2">
          {registerToPickElections.map((election) => (
            <RegisterToPickRow
              key={election.candidate_election_id}
              candidateName={candidate.display_name}
              raceName={election.official_ballot_title}
              dateLabel={formatElectionDate(election.election_date)}
            />
          ))}
        </div>
      ) : null}

      {ongoingElections.map((election) => (
        <OngoingElectionFinance
          key={election.candidate_election_id}
          election={election}
          summary={ongoingFinance[election.candidate_election_id] ?? null}
        />
      ))}

      {recordGroups.length > 0 ? (
        <section className="mt-6">
          <div className="flex flex-wrap items-center justify-between gap-3">
            {/* "Track record", not "Record"/"Records": bare "Record" read as
                a typo next to a list of many items, and "Records" reads as
                documents. This is the home-page promise ("who these
                candidates really are by their records") paid off. */}
            <h2 className="text-lg font-semibold">Track record</h2>
            <label className="flex items-center gap-2 text-sm text-ink-soft">
              View
              <select
                value={recordView}
                onChange={(event) => setChosenRecordView(event.target.value as RecordView)}
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
            recordGroups.map((group) => (
              // Every group starts collapsed; with no `open` prop React
              // never re-applies a default, so a reader's toggles survive a
              // view switch that reorders the groups.
              <details key={group.areaId ?? "other"} className="mt-4">
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
                      stanceAreaId={group.areaId}
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
                  · {formatElectionDate(election.election_date)} · {formatDistrictName(election.district.name)}
                  {election.is_incumbent ? " · incumbent" : ""}
                </span>
                {/* No finance on past-election rows: campaign finance shows
                    only for the election(s) the candidate is currently in
                    (the eager section above). */}
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

      {/* Last on purpose: reporting is a reaction to reading the profile, not
          a headline action worth space above the record. Per-record report
          buttons stay on their cards. */}
      <div className="mt-6">
        <ReportContentButton
          entityType="candidate"
          entityId={candidate.candidate_id}
          contextLabel="candidate profile"
          reporterEmail={me?.email}
        />
      </div>
    </div>
  );
}

export default CandidatePage;
