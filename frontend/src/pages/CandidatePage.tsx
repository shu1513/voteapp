import { Fragment, useState } from "react";
import { isRouteErrorResponse, Link, useLoaderData, useLocation, useRouteError } from "react-router";
import type { LoaderFunctionArgs, MetaFunction } from "react-router";
import type {
  CandidateDetail,
  CandidateElection,
  CandidateRecord,
  FinanceSummary,
  RecordAreaStance,
  ResearchAreaPreference,
} from "@voteapp/api-client";
import { aggregateRecordAreaStances } from "@voteapp/api-client";
import {
  CANDIDATE_RAIL_SORTS,
  candidateRailSortsOffered,
  sortCandidateRailEntries,
  type CandidateRailSortKey,
} from "@voteapp/api-client";
import { DetailPager } from "../components/DetailPager";
import { DetailRail } from "../components/DetailRail";
import {
  pagerNeighbors,
  readCandidateNavState,
  type CandidateNavState,
  type ElectionNavState,
} from "../lib/detailNavContext";
import { JsonLdScript } from "../components/JsonLdScript";
import { NotFoundNotice } from "../components/NotFoundNotice";
import { RouteError } from "../components/RouteError";
import { SourceLine } from "../components/SourceLine";
import { FollowButton } from "../components/FollowButton";
import { RegisterToFollowButton } from "../components/RegisterToFollowButton";
import { ShareButton } from "../components/ShareButton";
import { CandidatePickButton, CandidatePickRow } from "../components/ElectionChoiceControls";
import { draftChoicesByElectionId, useBallotDraft } from "../lib/ballotDraft";
import { PostPickActions } from "../components/PostPickActions";
import { useElectionChoices } from "@voteapp/api-client";
import { FinanceSummaryCard, hasFinanceContent } from "../components/FinanceSummaryCard";
import { ReportContentButton } from "../components/ReportContentButton";
import { formatDistrictName, formatElectionDate } from "@voteapp/api-client";
import { loadFromApi } from "../lib/loadFromApi";
import { pageMeta } from "../lib/pageMeta";
import { useHydrated } from "../lib/useHydrated";
import { usLatestLocalDate } from "../lib/usLatestLocalDate";
import { compareByResearchAreaPriority } from "@voteapp/api-client";
import { partyColorClass, profilePartyLabel } from "@voteapp/api-client";
import { candidateProfileLinks } from "@voteapp/api-client";
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
          <span className="text-lg font-semibold text-green-600" aria-hidden="true">$ </span>
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

// Collapsed-group stance tally: how many of the group's records are for /
// against THIS group's area (a record can lean differently per area, so the
// count must come from the group's own tag, same rule as recordStanceTag).
// Neutral-tagged records count toward neither, so the two numbers need not
// sum to the record count. The "Other records" group has no area and gets
// zeros.
function groupStanceCounts(group: RecordGroup): { forCount: number; againstCount: number } {
  let forCount = 0;
  let againstCount = 0;
  if (group.areaId != null) {
    for (const record of group.records) {
      const stance = recordStanceTag(record, group.areaId)?.stance;
      if (stance === "for") forCount += 1;
      else if (stance === "against") againstCount += 1;
    }
  }
  return { forCount, againstCount };
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

// Page-top stance summary: every area classifies on the same counts the
// election roster rows color by (aggregateRecordAreaStances) — all-for is
// Supports, all-against is Opposes, any split is Mixed. Deliberately no
// majority rule: one against-record among five for-records makes the area
// Mixed, because collapsing it to "Supports" would assert a position the
// evidence doesn't hold. Evaluative areas are excluded — their for/against
// grades the evidence, not advocacy (see EVALUATIVE_AREA_SLUGS), so they
// have no place in a supports/opposes box; null-stance areas (general,
// integrity_and_ethics) never leave the aggregator. Order matches the record
// groups below: the viewer's saved areas first by their own rank (passed
// only in the "my issues first" view — empty otherwise), everything else by
// public salience.
function classifyStanceSummary(
  records: readonly CandidateRecord[],
  preferences: readonly ResearchAreaPreference[]
): {
  supports: RecordAreaStance[];
  opposes: RecordAreaStance[];
  mixed: RecordAreaStance[];
} {
  const rankByAreaId = new Map(
    preferences.map((preference) => [preference.research_area_id, preference.rank ?? UNRANKED_RESEARCH_AREA_RANK])
  );
  const areas = aggregateRecordAreaStances(records)
    .filter((area) => !EVALUATIVE_AREA_SLUGS.has(area.slug))
    .sort(
      (a, b) =>
        (rankByAreaId.get(a.research_area_id) ?? Number.POSITIVE_INFINITY) -
          (rankByAreaId.get(b.research_area_id) ?? Number.POSITIVE_INFINITY) ||
        compareByResearchAreaPriority(a, b)
    );
  return {
    // Every aggregated area has for_count + against_count >= 1, so a zero on
    // one side means the record is unanimous the other way.
    supports: areas.filter((area) => area.against_count === 0),
    opposes: areas.filter((area) => area.for_count === 0),
    mixed: areas.filter((area) => area.for_count > 0 && area.against_count > 0),
  };
}

// The candidate-page counterpart of the measure page's "A YES vote means" /
// "A NO vote means" boxes: green what the record supports, red what it
// opposes, amber where it splits (full width below the pair — a third
// column would squeeze all three on desktop and mixed is the box that
// needs its counts read). Renders nothing when no area classifies, so a
// record-less or judicial-only profile gets no empty shell.
function StanceSummary({
  candidateName,
  records,
  preferences,
}: {
  candidateName: string;
  records: CandidateRecord[];
  preferences: readonly ResearchAreaPreference[];
}) {
  const { supports, opposes, mixed } = classifyStanceSummary(records, preferences);
  if (supports.length === 0 && opposes.length === 0 && mixed.length === 0) {
    return null;
  }
  // The viewer's saved areas render semibold so their issues stand out from
  // the rest of the list, mirroring the front-of-list ordering.
  const savedAreaIds = new Set(preferences.map((preference) => preference.research_area_id));
  // Comma-separated text, not boxed chips (boxes read as buttons — same
  // rule as the roster rows). Name and count stay one text node so an
  // exact-match query for the bare area name still resolves to the record
  // group heading, not this summary.
  const areaWithCount = (area: RecordAreaStance) => {
    const count = area.for_count + area.against_count;
    const text = `${area.name} (${count} record${count === 1 ? "" : "s"})`;
    return savedAreaIds.has(area.research_area_id) ? <span className="font-semibold">{text}</span> : text;
  };
  const sideBox = (side: "supports" | "opposes", areas: RecordAreaStance[]) =>
    areas.length === 0 ? null : (
      <div
        className={
          side === "supports"
            ? "rounded border border-green-200 bg-green-50 p-3"
            : "rounded border border-red-200 bg-red-50 p-3"
        }
      >
        <h3
          className={
            side === "supports"
              ? "text-sm font-semibold text-green-900"
              : "text-sm font-semibold text-red-900"
          }
        >
          {side === "supports" ? "Supports" : "Opposes"}
        </h3>
        <p className={side === "supports" ? "mt-1 text-sm text-green-900" : "mt-1 text-sm text-red-900"}>
          {areas.map((area, index) => (
            <Fragment key={area.research_area_id}>
              {index > 0 ? ", " : null}
              {areaWithCount(area)}
            </Fragment>
          ))}
        </p>
      </div>
    );
  return (
    <section className="mt-4">
      {/* sr-only heading so the section lands in heading navigation; the
          visible lead-in is aria-hidden because it says the same thing —
          without the name, which a heading jumped to on its own needs. */}
      <h2 className="sr-only">{`Where ${candidateName} stands, based on their records`}</h2>
      <p className="text-sm text-ink-soft" aria-hidden="true">
        Where they stand, based on their records:
      </p>
      {supports.length > 0 || opposes.length > 0 ? (
        // Two columns only when both sides exist — one box alone spans the
        // full row instead of leaving an empty half.
        <div className={`mt-2 grid gap-3${supports.length > 0 && opposes.length > 0 ? " sm:grid-cols-2" : ""}`}>
          {sideBox("supports", supports)}
          {sideBox("opposes", opposes)}
        </div>
      ) : null}
      {mixed.length > 0 ? (
        <div className="mt-3 rounded border border-amber-200 bg-amber-50 p-3">
          <h3 className="text-sm font-semibold text-amber-900">Mixed record</h3>
          <p className="mt-1 text-sm text-amber-900">
            {/* Same "N support · N oppose" phrasing as the record group
                headers, so the two surfaces can't drift apart. */}
            {mixed.map((area, index) => {
              const text = `${area.name} (${area.for_count} support · ${area.against_count} oppose)`;
              return (
                <Fragment key={area.research_area_id}>
                  {index > 0 ? ", " : null}
                  {savedAreaIds.has(area.research_area_id) ? (
                    <span className="font-semibold">{text}</span>
                  ) : (
                    text
                  )}
                </Fragment>
              );
            })}
          </p>
        </div>
      ) : null}
    </section>
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
// card or a share sheet. Placeholder parties ("Nonpartisan"/"Unknown") are
// hidden the same way as in the header. Mirrored on the mobile candidate
// screen.
function candidateShareText(candidate: { display_name: string; party: string; state: string }): string {
  const context = [profilePartyLabel(candidate.party), candidate.state].filter(Boolean).join(", ");
  return context ? `${candidate.display_name} (${context})` : candidate.display_name;
}

// One election list, rendered once for the races still ahead and once for
// the finished ones. Only the heading differs.
function ElectionHistorySection({
  heading,
  elections,
  navState,
}: {
  heading: string;
  elections: CandidateElection[];
  navState: ElectionNavState;
}) {
  return (
    <section className="mt-6">
      <h2 className="text-lg font-semibold">{heading}</h2>
      <ul className="mt-2 divide-y divide-line rounded-xl border border-line bg-white">
        {elections.map((election) => (
          <li key={election.candidate_election_id} className="px-3 py-2 text-sm">
            <Link
              to={`/elections/${election.election_id}`}
              state={navState}
              className="text-ink underline hover:text-rausch"
            >
              {election.official_ballot_title}
            </Link>{" "}
            <span className="text-ink-soft">
              · {formatElectionDate(election.election_date)} · {formatDistrictName(election.district.name)}
              {election.is_incumbent ? " · incumbent" : ""}
            </span>
            {/* No finance on any row here: campaign finance shows only for the
                election(s) the candidate is currently in (the eager section
                above). */}
          </li>
        ))}
      </ul>
    </section>
  );
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
  const { hasSaved, preferences, weights, isLoading: savedAreasLoading } = useMyResearchAreas();
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
  // Keyed by candidate, unlike the view pick above: the roster pager keeps
  // this component mounted across candidates, and a bare boolean would leak
  // one candidate's 50-record expansion into the next — defeating the
  // 20-record cap. Same per-entity keying as ElectionPage's party filter
  // (the view pick is a preference that travels; expanding a list is not);
  // stale state is simply never read.
  const [newestExpansion, setNewestExpansion] = useState<{ candidateId: string; on: boolean }>({
    candidateId: "",
    on: false,
  });

  const detail = useLoaderData<typeof loader>();
  const candidate = detail.candidate;
  const showAllNewest = newestExpansion.candidateId === candidate.candidate_id && newestExpansion.on;
  // ?? {}: tolerates loader data from before this field existed (deploy skew
  // between a cached document and fresh code) by rendering no finance.
  const ongoingFinance = detail.ongoing_finance ?? {};
  const isFollowing = (follows ?? []).some((follow) => follow.candidate_id === candidate.candidate_id);
  const profileLinks = candidateProfileLinks(candidate);
  const baseGroups = groupRecords(candidate.records);
  const recordGroups =
    recordView === "my_issues" ? orderGroupsByPreference(baseGroups, preferences) : baseGroups;
  const today = usLatestLocalDate();
  const ongoingElections = candidate.elections.filter((election) => election.election_date >= today);
  // The history list splits on the same date boundary: "is in" would misread
  // on a race that finished years ago. Within the ongoing bucket it also
  // splits on candidacy status — a withdrawn (or eliminated) candidate is
  // not "in" a race whose date is still ahead, but the candidacy stays
  // visible as history (the API keeps withdrawn links on purpose). Same
  // status rule as officeCandidacies below.
  const isExitedCandidacy = (election: CandidateElection): boolean =>
    election.status === "withdrawn" || election.status === "lost";
  const activeOngoingElections = ongoingElections.filter((election) => !isExitedCandidacy(election));
  const exitedOngoingElections = ongoingElections.filter(isExitedCandidacy);
  const pastElections = candidate.elections.filter((election) => election.election_date < today);
  // "My choice" rows: one per ongoing OFFICE candidacy the candidate hasn't
  // withdrawn or lost — a candidate can be in several races at once (and
  // have past ones), so each row names its election and only pickable
  // candidacies get a button. Rendered only once the choices list is loaded
  // (no-flash rule, like the follow button).
  const { choiceByElectionId, canChoose } = useElectionChoices();
  const draft = useBallotDraft();
  const officeCandidacies = ongoingElections.filter(
    (election) => election.race_type === "office" && election.status !== "withdrawn" && election.status !== "lost"
  );
  // Guests get the same rows writing to the local ballot draft
  // (lib/ballotDraft.ts) instead of the account endpoint. me is undefined
  // while the session loads — render nothing then to avoid a flash of the
  // wrong row (same no-flash rule as the follow button).
  const isGuest = me === null;
  const pickableElections =
    isGuest || (canChoose && choiceByElectionId !== undefined) ? officeCandidacies : [];
  // The page's primary action ("Add to cart"): the sticky bottom pick card.
  // Only when the candidate is in exactly one pickable race — the card's
  // button carries no race name, so with several races it can't say which
  // one it would pick; those pages rely on the self-describing rows below.
  const primaryPickElection = pickableElections.length === 1 ? pickableElections[0] : null;
  const choiceForElection = (electionId: string) =>
    isGuest ? draftChoicesByElectionId(draft).get(electionId) : choiceByElectionId?.get(electionId);
  // Whether THIS candidate holds (one of) the pick(s) for the card's race —
  // gates the card's post-pick actions. True on arrival too, not only right
  // after clicking: the "where to next" links are just as useful when a
  // reader returns to a candidate they already picked.
  const isPrimaryPicked = primaryPickElection !== null &&
    (choiceForElection(primaryPickElection.election_id)?.picks ?? []).some(
      (pick) => pick.candidate_id === candidate.candidate_id
    );
  const location = useLocation();
  const hydrated = useHydrated();
  // Same hydration gate as the election page: location.state survives
  // reloads via history.state, but SSR rendered with null — reading it
  // before hydration mismatches the server HTML.
  const navState = hydrated ? readCandidateNavState(location.state) : null;
  // The rail's roster sort: offered only for the sorts this snapshot can
  // honor (candidateRailSortsOffered — an old snapshot without the stance
  // keys offers none; My issues additionally needs saved areas). Same
  // persistence story as the election rail's sort: component state across
  // sibling walks (the route element stays mounted), nav state across
  // remounts (election round trips). No "As listed" option here, unlike the
  // election rail: the roster's arrival order is always one of the two
  // offered sorts (the election page's own options), so an always-engaged
  // sort loses nothing — the default is the first offered ("My issues
  // first" with saved areas, A–Z without, matching the roster's own
  // defaults).
  const railRoster = navState?.candidates;
  // Withheld while the saved areas are still loading: the default sort is
  // the first offered option, and engaging A–Z in the window before
  // hasSaved settles would visibly re-shuffle the rail on every arrival
  // for viewers whose default is My issues first.
  const offeredRailSorts = savedAreasLoading
    ? []
    : candidateRailSortsOffered(railRoster ?? [], hasSaved);
  const [railSortOverride, setRailSortState] = useState<CandidateRailSortKey | null>(null);
  const railSortState = railSortOverride ?? navState?.railSort ?? null;
  const railSort =
    railSortState !== null && offeredRailSorts.includes(railSortState)
      ? railSortState
      : (offeredRailSorts[0] ?? null);
  // Sorting re-orders but never removes, so the displayed roster keeps the
  // same membership gate as the arrival list.
  const displayedRoster =
    railSort !== null && railRoster !== undefined
      ? sortCandidateRailEntries(railRoster, railSort, weights)
      : railRoster;
  // The context handed onward — sibling walks and the election round trip —
  // carries the rail's CURRENT sort; the back destination needs no rewrite
  // here (the election page's roster sort is component state, not URL).
  const forwardedNavState: CandidateNavState | null =
    navState === null
      ? null
      : offeredRailSorts.length === 0
        ? navState
        : (() => {
            // Field removal only on the copy — never mutate the shared
            // original.
            const forwarded: CandidateNavState = { ...navState };
            if (railSort) {
              forwarded.railSort = railSort;
            } else {
              delete forwarded.railSort;
            }
            return forwarded;
          })();
  // The back hop's election state: the arrival context with rosterSort
  // overridden by this rail's CURRENT sort, so a sort switched here walks
  // back into the election page's roster (rail and roster are one
  // continuous control — same value space by construction). Recomputed at
  // render on every sibling page, so the override always reflects the sort
  // on screen, not the one at departure. With no engaged rail sort (an old
  // unkeyed snapshot) the arrival state passes through untouched.
  const backToElectionState =
    navState?.backState !== undefined && railSort !== null
      ? { ...navState.backState, rosterSort: railSort }
      : navState?.backState;
  // Every election link on this page (the back-link fallback and the
  // Elections history list) tells the election page to come back here. This
  // page's own arrival context rides along (backState) so the round trip
  // hands it back — without it, My Picks → candidate → election → back
  // would land on a candidate page that forgot it came from My Picks.
  const electionNavState: ElectionNavState = {
    backTo: { path: `/candidates/${candidate.candidate_id}`, label: candidate.display_name },
    ...(forwardedNavState ? { backState: forwardedNavState } : {}),
  };
  // Prev/next over the arrival election's roster as the rail displays it (a
  // candidate can be in several races — the sequence is scoped to the one
  // the reader came from). Null (back slot only) when this candidate fell
  // out of the snapshot. The nav bar exists only for in-app arrivals: no
  // router state (deep link) = no bar, by product choice.
  const rosterNeighbors = pagerNeighbors(displayedRoster, candidate.candidate_id);
  // Desktop rail: the arrival race's roster under the same guard as
  // prev/next (pagerNeighbors is null unless the list has >= 2 entries and
  // contains this candidate). navState is re-read only for the type system —
  // non-null neighbors implies it.
  const railCandidates = rosterNeighbors !== null ? (displayedRoster ?? null) : null;
  // The rail's pick checks, mirroring the election rail: the green mark on
  // the candidate(s) the viewer picked IN THIS RACE — same choice source as
  // the "My choice" rows (account choices signed-in, local draft as guest).
  // Scoped through the snapshot's electionId (the race the roster belongs
  // to); an old snapshot without it degrades to no checks. Multi-seat races
  // can legitimately check several rows.
  const railChoice = navState?.electionId !== undefined ? choiceForElection(navState.electionId) : undefined;
  const railPickedIds = new Set((railChoice?.picks ?? []).map((pick) => pick.candidate_id));

  // Display label for the back slot: when the destination is an election,
  // its official ballot title runs to legal-name length ("For United States
  // Representative, 1st Congressional District") and the reader just left
  // it — the Elections section below names it anyway. A generic "Election"
  // reads cleaner. List destinations ("My Picks", "Shared picks") keep
  // their short names.
  const pagerBackTo = navState
    ? navState.backTo.path.startsWith("/elections/")
      ? { path: navState.backTo.path, label: "Election" }
      : navState.backTo
    : null;

  // The nav bar at the top: prev | back | next, each slot captioned. The
  // back slot restores the election page's own ballot sequence (backState).
  // With the rail on screen (lg+) the bar is redundant, so it drops to
  // narrow screens only; rail-less arrivals keep it at every width.
  const pagerBar =
    navState && pagerBackTo ? (
      <DetailPager
        ariaLabel="Candidate navigation"
        prev={
          rosterNeighbors?.prev
            ? { path: `/candidates/${rosterNeighbors.prev.id}`, label: rosterNeighbors.prev.name }
            : null
        }
        next={
          rosterNeighbors?.next
            ? { path: `/candidates/${rosterNeighbors.next.id}`, label: rosterNeighbors.next.name }
            : null
        }
        backTo={pagerBackTo}
        backToState={backToElectionState}
        siblingState={forwardedNavState}
      />
    ) : null;

  return (
    // With rail context the page widens to a two-column grid on lg+ (rail |
    // detail); without it — deep links, stale snapshots — the markup is the
    // classic centered column at every width. Mirrors ElectionPage.
    <div
      className={
        railCandidates !== null
          ? "mx-auto max-w-3xl px-4 py-8 lg:grid lg:max-w-6xl lg:grid-cols-[18rem_minmax(0,1fr)] lg:gap-8"
          : "mx-auto max-w-3xl px-4 py-8"
      }
    >
      {railCandidates !== null && navState !== null ? (
        // The rail's exit link keeps the full backTo label (the election's
        // ballot title): rail rows truncate, so length is fine there, and
        // the fuller name is clearer than the pager's generic "Election".
        <DetailRail
          ariaLabel="Candidates in this race"
          entries={railCandidates.map((entry) => ({
            id: entry.id,
            label: entry.name,
            path: `/candidates/${entry.id}`,
            picked: railPickedIds.has(entry.id),
          }))}
          pickedSrLabel="my pick"
          currentId={candidate.candidate_id}
          backTo={navState.backTo}
          backToState={backToElectionState}
          siblingState={forwardedNavState}
          headerSlot={
            // The list label renders even when no sort is offerable (an old
            // snapshot): naming WHAT the rows are never depends on the keys.
            <div className="flex flex-col gap-1.5">
              {/* text-ink, not -soft: the label is the rail's identity, not
                  a caption — it must register at a glance. */}
              <p className="text-xs font-semibold uppercase tracking-wide text-ink">Candidates:</p>
              {offeredRailSorts.length > 0 ? (
                <label className="flex items-center gap-1.5 text-xs text-ink-soft">
                  Sort
                  <select
                    value={railSort ?? ""}
                    onChange={(event) =>
                      setRailSortState(event.target.value as CandidateRailSortKey)
                    }
                    className="min-w-0 flex-1 rounded-md border border-line bg-white px-1.5 py-1 text-xs text-ink focus:border-ink focus:outline-none"
                  >
                    {CANDIDATE_RAIL_SORTS.filter((option) =>
                      offeredRailSorts.includes(option.value)
                    ).map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </label>
              ) : null}
            </div>
          }
        />
      ) : null}
      {/* min-w-0: the grid column must be allowed to shrink or long names
          blow the layout; lg:max-w-3xl keeps the reading measure of the
          classic column even though the grid column is wider. In rail mode a
          before pseudo-element draws the rail/detail divider a rem into the
          gutter (centered in gap-8) — a pseudo, not border-l + pl, because
          box-sizing is border-box and padding on this max-w-3xl div would
          eat 17px of reading measure. On the detail side (not the rail) so
          the rule spans the full content height; conditional so deep links
          never grow a stray rule. */}
      <div
        className={
          railCandidates !== null
            ? "min-w-0 lg:relative lg:max-w-3xl lg:before:absolute lg:before:inset-y-0 lg:before:-left-4 lg:before:w-px lg:before:bg-line lg:before:content-['']"
            : "min-w-0 lg:max-w-3xl"
        }
      >
        {railCandidates !== null ? <div className="lg:hidden">{pagerBar}</div> : pagerBar}
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
              <FollowButton
                // Remount on candidate change: the route element stays mounted
                // across candidate-to-candidate navigation, and without the key
                // a follow error from the previous candidate would linger under
                // this one's button.
                key={candidate.candidate_id}
                candidateId={candidate.candidate_id}
                isFollowing={isFollowing}
              />
            ) : me === null ? (
              // Logged-out visitors get a Follow button that prompts them to
              // register (me is undefined while the session is still loading —
              // render nothing then to avoid a flash of the wrong button).
              <RegisterToFollowButton candidateName={candidate.display_name} />
            ) : null}
          </div>
        </div>
        <p className="mt-1 text-sm text-ink-soft">
          {profilePartyLabel(candidate.party) ? (
            <>
              <span className={partyColorClass(candidate.party) || undefined}>
                {profilePartyLabel(candidate.party)}
              </span>{" "}
              ·{" "}
            </>
          ) : null}
          {candidate.state}
          {candidate.current_office ? <> · {candidate.current_office}</> : null}
        </p>
        {profileLinks.length > 0 ? (
          <p className="mt-1 text-sm">
            {profileLinks.map((link, index) => (
              <Fragment key={link.label}>
                {index > 0 ? <span className="text-ink-soft"> · </span> : null}
                <a
                  href={link.href}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-ink underline hover:text-rausch"
                >
                  {link.label}
                </a>
              </Fragment>
            ))}
          </p>
        ) : null}
        {candidate.summary ? <p className="mt-3 text-ink">{candidate.summary}</p> : null}

        {/* Directly after the summary, before the pick rows — the same order
            as the measure page (explainer boxes, then choice buttons). */}
        <StanceSummary
          candidateName={candidate.display_name}
          records={candidate.records}
          // Personalized order/emphasis only in the "my issues first" view,
          // so the summary always matches the record groups below it.
          preferences={recordView === "my_issues" ? preferences : []}
        />

        {/* In-body rows only when the sticky bar can't act: with several
            concurrent races the bar's bare "Make my pick" can't say which
            race it would pick, so each race keeps its self-describing row.
            Single-race pages leave picking to the sticky bar alone. */}
        {primaryPickElection === null && pickableElections.length > 0 ? (
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
                electionDate={election.election_date}
                choice={choiceForElection(election.election_id)}
                seatsToFill={election.seats_to_fill ?? null}
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
                    onClick={() => setNewestExpansion({ candidateId: candidate.candidate_id, on: true })}
                    className="mt-3 rounded-lg border border-line bg-white px-3 py-1.5 text-sm font-medium text-ink transition hover:border-ink"
                  >
                    Show all {candidate.records.length} records
                  </button>
                ) : null}
              </>
            ) : (
              recordGroups.map((group) => {
                // Stance tally shown while collapsed, so the split is readable
                // without opening the group. Evaluative areas keep their
                // evidence wording (favorable/unfavorable), matching the cards
                // inside; zero-count sides stay hidden to avoid "0 oppose"
                // noise. Same colored-text-only treatment as StanceChip.
                const { forCount, againstCount } = groupStanceCounts(group);
                const evaluative = group.areaSlug != null && EVALUATIVE_AREA_SLUGS.has(group.areaSlug);
                return (
                  <div key={group.areaId ?? "other"} className="mt-4">
                    {/* The heading lives OUTSIDE the summary, sr-only — same
                        rule as the finance disclosure above: <summary> maps to
                        a button, and a heading inside it can drop out of
                        screen-reader heading navigation. "Track record — "
                        prefixes the area so the heading reads meaningfully
                        when jumped to on its own, and keeps its text distinct
                        from the visible summary line (which repeats the bare
                        area name). */}
                    <h3 className="sr-only">{`Track record — ${group.areaName}`}</h3>
                    {/* Every group starts collapsed; with no `open` prop React
                        never re-applies a default, so a reader's toggles
                        survive a view switch that reorders the groups. */}
                    <details>
                      <summary className="cursor-pointer select-none">
                        <span className="text-sm font-semibold uppercase tracking-wide text-ink-soft">
                          {group.areaName}
                        </span>{" "}
                        <span className="text-xs text-ink-soft">
                          · {group.records.length} record{group.records.length === 1 ? "" : "s"}
                        </span>
                        {forCount > 0 ? (
                          <span className="text-xs font-medium text-green-900">
                            {" "}
                            · {forCount} {evaluative ? "favorable" : "support"}
                          </span>
                        ) : null}
                        {againstCount > 0 ? (
                          <span className="text-xs font-medium text-red-900">
                            {" "}
                            · {againstCount} {evaluative ? "unfavorable" : "oppose"}
                          </span>
                        ) : null}
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
                  </div>
                );
              })
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

        {/* Not a bare "Elections": on a candidate page that reads as a generic
            section of election news. Name the person and the relationship, and
            split on the election date — "is in" would misread on a race that
            finished years ago, and on a race the candidate withdrew from. */}
        {activeOngoingElections.length > 0 ? (
          <ElectionHistorySection
            heading={`${activeOngoingElections.length === 1 ? "Race" : "Races"} ${candidate.display_name} is in:`}
            elections={activeOngoingElections}
            navState={electionNavState}
          />
        ) : null}

        {exitedOngoingElections.length > 0 ? (
          <ElectionHistorySection
            heading={`${exitedOngoingElections.length === 1 ? "Race" : "Races"} ${candidate.display_name} is no longer in:`}
            elections={exitedOngoingElections}
            navState={electionNavState}
          />
        ) : null}

        {pastElections.length > 0 ? (
          <ElectionHistorySection
            heading={`Past ${pastElections.length === 1 ? "race" : "races"} ${candidate.display_name} ran in:`}
            elections={pastElections}
            navState={electionNavState}
          />
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

        {primaryPickElection ? (
          // The page's ONE pick control: a sticky card pinned to the bottom
          // of the viewport while the profile scrolls, at every width —
          // sticky (not fixed) so in the split layout it stays inside the
          // detail column instead of overlaying the rail. No caption naming
          // the race: the card renders only when the candidate is in exactly
          // one pickable race (see primaryPickElection), so the page itself
          // is the context — a title line here just repeats the "Race X is
          // in" section.
          // data-sticky-pick-cta: index.css lifts the chatbot's floating
          // launcher above this card (both pin to the viewport bottom and
          // the launcher would cover the button's right end on phones).
          <div
            data-sticky-pick-cta=""
            className="sticky bottom-3 z-30 mt-6 rounded-xl border border-line bg-white p-3 shadow-lg"
          >
            <CandidatePickButton
              // Remount on candidate change, like the Follow button above:
              // the route element stays mounted across roster navigation,
              // and without the key a failed save's error from the previous
              // candidate would linger under this one's button.
              key={candidate.candidate_id}
              electionId={primaryPickElection.election_id}
              candidateId={candidate.candidate_id}
              candidateName={candidate.display_name}
              raceTitle={primaryPickElection.official_ballot_title}
              electionDate={primaryPickElection.election_date}
              choice={choiceForElection(primaryPickElection.election_id)}
              seatsToFill={primaryPickElection.seats_to_fill ?? null}
              fullWidth
            />
            {/* "Back to election" only for election arrivals: a My-Picks
                arrival would get a back link and a draft link to the same
                place (see PostPickActions). */}
            {isPrimaryPicked ? (
              <PostPickActions
                back={
                  navState?.backTo.path.startsWith("/elections/")
                    ? { path: navState.backTo.path, state: backToElectionState, label: "election" }
                    : null
                }
              />
            ) : null}
          </div>
        ) : null}
      </div>
    </div>
  );
}

export default CandidatePage;
