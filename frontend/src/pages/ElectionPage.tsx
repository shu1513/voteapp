import { Fragment, useState } from "react";
import { isRouteErrorResponse, Link, useLoaderData, useLocation, useRouteError } from "react-router";
import type { LoaderFunctionArgs, MetaFunction } from "react-router";
import type { ElectionDetail, PartyBucket } from "@voteapp/api-client";
import { DetailPager } from "../components/DetailPager";
import { pagerNeighbors, readElectionNavState, type CandidateNavState } from "../lib/detailNavContext";
import { JsonLdScript } from "../components/JsonLdScript";
import { NotFoundNotice } from "../components/NotFoundNotice";
import { RouteError } from "../components/RouteError";
import { SourceLine } from "../components/SourceLine";
import { ReportContentButton } from "../components/ReportContentButton";
import { ShareButton } from "../components/ShareButton";
import {
  deriveCandidateResultBadges,
  formatDistrictType,
  formatDistrictName,
  formatElectionDate,
  formatOutcome,
  formatRosterStatus,
  formatVotePowerLabel,
} from "@voteapp/api-client";
import { loadFromApi } from "../lib/loadFromApi";
import { pageMeta } from "../lib/pageMeta";
import { usLatestLocalDate } from "../lib/usLatestLocalDate";
import { AREA_TEXT_CLASS } from "../components/ElectionCard";
import { CandidatePickButton, MeasureChoiceButtons } from "../components/ElectionChoiceControls";
import { RegisterToPickButton } from "../components/RegisterToPickControls";
import { splitResearchAreasBySaved, useElectionChoices } from "@voteapp/api-client";
import { votePowerBadgeClass } from "../lib/votePowerBadge";
import { APP_NAME } from "@voteapp/api-client";
import { useMe } from "@voteapp/api-client";
import { useMyResearchAreas } from "@voteapp/api-client";
import { aggregateRecordAreaStances, scoreStanceRelevance } from "@voteapp/api-client";
import { partyBucket } from "@voteapp/api-client";

// "alphabetical" is the payload's own order: the API sorts candidates by
// display name (there is no true ballot-position data). "my_issues" is the
// default for viewers with saved research areas.
type CandidateSort = "alphabetical" | "my_issues";

// The party filter over the candidates list. Order fixes the chip row;
// labels are plural because the chips answer "show me the …".
const PARTY_FILTER_OPTIONS: { bucket: PartyBucket; label: string }[] = [
  { bucket: "democratic", label: "Democrats" },
  { bucket: "republican", label: "Republicans" },
  { bucket: "other", label: "Other" },
];

// Catalog bucket names that are not real-world office titles — "State Lower
// Chamber Legislator is responsible for:" reads as internal jargon next to a
// page titled "State Representative". Real titles (Mayor, Sheriff, Governor)
// keep the personalized heading.
const GENERIC_OFFICE_NAMES = new Set([
  "State Lower Chamber Legislator",
  "State Level Judge",
  "County Level Judge",
  "Place Level Judge",
]);

function officeHeadingName(canonicalName: string): string {
  return GENERIC_OFFICE_NAMES.has(canonicalName) ? "This office" : canonicalName;
}

// Server loader: the election subject arrives in the document HTML so
// non-JS crawlers can read it. Anonymous by design — see loadFromApi.
export async function loader({ params, request }: LoaderFunctionArgs) {
  return loadFromApi<ElectionDetail>(`/api/elections/${params.electionId}`, request);
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
  return pageMeta({
    title: `${data.official_ballot_title} · ${APP_NAME}`,
    // No "campaign finance" here: this page stopped rendering finance
    // (it lives on candidate profiles now), and a search preview must not
    // promise content the page doesn't have.
    description: `${data.official_ballot_title} — ${formatDistrictName(data.district.name)} election on ${data.election_date}: candidates and issue research.`,
    path: location.pathname,
  });
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
  // null = no explicit pick; viewers with saved areas default to "my
  // issues first" (their picks are the point of saving areas), everyone
  // else to the alphabetical payload order. A picked "my_issues" is
  // ignored while saved areas are empty — same resilience as the record
  // view on CandidatePage — and honored again once areas are re-saved.
  const [chosenSort, setChosenSort] = useState<CandidateSort | null>(null);
  const effectiveChosenSort = chosenSort === "my_issues" && !hasSaved ? null : chosenSort;
  const candidateSort = effectiveChosenSort ?? (hasSaved ? "my_issues" : "alphabetical");
  // The pick carries the election it was made on: this component stays
  // mounted across param changes, and unlike the sort (a preference that
  // travels), a party filter is a per-race choice — carrying it into the
  // next election would silently hide candidates there. A pick from another
  // election reads as "all"; no effect needed, stale state is simply never
  // read.
  const [partyPick, setPartyPick] = useState<{ electionId: string; bucket: PartyBucket | "all" }>({
    electionId: "",
    bucket: "all",
  });
  // "Has a record on my issues" — same per-race keying as the party pick,
  // for the same reason: it hides candidates, so it must not travel to the
  // next election this mounted component renders.
  const [recordsPick, setRecordsPick] = useState<{ electionId: string; on: boolean }>({
    electionId: "",
    on: false,
  });

  const data = useLoaderData<typeof loader>();
  const chosenPartyFilter = partyPick.electionId === data.id ? partyPick.bucket : "all";
  // Data-driven visibility: the filter renders only when the roster spans
  // >= 2 buckets — a nonpartisan or one-party roster gets no filter because
  // it could not change anything. is_partisan is deliberately not consulted
  // (it can be null, and a partisan race whose roster is all one bucket
  // still has nothing to filter). The count guard mirrors the sort's
  // resilience: a picked bucket is ignored — not cleared — while the filter
  // is hidden.
  const partyCounts: Record<PartyBucket, number> = { democratic: 0, republican: 0, other: 0 };
  for (const candidate of data.candidates) {
    partyCounts[partyBucket(candidate.party)] += 1;
  }
  const presentPartyOptions = PARTY_FILTER_OPTIONS.filter((option) => partyCounts[option.bucket] > 0);
  const showPartyFilter = presentPartyOptions.length >= 2;
  const partyFilter =
    showPartyFilter && chosenPartyFilter !== "all" && partyCounts[chosenPartyFilter] > 0
      ? chosenPartyFilter
      : "all";
  const partyFilteredCandidates =
    partyFilter === "all"
      ? data.candidates
      : data.candidates.filter((candidate) => partyBucket(candidate.party) === partyFilter);
  // "Has a record on my issues": the exact relevance scoring the "my issues
  // first" sort uses — score > 0 means at least one stance-bearing record on
  // a saved area (relevance, not agreement). Applied after the party filter.
  // While the toggle is OFF it appears only when it could change the current
  // view: signed-in with saved areas, and the party-filtered set splits into
  // matched + unmatched. While ON it stays visible and keeps applying — even
  // when that empties the current party view ("N hidden · Show all" explains
  // the empty list) — because an active filter that silently stops applying
  // would show a full roster the viewer believes is filtered. Only a viewer
  // with no saved areas gets the pick ignored (the scoring is meaningless
  // without them), same as the sort.
  const chosenRecordsFilter = recordsPick.electionId === data.id ? recordsPick.on : false;
  const matchedOnMyIssues = partyFilteredCandidates.filter(
    (candidate) => scoreStanceRelevance(aggregateRecordAreaStances(candidate.records), weights).score > 0
  );
  const recordsFilterOn = hasSaved && chosenRecordsFilter;
  const showRecordsFilter =
    recordsFilterOn ||
    (hasSaved && matchedOnMyIssues.length > 0 && matchedOnMyIssues.length < partyFilteredCandidates.length);
  const visibleCandidates = recordsFilterOn ? matchedOnMyIssues : partyFilteredCandidates;
  const hiddenByRecordsFilter = partyFilteredCandidates.length - matchedOnMyIssues.length;
  const measure = data.ballot_measure;
  // "My choice" controls render only for logged-in viewers with a loaded
  // choices list (no-flash rule, like FollowButton) on upcoming elections —
  // the backend rejects choice writes to past ones.
  const { choiceByElectionId, canChoose } = useElectionChoices();
  const myChoice = choiceByElectionId?.get(data.id);
  const showChoiceControls =
    canChoose && choiceByElectionId !== undefined && data.election_date >= usLatestLocalDate();
  // Logged-out visitors get the same pick buttons, but clicking prompts them
  // to register (mirrors RegisterToFollowButton). me is undefined while the
  // session loads — render nothing then to avoid a flash of the wrong button.
  const showRegisterToPick = me === null && data.election_date >= usLatestLocalDate();
  // Per-candidate result badges (Won / Advanced / Lost / …); the matching
  // guards — roster-matched winners only, losers only under an exhaustive
  // winner set — live in deriveCandidateResultBadges.
  const resultBadges = deriveCandidateResultBadges(data.results, data.candidates);
  // Full set, uncapped — the list card previews these; the detail page is
  // where they all fit. Measure elections skip this row: the measure section
  // already shows the same areas with their for/against stance. The ??
  // fallbacks cover deploy skew — a not-yet-redeployed backend omits both
  // fields, which must degrade to "no section", not a crash.
  const office = data.office ?? null;
  const researchAreas = data.research_areas ?? [];
  const orderedAreas = splitResearchAreasBySaved(researchAreas, weights);
  const showOfficeInfo = data.race_type !== "ballot_measure" && (office !== null || researchAreas.length > 0);
  // The nav bar exists only for in-app arrivals: router state carries where
  // "back" goes and the ballot sequence. Deep links (shares, search
  // engines) have neither — they get no bar at all, by product choice.
  const location = useLocation();
  const navState = readElectionNavState(location.state);
  // Prev/next over the ballot sequence the visitor arrived with; null (back
  // slot only) on single-contest lists or when this election fell out of
  // the snapshot.
  const contestNeighbors = pagerNeighbors(navState?.contests, data.id);
  // Computed once, before render: the roster links hand the candidate page
  // this exact displayed order (sort + party + records filters applied), so
  // the JSX and the state payload must come from the same array.
  const orderedCandidates = sortCandidatesByStance(visibleCandidates, candidateSort, weights);
  const candidateNavState: CandidateNavState = {
    backTo: { path: `/elections/${data.id}`, label: data.official_ballot_title },
    // The election page's own incoming context rides along so the back hop
    // restores it (election → candidate → back keeps the ballot sequence).
    ...(navState ? { backState: navState } : {}),
    electionId: data.id,
    candidates: orderedCandidates.map(({ candidate }) => ({
      id: candidate.candidate_id,
      name: candidate.display_name,
    })),
  };

  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      {/* One nav bar at the top: prev | back | next, each slot captioned.
          backToState: when the back destination is a candidate page, restore
          its own context (the mirror of the roster links' backState). */}
      {navState ? (
        <DetailPager
          ariaLabel="Ballot navigation"
          prev={
            contestNeighbors?.prev
              ? { path: `/elections/${contestNeighbors.prev.id}`, label: contestNeighbors.prev.title }
              : null
          }
          next={
            contestNeighbors?.next
              ? { path: `/elections/${contestNeighbors.next.id}`, label: contestNeighbors.next.title }
              : null
          }
          backTo={navState.backTo}
          backToState={navState.backState}
          siblingState={navState}
        />
      ) : null}
      <JsonLdScript
        data={{
          "@type": "Event",
          name: data.official_ballot_title,
          startDate: data.election_date,
          location: { "@type": "AdministrativeArea", name: formatDistrictName(data.district.name) },
        }}
      />
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-2xl font-bold">{data.official_ballot_title}</h1>
        <ShareButton
          path={`/elections/${data.id}`}
          shareText={`${data.official_ballot_title} — ${formatElectionDate(data.election_date)}`}
        />
      </div>
      <p className="mt-1 text-sm text-ink-soft">
        {formatElectionDate(data.election_date)} · {formatDistrictName(data.district.name)} ·{" "}
        {formatDistrictType(data.district.district_type)}
        {data.election_stage ? <> · {data.election_stage}</> : null}
        {data.seats_to_fill != null && data.seats_to_fill > 1 ? <> · {data.seats_to_fill} seats</> : null}
      </p>
      <div className="mt-2 flex flex-wrap gap-2 text-xs">
        {data.vote_power.label !== "unknown" ? (
          <span className={`font-medium ${votePowerBadgeClass(data.vote_power.label)}`}>
            Vote impact: {formatVotePowerLabel(data.vote_power.label)}
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
            How do we calculate vote impact?
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

      {showOfficeInfo ? (
        // Description first, then what the election affects — what the office does,
        // then which issues it touches.
        <section className="mt-6 rounded-xl border border-line bg-white p-4">
          <h2 className="text-lg font-semibold">
            {office ? `${officeHeadingName(office.canonical_name)} is responsible for:` : "About this office"}
          </h2>
          {office ? (
            // The summary is seeded as newline-separated duty bullets
            // (seedOffices.ts); a legacy single-paragraph summary renders as
            // one bullet until the seed is re-run.
            <ul className="mt-2 list-disc space-y-1 pl-5 text-sm text-ink">
              {office.summary
                .split("\n")
                .filter((line) => line.trim() !== "")
                .map((line, i) => (
                  <li key={i}>{line}</li>
                ))}
            </ul>
          ) : null}
          {researchAreas.length > 0 ? (
            // Same one-list, comma-separated presentation as the ballot
            // cards: saved matches lead with a screen-reader-only "(saved)"
            // cue, position is the only sighted distinction.
            <p className="mt-3 text-xs">
              {/* Same verb label as the ballot cards — see ElectionCard. */}
              <span className="font-medium text-ink-soft">Affects:</span>{" "}
              {/* Comma separators live outside the spans as plain text
                  nodes, so each span's text stays exactly the area name. */}
              {[...orderedAreas.saved, ...orderedAreas.others].map((area, index, all) => (
                <Fragment key={area.id}>
                  <span className={AREA_TEXT_CLASS}>
                    {area.name}
                    {orderedAreas.saved.includes(area) ? <span className="sr-only"> (saved)</span> : null}
                  </span>
                  {index < all.length - 1 ? ", " : null}
                </Fragment>
              ))}
            </p>
          ) : null}
        </section>
      ) : null}

      {measure ? (
        <section className="mt-6 rounded-xl border border-line bg-white p-4">
          <h2 className="text-lg font-semibold text-dem-blue">Ballot Measure</h2>
          {measure.research_area_tags.length > 0 ? (
            // Comma-separated colored text, not boxed chips (boxes read as
            // buttons). Tags group by stance under a leading verb
            // ("Supports: X, Y" / "Opposes: Z") so the direction reads once
            // per group instead of as a "(for)" suffix on every name. Color
            // matches the YES/NO boxes below (green = supports, red =
            // opposes), but the verb carries the meaning — color alone would
            // be invisible to color-blind readers. Stanceless tags keep the
            // ballot cards' "Affects:" label and saved/muted styling, and
            // saved areas keep the sr-only cue used elsewhere.
            <div className="mt-2 space-y-1 text-xs">
              {(
                [
                  ["Supports:", "for", "font-medium text-green-900"],
                  ["Opposes:", "against", "font-medium text-red-900"],
                ] as const
              ).map(([label, stance, tagClass]) => {
                const tags = measure.research_area_tags.filter((tag) => tag.stance === stance);
                if (tags.length === 0) {
                  return null;
                }
                return (
                  <p key={stance}>
                    <span className="font-medium text-ink-soft">{label}</span>{" "}
                    {tags.map((tag, index, all) => (
                      <Fragment key={tag.research_area_id}>
                        <span className={tagClass}>
                          {tag.name}
                          {savedAreaIds.has(tag.research_area_id) ? <span className="sr-only"> (saved)</span> : null}
                        </span>
                        {index < all.length - 1 ? ", " : null}
                      </Fragment>
                    ))}
                  </p>
                );
              })}
              {measure.research_area_tags.some((tag) => tag.stance !== "for" && tag.stance !== "against") ? (
                <p>
                  <span className="font-medium text-ink-soft">Affects:</span>{" "}
                  {measure.research_area_tags
                    .filter((tag) => tag.stance !== "for" && tag.stance !== "against")
                    .map((tag, index, all) => (
                      <Fragment key={tag.research_area_id}>
                        <span
                          className={
                            savedAreaIds.has(tag.research_area_id) ? "font-medium text-green-900" : "text-ink-soft"
                          }
                        >
                          {tag.name}
                          {savedAreaIds.has(tag.research_area_id) ? <span className="sr-only"> (saved)</span> : null}
                        </span>
                        {index < all.length - 1 ? ", " : null}
                      </Fragment>
                    ))}
                </p>
              ) : null}
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
          {showChoiceControls ? (
            <div className="mt-3">
              <MeasureChoiceButtons electionId={data.id} choice={myChoice} />
            </div>
          ) : null}
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
            {showChoiceControls && data.seats_to_fill != null && data.seats_to_fill > 1 ? (
              <span className="text-xs text-ink-soft">
                This election fills {data.seats_to_fill} seats — pick up to {data.seats_to_fill} candidates.
              </span>
            ) : null}
            {hasSaved && data.candidates.length > 1 ? (
              <label className="flex items-center gap-2 text-sm text-ink-soft">
                Sort by
                <select
                  value={candidateSort}
                  onChange={(event) => setChosenSort(event.target.value as CandidateSort)}
                  className="rounded-md border border-line bg-white px-2 py-1.5 text-sm text-ink focus:border-ink focus:outline-none"
                >
                  <option value="my_issues">My issues first</option>
                  <option value="alphabetical">Alphabetical</option>
                </select>
              </label>
            ) : null}
          </div>
          {showPartyFilter ? (
            <div className="mt-3 flex flex-wrap gap-2" role="group" aria-label="Filter candidates by party">
              {[{ bucket: "all" as const, label: "All" }, ...presentPartyOptions].map((option) => (
                <button
                  key={option.bucket}
                  type="button"
                  onClick={() => setPartyPick({ electionId: data.id, bucket: option.bucket })}
                  aria-pressed={partyFilter === option.bucket}
                  className={`rounded-full border px-3 py-1 text-sm font-medium transition ${
                    partyFilter === option.bucket
                      ? "border-ink bg-ink text-white"
                      : "border-line bg-white text-ink hover:bg-surface"
                  }`}
                >
                  {option.bucket === "all"
                    ? `All (${data.candidates.length})`
                    : `${option.label} (${partyCounts[option.bucket]})`}
                </button>
              ))}
            </div>
          ) : null}
          {showRecordsFilter ? (
            <div className="mt-3 flex flex-wrap items-center gap-2">
              <button
                type="button"
                onClick={() => setRecordsPick({ electionId: data.id, on: !recordsFilterOn })}
                aria-pressed={recordsFilterOn}
                className={`rounded-full border px-3 py-1 text-sm font-medium transition ${
                  recordsFilterOn
                    ? "border-ink bg-ink text-white"
                    : "border-line bg-white text-ink hover:bg-surface"
                }`}
              >
                Has a record on my issues
              </button>
              {recordsFilterOn && hiddenByRecordsFilter > 0 ? (
                // The hidden count is always visible while the filter hides
                // anyone: no records ≠ no stances (rosters are unevenly
                // researched), so the filtered list must never look like the
                // full roster. At 0 hidden there is nothing concealed and
                // the pressed chip alone carries the state.
                <span className="text-xs text-ink-soft">
                  {hiddenByRecordsFilter} candidate{hiddenByRecordsFilter === 1 ? "" : "s"} hidden ·{" "}
                  <button
                    type="button"
                    onClick={() => setRecordsPick({ electionId: data.id, on: false })}
                    className="font-medium underline decoration-dotted underline-offset-2 hover:text-ink"
                  >
                    Show all
                  </button>
                </span>
              ) : null}
            </div>
          ) : null}
          <div className="mt-3 space-y-3">
            {orderedCandidates.map(({ candidate, stances }) => (
              // Whole-card click target via a stretched link: the name
              // Link's ::after overlays the wrapper. Campaign finance is
              // deliberately NOT rendered here — it lives on the candidate
              // profile page only. Following also happens there.
              <div
                key={candidate.candidate_id}
                // Faint tint at rest; hover matches the ballot cards — brand
                // border plus the name taking the link color (group-hover).
                className="group relative rounded-xl border border-line bg-surface/50 shadow-sm transition hover:border-rausch hover:shadow-md"
              >
                <div className="p-4">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    {/* The badge sits beside the heading, not inside it (an
                        in-heading badge fuses into the accessible name —
                        "Jordan VoterAdvanced"), and the wrapper is a div
                        because a heading is flow content, invalid in a span. */}
                    <div className="flex items-center gap-2">
                      <h3 className="font-semibold">
                        <Link
                          to={`/candidates/${candidate.candidate_id}`}
                          state={candidateNavState}
                          // rausch-deep, not -dark: AA contrast on the tinted card
                          // bg — see ElectionCard's title.
                          className="transition after:absolute after:inset-0 group-hover:text-rausch-deep"
                        >
                          {candidate.display_name}
                        </Link>
                      </h3>
                      {(() => {
                        const badge = resultBadges.get(candidate.candidate_id);
                        if (!badge) {
                          return null;
                        }
                        return (
                          <span
                            className={
                              badge.kind === "winner"
                                ? "rounded border border-green-700 bg-green-50 px-2 py-0.5 text-xs font-medium text-green-900"
                                : "rounded border border-line bg-surface px-2 py-0.5 text-xs font-medium text-ink-soft"
                            }
                          >
                            {badge.label}
                          </span>
                        );
                      })()}
                    </div>
                    {/* Withdrawn candidacies never reach this payload
                        (ballotLookup filters them), but the writer also
                        rejects withdrawn/lost — don't render a button whose
                        only outcome is an error. */}
                    {(showChoiceControls || showRegisterToPick) &&
                    candidate.status !== "withdrawn" &&
                    candidate.status !== "lost" ? (
                      // z-10 lifts the button above the card's stretched
                      // link so clicking it doesn't navigate.
                      <span className="relative z-10">
                        {showChoiceControls ? (
                          <CandidatePickButton
                            electionId={data.id}
                            candidateId={candidate.candidate_id}
                            candidateName={candidate.display_name}
                            choice={myChoice}
                            seatsToFill={data.seats_to_fill ?? null}
                            size="sm"
                          />
                        ) : (
                          <RegisterToPickButton candidateName={candidate.display_name} size="sm" />
                        )}
                      </span>
                    ) : null}
                  </div>
                  <p className="text-sm text-ink-soft">
                    {candidate.party}
                    {candidate.is_incumbent ? " · Incumbent" : ""}
                    {candidate.status !== "active" ? ` · ${candidate.status}` : ""}
                  </p>
                  {candidate.summary ? (
                    <p className="mt-2 line-clamp-3 text-sm text-ink">{candidate.summary}</p>
                  ) : null}
                  {stances.length > 0 ? (
                    // Comma-separated colored text, not boxed chips (boxes
                    // read as buttons). Stance direction colors the name:
                    // all-for green, all-against red, mixed amber —
                    // replacing the saved-area green, which said nothing
                    // about the candidate (saved areas keep their sr-only
                    // cue). Counts compress to +N/-N; screen readers get the
                    // spelled-out counts instead, since "-2" can be read as
                    // just "2". Every stance has for_count + against_count
                    // >= 1 — aggregateRecordAreaStances drops
                    // neutral/untagged records — so "against == 0" can only
                    // mean all-for.
                    <p className="mt-2 text-xs">
                      {/* Without a label the row was a bare "Housing
                          Affordability +1" — an issue name and a number with
                          nothing saying what was counted. "Records:" names
                          the source, matching the "Affects:" row on
                          the election cards. */}
                      <span className="font-medium text-ink-soft">Records:</span>{" "}
                      {stances.map((stance, index, all) => (
                        <Fragment key={stance.research_area_id}>
                          <span
                            className={
                              stance.against_count === 0
                                ? "font-medium text-green-900"
                                : stance.for_count === 0
                                  ? "font-medium text-red-900"
                                  : "font-medium text-amber-900"
                            }
                          >
                          {stance.name}{" "}
                          <span aria-hidden="true">
                            {[
                              stance.for_count > 0 ? `+${stance.for_count}` : null,
                              stance.against_count > 0 ? `-${stance.against_count}` : null,
                            ]
                              .filter(Boolean)
                              .join(" ")}
                          </span>
                          <span className="sr-only">
                            {[
                              stance.for_count > 0 ? `${stance.for_count} for` : null,
                              stance.against_count > 0 ? `${stance.against_count} against` : null,
                            ]
                              .filter(Boolean)
                              .join(", ")}
                          </span>
                            {savedAreaIds.has(stance.research_area_id) ? (
                              <span className="sr-only"> (saved)</span>
                            ) : null}
                          </span>
                          {index < all.length - 1 ? ", " : null}
                        </Fragment>
                      ))}
                    </p>
                  ) : null}
                </div>
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

      {/* Last on purpose: reporting is a reaction to reading the page, not a
          headline action worth space above the candidates. */}
      <div className="mt-6">
        <ReportContentButton
          entityType="election"
          entityId={data.id}
          contextLabel="election"
          reporterEmail={me?.email}
        />
      </div>
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

// Client-side "my issues first" candidate ordering: weighted unique matched
// areas dominate, matching record volume breaks ties, and candidates that
// tie completely (including all zero-scores) keep the payload's alphabetical
// order — the sort is stable over the original sequence. Relevance, not
// agreement: against-only records on a saved issue still count as a track
// record on it (scoreStanceRelevance), matching the direction-neutral label.
function sortCandidatesByStance(
  candidates: ElectionDetail["candidates"],
  sort: CandidateSort,
  weights: ReturnType<typeof useMyResearchAreas>["weights"]
): Array<{ candidate: ElectionDetail["candidates"][number]; stances: ReturnType<typeof aggregateRecordAreaStances> }> {
  const entries = candidates.map((candidate) => ({
    candidate,
    stances: aggregateRecordAreaStances(candidate.records),
  }));
  if (sort === "alphabetical") {
    return entries;
  }
  return entries
    .map((entry, index) => ({ entry, index, score: scoreStanceRelevance(entry.stances, weights) }))
    .sort(
      (a, b) =>
        b.score.score - a.score.score || b.score.recordCount - a.score.recordCount || a.index - b.index
    )
    .map(({ entry }) => entry);
}
