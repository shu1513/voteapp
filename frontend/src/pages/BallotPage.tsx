import { useEffect } from "react";
import { Link, useLocation, useSearchParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { apiRequest, useMe } from "@voteapp/api-client";
import {
  BALLOT_SORTS,
  PUBLIC_BALLOT_SORTS,
  type BallotSort,
  type BallotSummary,
  type ElectionSummary,
} from "@voteapp/api-client";
import { ElectionList } from "../components/ElectionCard";
import { BallotFiltersControl } from "../components/BallotFiltersControl";
import { RaceTypeTabs } from "../components/RaceTypeTabs";
import { HowToVoteControl } from "../components/HowToVoteControl";
import {
  deriveBallotFilters,
  railSortForBallotSort,
  sortRailEntries,
  useElectionChoices,
  useMyResearchAreas,
} from "@voteapp/api-client";
import { draftChoicesByElectionId, setDraftBallotContext, useBallotDraft } from "../lib/ballotDraft";
import { useBallotFilterParams } from "../lib/useBallotFilterParams";
import { EmptyNotice, ErrorNotice, LoadingNotice } from "../components/Status";
import { useDocumentTitle } from "../lib/useDocumentTitle";
import { useHydrated } from "../lib/useHydrated";
import { usLatestLocalDate } from "../lib/usLatestLocalDate";

// The draft's progress denominator: the nearest upcoming election day's
// races (all of them, matching PickDateCard's "N of M races decided" count).
// Computed from the FULL payload, not the filtered view — hiding races with
// a filter must not shrink the goal.
function nearestUpcomingTarget(
  elections: ElectionSummary[]
): { election_date: string; election_ids: string[] } | null {
  const today = usLatestLocalDate();
  const upcoming = elections.filter((election) => election.election_date >= today);
  if (upcoming.length === 0) {
    return null;
  }
  const date = upcoming.reduce(
    (min, election) => (election.election_date < min ? election.election_date : min),
    upcoming[0].election_date
  );
  return {
    election_date: date,
    election_ids: upcoming.filter((election) => election.election_date === date).map((election) => election.id),
  };
}

// The anonymous endpoint can honor these server-side. my_areas is offered on
// top of them to signed-in visitors with saved research areas, honored by a
// CLIENT-side re-sort (sortRailEntries — the same mirror of the backend
// comparator the detail rail uses), because /api/ballot has no user to score
// against. For anonymous visitors a my_areas URL value falls back to
// vote_power so the UI never claims an ordering nobody computed.
const SORT_VALUES: readonly string[] = PUBLIC_BALLOT_SORTS.map((option) => option.value);

export function BallotPage() {
  useDocumentTitle("Elections");
  // Signed-in verified visitors get their saved areas listed first (in their
  // own rank order) even on the public ballot; anonymous visitors get an
  // empty map (no personalization). The same saved areas gate the "Only my
  // issues" filter, so a verified visitor's one-off search is filterable too.
  const {
    weights: savedAreaWeights,
    savedAreaIds,
    hasSaved,
    isLoading: savedAreasLoading,
  } = useMyResearchAreas();
  const { choiceByElectionId } = useElectionChoices();
  // Guests read their picks from the local ballot draft instead of the
  // account endpoint — the same chips render from either source.
  const { me } = useMe();
  const isGuest = me === null;
  const draft = useBallotDraft();
  // Set by the home page's post-search navigation so the visitor can confirm
  // the geocoder matched the right address. Router state only — the address is
  // personal data and must stay out of the URL; a shared link omits the
  // confirmation line. A refresh does NOT: the browser restores router state
  // from history.state, which is exactly why it must read as null until
  // hydration — SSR rendered this document without it.
  const location = useLocation();
  const hydrated = useHydrated();
  const routerState = hydrated
    ? (location.state as { matchedAddress?: unknown; addressMatchCount?: unknown } | null)
    : null;
  const matchedAddress = typeof routerState?.matchedAddress === "string" ? routerState.matchedAddress : null;
  // The geocoder returned more than one candidate address and the ballot is
  // for the first one — the confirmation line alone is too easy to skim past.
  const ambiguousMatchCount =
    typeof routerState?.addressMatchCount === "number" && routerState.addressMatchCount > 1
      ? routerState.addressMatchCount
      : null;
  const [searchParams, setSearchParams] = useSearchParams();
  // Set by ZIP searches; deliberately a URL param, not router state — it
  // holds no location, and the partial label must survive refresh and
  // shared links.
  const isPartialBallot = searchParams.get("partial") === "1";
  const districtIds = (searchParams.get("d") ?? "")
    .split(",")
    .map((id) => id.trim())
    .filter((id) => id.length > 0);
  const rawSort = searchParams.get("sort") ?? "";
  // my_areas is only real once the viewer's saved areas confirm (hasSaved);
  // until then — and for anonymous visitors forever — it degrades to the
  // vote_power default. The list is withheld while that's still unsettled
  // (awaitingSavedAreas below) so the degraded order never flashes.
  const myAreasRequested = rawSort === "my_areas";
  const sort: BallotSort = myAreasRequested
    ? hasSaved
      ? "my_areas"
      : "vote_power"
    : SORT_VALUES.includes(rawSort)
      ? (rawSort as BallotSort)
      : "vote_power";
  // What the anonymous endpoint is asked for: my_areas is client-side here,
  // so its fetch requests (and caches under) the plain vote_power payload.
  const fetchSort: BallotSort = sort === "my_areas" ? "vote_power" : sort;
  const {
    issuesRequested,
    impactRequested,
    raceTypeRequested,
    onIssuesFilterChange,
    onImpactFilterChange,
    onRaceTypeChange,
    onShowAll,
  } = useBallotFilterParams();

  const ballot = useQuery({
    queryKey: ["ballot", districtIds.join(","), fetchSort],
    queryFn: () =>
      apiRequest<BallotSummary>(
        `/api/ballot?district_ids=${encodeURIComponent(districtIds.join(","))}&sort=${fetchSort}`
      ),
    enabled: districtIds.length > 0,
  });

  // Keep the guest draft's badge link and progress denominator tracking the
  // ballot the guest actually looked at last. Signed-in visitors never touch
  // the draft here — theirs lives in the account.
  const ballotElections = ballot.data?.elections;
  useEffect(() => {
    if (!isGuest || !ballotElections) {
      return;
    }
    setDraftBallotContext(districtIds, nearestUpcomingTarget(ballotElections));
    // districtIds is rebuilt each render; its joined string is the stable key.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isGuest, ballotElections, districtIds.join(",")]);

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

  const filtersView = deriveBallotFilters({
    elections: ballot.data?.elections ?? [],
    savedAreaIds,
    hasSaved,
    issuesRequested,
    impactRequested,
    raceTypeRequested,
  });
  // A ?issues=mine (or ?sort=my_areas) load must not flash the full/unsorted
  // ballot while the saved areas are still unknown (the ballot is one
  // request; the saved areas are two chained ones, so the ballot usually
  // lands first). Withhold the list until the flag settles — it settles on
  // failure too, falling open to the full list with the request ignored: a
  // ballot app errs toward showing races, and no on-page element claims
  // filtering or an issue ordering in that state.
  const awaitingSavedAreas = (issuesRequested || myAreasRequested) && savedAreasLoading;

  // The my_areas re-sort. Runs over the tab/filter-visible list only; the
  // awaiting-candidates tail needs no special handling because ElectionList
  // splits it into its own closing section regardless of input order (the
  // backend's sink + compare produces the same outcome). The wrapper objects
  // adapt ElectionSummary's field names to the shared comparator's keys.
  const visibleElections =
    sort === "my_areas"
      ? sortRailEntries(
          filtersView.visibleElections.map((election) => ({
            id: election.id,
            title: election.official_ballot_title,
            race_type: election.race_type,
            vote_power_score: election.vote_power.score,
            election_date: election.election_date,
            research_area_ids: election.research_areas.map((area) => area.id),
            election,
          })),
          "my_areas",
          savedAreaWeights
        ).map((entry) => entry.election)
      : filtersView.visibleElections;

  // my_areas rides on top of the public sorts, only for viewers who can be
  // scored against (saved research areas confirmed).
  const sortOptions = hasSaved ? BALLOT_SORTS : PUBLIC_BALLOT_SORTS;

  if (districtIds.length === 0) {
    return (
      <div className="mx-auto max-w-3xl px-4 py-10">
        <h1 className="sr-only">Elections</h1>
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
      {/* Visible page heading, one step larger than the date group headings
          ("Elections on …") below it, so a first-time visitor landing here
          straight from the address form knows what the list is: THEIR
          elections. "My elections", not "Upcoming elections": the list keeps
          just-finished elections for BALLOT_PAST_ELECTION_VISIBILITY_DAYS so
          their results stay discoverable, and those are not upcoming. */}
      <h1 className="mb-4 text-2xl font-bold text-ink">My elections:</h1>
      {/* Filters and sorting on the left, the "How to vote" resources on the
          right — the disclosure panels each open inline under their own
          column. The how-to-vote control waits for the ballot response
          because that's where its state abbreviation(s) come from. */}
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="flex flex-wrap items-start gap-3">
          {/* Offered only when the ballot mixes candidate races and ballot
              measures — a single-type ballot has nothing to switch between. */}
          {filtersView.showRaceTypeTabs ? (
            <RaceTypeTabs raceType={filtersView.raceType} onChange={onRaceTypeChange} />
          ) : null}
          <BallotFiltersControl
            showIssues={filtersView.showIssuesFilter}
            issuesOn={filtersView.issuesOn}
            onIssuesChange={onIssuesFilterChange}
            showImpactHigh={filtersView.showImpactHigh}
            showImpactMedium={filtersView.showImpactMedium}
            impactLevel={filtersView.impactLevel}
            onImpactChange={onImpactFilterChange}
            activeFilterCount={filtersView.activeFilterCount}
            hiddenCount={filtersView.hiddenCount}
            onShowAll={onShowAll}
          />
          <label className="flex items-center gap-2 text-sm text-ink-soft">
            Sort by
            <select
              value={sort}
              onChange={(event) => onSortChange(event.target.value)}
              className="rounded-md border border-line bg-white px-2 py-1.5 text-sm text-ink focus:border-ink focus:outline-none"
            >
              {sortOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
        </div>
        {ballot.isSuccess ? (
          <HowToVoteControl states={ballot.data.districts.map((district) => district.state)} />
        ) : null}
      </div>

      {/* The always-on "Matched address" confirmation line was dropped as
          clutter; the warning below is self-contained (it names the matched
          address) and only appears when the geocoder was ambiguous — the one
          case where the ballot has a real chance of being for the wrong
          address. */}
      {/* ZIP searches land here with partial=1 in the URL (the flag carries
          no location, so unlike the matched address it survives refreshes
          and shared links). The ZIP itself rides router state as the
          matched address; a bare link renders the generic wording. */}
      {isPartialBallot ? (
        <p role="alert" className="mt-2 rounded-md border border-line bg-surface px-3 py-2 text-sm text-ink">
          This is a partial ballot{matchedAddress ? (
            <>
              {" "}for ZIP code <span className="font-medium">{matchedAddress}</span>
            </>
          ) : (
            " from a ZIP code search"
          )}
          : it lists only the races every address in the ZIP shares.{" "}
          <Link to="/?new=1" className="underline hover:text-rausch">
            Enter your street address
          </Link>{" "}
          to check for additional congressional, legislative, local, and school races.
        </p>
      ) : null}

      {matchedAddress && ambiguousMatchCount ? (
        <p role="alert" className="mt-2 rounded-md border border-line bg-surface px-3 py-2 text-sm text-ink">
          Your search matched {ambiguousMatchCount} possible addresses, and this ballot is for{" "}
          <span className="font-medium">{matchedAddress}</span>. If that is not your address,{" "}
          <Link to="/?new=1" className="underline hover:text-rausch">
            search again
          </Link>{" "}
          with your full street address, city, and ZIP code.
        </p>
      ) : null}

      {/* A ballot error wins over the saved-areas withhold: with no list to
          withhold, gating on the filter would pair the loading notice with
          the error (or hide the error outright). */}
      {ballot.isPending || (awaitingSavedAreas && !ballot.isError) ? (
        <LoadingNotice text="Loading your elections…" />
      ) : null}
      {ballot.isError ? (
        <div className="mt-4">
          <ErrorNotice error={ballot.error} />
        </div>
      ) : null}

      {ballot.isSuccess && !awaitingSavedAreas ? (
        <>
          {ballot.data.elections.length === 0 ? (
            <EmptyNotice text="No upcoming elections found for these districts yet. Check back — new elections are added as they are announced." />
          ) : (
            // An active filter can empty this list; the "N elections hidden ·
            // Show all" line in the controls row explains the empty view.
            <ElectionList
              elections={visibleElections}
              savedAreaWeights={savedAreaWeights}
              choicesByElectionId={isGuest ? draftChoicesByElectionId(draft) : choiceByElectionId}
              // Full query string: the back link must return to this exact
              // list — same districts, sort, and filters.
              backTo={{ path: location.pathname + location.search, label: "All elections" }}
              // Tab-unsliced pool + the engaged tab: the detail rail's own
              // race-type tabs start here and can reach the other tab's races.
              contestsPool={filtersView.filteredElections}
              raceType={filtersView.raceType}
              // Seed the rail's always-engaged sort from this list's sort.
              railSort={railSortForBallotSort(sort)}
            />
          )}
        </>
      ) : null}
    </div>
  );
}

export default BallotPage;
