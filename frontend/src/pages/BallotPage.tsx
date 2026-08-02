import { Link, useLocation, useSearchParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { apiRequest } from "@voteapp/api-client";
import {
  PUBLIC_BALLOT_SORTS,
  type BallotSort,
  type BallotSummary,
} from "@voteapp/api-client";
import { ElectionList } from "../components/ElectionCard";
import { BallotFiltersControl } from "../components/BallotFiltersControl";
import { HowToVoteControl } from "../components/HowToVoteControl";
import { deriveBallotFilters, useElectionChoices, useMyResearchAreas } from "@voteapp/api-client";
import { useBallotFilterParams } from "../lib/useBallotFilterParams";
import { EmptyNotice, ErrorNotice, LoadingNotice } from "../components/Status";
import { useDocumentTitle } from "../lib/useDocumentTitle";

// Public page: only the sorts the anonymous endpoint can honor. A my_areas
// value (typed into the URL or copied from a signed-in session) falls back to
// vote_power so the subtitle never claims an ordering the backend cannot do.
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
  // Set by the home page's post-search navigation so the visitor can confirm
  // the geocoder matched the right address. Router state only — the address is
  // personal data and must stay out of the URL; a refresh or shared link
  // simply omits the confirmation line.
  const location = useLocation();
  const routerState = location.state as { matchedAddress?: unknown; addressMatchCount?: unknown } | null;
  const matchedAddress = typeof routerState?.matchedAddress === "string" ? routerState.matchedAddress : null;
  // The geocoder returned more than one candidate address and the ballot is
  // for the first one — the confirmation line alone is too easy to skim past.
  const ambiguousMatchCount =
    typeof routerState?.addressMatchCount === "number" && routerState.addressMatchCount > 1
      ? routerState.addressMatchCount
      : null;
  const [searchParams, setSearchParams] = useSearchParams();
  const districtIds = (searchParams.get("d") ?? "")
    .split(",")
    .map((id) => id.trim())
    .filter((id) => id.length > 0);
  const rawSort = searchParams.get("sort") ?? "";
  const sort: BallotSort = SORT_VALUES.includes(rawSort) ? (rawSort as BallotSort) : "vote_power";
  const { issuesRequested, impactRequested, onIssuesFilterChange, onImpactFilterChange, onShowAll } =
    useBallotFilterParams();

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

  const filtersView = deriveBallotFilters({
    elections: ballot.data?.elections ?? [],
    savedAreaIds,
    hasSaved,
    issuesRequested,
    impactRequested,
  });
  // A ?issues=mine load must not flash the full ballot while the saved
  // areas are still unknown (the ballot is one request; the saved areas are
  // two chained ones, so the ballot usually lands first). Withhold the list
  // until the flag settles — it settles on failure too, falling open to the
  // full list with the request ignored: a ballot app errs toward showing
  // races, and no on-page element claims filtering in that state.
  const awaitingSavedAreas = issuesRequested && savedAreasLoading;

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
      {/* No visible page heading: the date group headings ("Elections on …")
          carry the page's identity, so an "Elections" banner above them was
          redundant. The sr-only h1 keeps a level-1 target for screen-reader
          heading navigation — and gives the loading/error/empty states,
          which render before any date heading exists, a heading at all.
          "Elections", not "Upcoming elections": the list keeps just-finished
          elections for BALLOT_PAST_ELECTION_VISIBILITY_DAYS so their results
          stay discoverable, and those are not upcoming. */}
      <h1 className="sr-only">Elections</h1>
      {/* Filters and sorting on the left, the "How to vote" resources on the
          right — the disclosure panels each open inline under their own
          column. The how-to-vote control waits for the ballot response
          because that's where its state abbreviation(s) come from. */}
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="flex flex-wrap items-start gap-3">
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
              {PUBLIC_BALLOT_SORTS.map((option) => (
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
              elections={filtersView.visibleElections}
              savedAreaWeights={savedAreaWeights}
              choicesByElectionId={choiceByElectionId}
            />
          )}
        </>
      ) : null}
    </div>
  );
}

export default BallotPage;
