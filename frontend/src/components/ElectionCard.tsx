import { Fragment } from "react";
import { Link } from "react-router";
import type { ElectionChoice, ElectionSummary, ResearchAreaWeight } from "@voteapp/api-client";
import type { BackTo, ElectionNavState } from "../lib/detailNavContext";
import {
  formatDistrictName,
  formatElectionDate,
  formatOutcome,
  formatRosterStatus,
  formatVotePowerLabel,
} from "@voteapp/api-client";
import { splitResearchAreasBySaved } from "../lib/researchAreaPriority";
import { usLatestLocalDate } from "../lib/usLatestLocalDate";
import { votePowerBadgeClass } from "../lib/votePowerBadge";

// "My pick: Jane Doe" / "My picks: Jane Doe, John Roe" (multi-seat) /
// "My pick: Yes" on a measure. First person throughout, because both chips
// render side by side in one ballot list and because these labels echo the
// controls that set them — MeasureChoiceButtons is headed "My pick:" and
// the candidate button reads "My pick" (ElectionChoiceControls.tsx). It also
// matches the rest of the signed-in surface ("My Elections", "My
// Candidates", "My issues first"). A pick whose candidate has since
// withdrawn gets flagged inline instead of vanishing.
function formatChoiceLabel(choice: ElectionChoice): string | null {
  if (choice.measure_position !== null) {
    return `My pick: ${choice.measure_position === "yes" ? "Yes" : "No"}`;
  }
  if (choice.picks.length === 0) {
    return null;
  }
  const names = choice.picks
    .map((pick) => (pick.candidacy_status === "withdrawn" ? `${pick.display_name} (withdrew)` : pick.display_name))
    .join(", ");
  return `${choice.picks.length === 1 ? "My pick" : "My picks"}: ${names}`;
}

// Statewide races carry a dozen-plus research areas; rendering every one
// buried the card's actual signal (title, candidates, vote power) under a
// wall of identical chips. The card is a preview — saved-area matches all
// show (they are the personal signal), other areas cap out and the election
// page carries the full set.
const MAX_UNSAVED_AREA_CHIPS = 3;

// Research areas render as plain colored text, comma-separated — NOT boxed
// chips. Boxed/pill styling is reserved for interactive elements; a bordered
// area "chip" read as a button and invited dead clicks. Saved and unsaved
// areas deliberately share one style: the row reads as one list, and
// position alone marks the saved matches (they lead). Exported so the
// election detail page's affects row matches the card's.
export const AREA_TEXT_CLASS = "font-medium text-green-900";

// An office race with no published candidate list renders a placeholder card
// ("Candidate list not final") with nothing to read. Ballot measures are
// exempt: zero candidates is their normal state, and the measure text is the
// content. A recorded result also exempts — winners can be recorded without
// candidate links, and a decided race is readable regardless of its roster.
// Mirrors hasNothingToRead in the backend's ballotElectionOrdering, which
// sinks these races to the end of the payload.
function isAwaitingCandidates(election: ElectionSummary): boolean {
  return election.race_type !== "ballot_measure" && election.candidate_count === 0 && !election.has_results;
}

/**
 * Date-grouped card list shared by both ballot pages. Elections cluster on
 * election days (a typical ballot is one or two dates), so the date renders
 * once as a group heading instead of being stamped on every card. Grouping
 * is by consecutive run — purely presentational — so it cannot reorder
 * whatever sort the page requested; a sort that interleaves dates just
 * produces more headings.
 *
 * Races still waiting on a candidate list render apart, under one closing
 * section instead of inside the date groups: the backend sinks them to the
 * end of the payload, and date-grouping that tail would repeat date headings
 * at the bottom of the list. Their cards carry their own date (their section
 * heading names no date), and they keep the payload's relative order.
 */
export function ElectionList({
  elections,
  savedAreaWeights,
  choicesByElectionId,
  backTo,
}: {
  elections: ElectionSummary[];
  /**
   * The session holder's saved research areas (useMyResearchAreas().weights):
   * membership decides which chips lead, rank decides their order.
   */
  savedAreaWeights?: Map<string, ResearchAreaWeight>;
  /**
   * The session holder's planned votes (useElectionChoices().choiceByElectionId).
   * Undefined while anonymous or still loading. Only elections present in the
   * map get a pick chip; a race without one shows nothing, deliberately — an
   * empty-state badge on every undecided race was more noise than signal.
   */
  choicesByElectionId?: Map<string, ElectionChoice>;
  /**
   * Where a detail page's back link should return (the calling page's own
   * URL including its query string, so sort and filters survive the round
   * trip). When set, every card hands the election page this destination
   * plus the ballot's displayed contest order via router state.
   */
  backTo?: BackTo;
}) {
  const awaitingCandidates = elections.filter(isAwaitingCandidates);
  const readable = elections.filter((election) => !isAwaitingCandidates(election));
  const groups: { date: string; elections: ElectionSummary[] }[] = [];
  for (const election of readable) {
    const lastGroup = groups[groups.length - 1];
    if (lastGroup && lastGroup.date === election.election_date) {
      lastGroup.elections.push(election);
    } else {
      groups.push({ date: election.election_date, elections: [election] });
    }
  }
  // Contest order = exactly what this list renders: readable races (their
  // grouping is presentational and keeps this order), then the awaiting
  // tail. Built here, not by the pages, so it can never drift from the DOM.
  const navState: ElectionNavState | undefined = backTo
    ? {
        backTo,
        contests: [...readable, ...awaitingCandidates].map((election) => ({
          id: election.id,
          title: election.official_ballot_title,
        })),
      }
    : undefined;
  return (
    <div className="mt-4 space-y-6">
      {groups.map((group) => (
        // The same date can head several runs under a date-interleaving
        // sort; the first election id makes the key unique.
        <section key={`${group.date}-${group.elections[0].id}`}>
          {/* The ballot pages carry no h1 banner; these date headings are the
              page's identity, so they read as full sentences and lead the
              visual hierarchy. */}
          <h2 className="text-xl font-bold text-ink">Elections on {formatElectionDate(group.date)}</h2>
          <div className="mt-2 space-y-3">
            {group.elections.map((election) => (
              <ElectionCard
                key={election.id}
                election={election}
                savedAreaWeights={savedAreaWeights}
                myChoice={choicesByElectionId?.get(election.id)}
                navState={navState}
              />
            ))}
          </div>
        </section>
      ))}
      {awaitingCandidates.length > 0 ? (
        <section>
          {/* Neutral about WHO the wait is on: this section spans every
              zero-candidate reason, and roster_processing means the list is
              published and this app is still preparing profiles — "waiting
              on officials" would misplace that blame. Matches the generic
              roster-status copy. Leads with "Elections" to parallel the
              "Elections on {date}" headings above it. */}
          <h2 className="text-xl font-bold text-ink">Elections awaiting candidate information</h2>
          <div className="mt-2 space-y-3">
            {awaitingCandidates.map((election) => (
              <ElectionCard
                key={election.id}
                election={election}
                savedAreaWeights={savedAreaWeights}
                myChoice={choicesByElectionId?.get(election.id)}
                navState={navState}
                showDate
              />
            ))}
          </div>
        </section>
      ) : null}
    </div>
  );
}

/**
 * A single election card. Deliberately NOT exported: it omits its own date
 * (ElectionList's group heading carries it), so a standalone render would be
 * dateless. Render elections through ElectionList, which is the public API
 * and is shared between the anonymous and saved ballots. savedAreaWeights
 * (verified users with saved research areas) puts the matching area chips
 * first so "affects what I care about" reads at a glance.
 */
function ElectionCard({
  election,
  savedAreaWeights,
  myChoice,
  navState,
  showDate = false,
}: {
  election: ElectionSummary;
  savedAreaWeights?: Map<string, ResearchAreaWeight>;
  /** The viewer's planned vote for this election, when they have one. */
  myChoice?: ElectionChoice;
  /** ElectionList's nav context (back destination + contest order),
   * delivered to the election page via the card link's router state. */
  navState?: ElectionNavState;
  /**
   * The "Elections awaiting candidate information" section spans dates under
   * one heading, so its cards must say their own date; everywhere else the
   * group heading carries it.
   */
  showDate?: boolean;
}) {
  // Saved matches lead (in the user's rank order), unsaved follow in public-
  // salience order — see splitResearchAreasBySaved. The chips that survive
  // the cap are the areas voters care about most.
  const { saved: savedAreas, others: otherAreas } = splitResearchAreasBySaved(
    election.research_areas,
    savedAreaWeights
  );
  const visibleOtherAreas = otherAreas.slice(0, MAX_UNSAVED_AREA_CHIPS);
  const hiddenAreaCount = otherAreas.length - visibleOtherAreas.length;
  // The viewer's planned vote, shown only on upcoming races: a past
  // election's choice is history. Withdrawn picks stay visible with a flag —
  // a silent disappearance would read as data loss. Races WITHOUT a pick show
  // nothing: an empty-state badge on every undecided race read as noise, and
  // the absence of a green chip already marks them.
  const isUpcoming = election.election_date >= usLatestLocalDate();
  const choiceLabel = myChoice && isUpcoming ? formatChoiceLabel(myChoice) : null;
  // Skip an empty chip row so the card doesn't carry stray spacing when a
  // race has no signals to show.
  const hasSignalChips =
    (election.followed_candidates?.length ?? 0) > 0 ||
    election.historical_competitiveness !== null ||
    election.has_results ||
    choiceLabel !== null;
  return (
    <Link
      to={`/elections/${election.id}`}
      state={navState}
      // Faint tint at rest; on hover the border goes brand and the title
      // takes the link color (via group-hover below). The old cue — gray bg
      // one step grayer — was under 2% lightness and read as nothing.
      className="group block rounded-xl border border-line bg-surface/50 p-4 shadow-sm transition hover:border-rausch hover:shadow-md"
    >
      {/* No per-card date: ElectionList's group heading carries it. The title
          row keeps vote power and the candidate count flush right, so every
          card answers "how much does my vote matter, and who's running?" on
          its first line. */}
      <div className="flex flex-wrap items-baseline justify-between gap-x-3 gap-y-1">
        {/* rausch-deep, not -dark: 16px semibold needs 4.5:1 on the card's
            tinted bg, and rausch-dark is 4.41:1 there. */}
        <h3 className="font-semibold text-ink transition group-hover:text-rausch-deep">
          {election.official_ballot_title}
        </h3>
        {/* The group wraps between chip and count on very narrow screens;
            nowrap sits on each label so neither breaks mid-phrase. */}
        <span className="flex flex-wrap items-baseline justify-end gap-x-2 gap-y-1">
          {election.vote_power.label !== "unknown" ? (
            // Colored text, not a pill: the tinted badge read as a button.
            <span
              className={`whitespace-nowrap text-xs font-medium ${votePowerBadgeClass(election.vote_power.label)}`}
            >
              Vote impact: {formatVotePowerLabel(election.vote_power.label)}
            </span>
          ) : null}
          {election.race_type === "ballot_measure" ? (
            <span className="whitespace-nowrap text-sm text-dem-blue">Ballot Measure</span>
          ) : (
            <span className="whitespace-nowrap text-sm text-ink-soft">
              {election.candidate_count === 0 && election.candidate_roster_status
                ? formatRosterStatus(election.candidate_roster_status).short
                : `${election.candidate_count} candidate${election.candidate_count === 1 ? "" : "s"}`}
            </span>
          )}
        </span>
      </div>
      {/* Always show the district: ballot titles are often generic ("Mayor",
          "Governor", "State Representative"), and the district name is what
          tells the voter WHERE the race is. */}
      <p className="mt-0.5 text-sm text-ink-soft">
        {formatDistrictName(election.district.name)}
        {showDate ? <> · {formatElectionDate(election.election_date)}</> : null}
      </p>
      {hasSignalChips ? (
        <div className="mt-2 flex flex-wrap items-center gap-2 text-xs">
          {choiceLabel ? (
            // Leads the chip row: the voter's own decision outranks the
            // other signals. Bordered green, distinct from the solid
            // followed-candidates chip.
            <span className="rounded border border-green-700 bg-green-50 px-2 py-0.5 font-medium text-green-900">
              {choiceLabel}
            </span>
          ) : null}
          {election.followed_candidates && election.followed_candidates.length > 0 ? (
            <span className="rounded bg-green-600 px-2 py-0.5 font-medium text-white">
              {election.followed_candidates.map((candidate) => candidate.display_name).join(", ")}{" "}
              {election.followed_candidates.length === 1 ? "is" : "are"} running
            </span>
          ) : null}
          {election.historical_competitiveness ? (
            <span className="rounded bg-surface px-2 py-0.5 text-ink-soft">
              {election.historical_competitiveness.display_label}
            </span>
          ) : null}
          {election.has_results ? (
            <span className="rounded bg-surface px-2 py-0.5 text-ink">
              {election.current_result_outcome
                ? `Result: ${formatOutcome(election.current_result_outcome)}`
                : "Results available"}
            </span>
          ) : null}
        </div>
      ) : null}
      {election.research_areas.length > 0 ? (
        // Visually one comma-separated list: saved matches lead (all of
        // them, in the user's rank order), unsaved follow under the cap.
        // Position is the only sighted cue, so saved areas carry a
        // screen-reader-only "(saved)" to keep the distinction audible.
        <p className="mt-3 text-sm">
          {/* A verb, not a noun phrase: the election is the subject, so the
              row reads "this election affects these things". A noun label
              ("Key issues") left it ambiguous whether the topics were the
              race's subject matter or what it changes. */}
          <span className="font-medium text-ink-soft">Affects:</span>{" "}
          {/* Comma separators live OUTSIDE the area spans as plain text
              nodes, so each span's text stays exactly the area name. */}
          {[...savedAreas, ...visibleOtherAreas].map((area, index, all) => (
            <Fragment key={area.id}>
              <span className={AREA_TEXT_CLASS}>
                {area.name}
                {savedAreas.includes(area) ? <span className="sr-only"> (saved)</span> : null}
              </span>
              {index < all.length - 1 || hiddenAreaCount > 0 ? ", " : null}
            </Fragment>
          ))}
          {hiddenAreaCount > 0 ? (
            // "issues", matching the row's own label. Same green as the
            // issue names: the overflow count is part of the same list.
            <span className={AREA_TEXT_CLASS}>
              +{hiddenAreaCount} more issue{hiddenAreaCount === 1 ? "" : "s"}
            </span>
          ) : null}
        </p>
      ) : null}
    </Link>
  );
}
