import { Link } from "react-router";
import type { ElectionSummary, ResearchAreaWeight } from "@voteapp/api-client";
import {
  formatElectionDate,
  formatOutcome,
  formatRosterStatus,
  formatVotePowerLabel,
} from "@voteapp/api-client";
import { splitResearchAreasBySaved } from "../lib/researchAreaPriority";
import { votePowerBadgeClass } from "../lib/votePowerBadge";

// Statewide races carry a dozen-plus research areas; rendering every one
// buried the card's actual signal (title, candidates, vote power) under a
// wall of identical chips. The card is a preview — saved-area matches all
// show (they are the personal signal), other areas cap out and the election
// page carries the full set.
const MAX_UNSAVED_AREA_CHIPS = 3;

// Saved and unsaved chips deliberately share one style: the row reads as one
// list, and position alone marks the saved matches (they lead). Exported so
// the election detail page's affected-areas row matches the card's.
export const AREA_CHIP_CLASS =
  "rounded border border-green-600/40 bg-green-600/10 px-2 py-0.5 font-medium text-green-900";

/**
 * Date-grouped card list shared by both ballot pages. Elections cluster on
 * election days (a typical ballot is one or two dates), so the date renders
 * once as a group heading instead of being stamped on every card. Grouping
 * is by consecutive run — purely presentational — so it cannot reorder
 * whatever sort the page requested; a sort that interleaves dates just
 * produces more headings.
 */
export function ElectionList({
  elections,
  savedAreaWeights,
}: {
  elections: ElectionSummary[];
  /**
   * The session holder's saved research areas (useMyResearchAreas().weights):
   * membership decides which chips lead, rank decides their order.
   */
  savedAreaWeights?: Map<string, ResearchAreaWeight>;
}) {
  const groups: { date: string; elections: ElectionSummary[] }[] = [];
  for (const election of elections) {
    const lastGroup = groups[groups.length - 1];
    if (lastGroup && lastGroup.date === election.election_date) {
      lastGroup.elections.push(election);
    } else {
      groups.push({ date: election.election_date, elections: [election] });
    }
  }
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
              <ElectionCard key={election.id} election={election} savedAreaWeights={savedAreaWeights} />
            ))}
          </div>
        </section>
      ))}
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
}: {
  election: ElectionSummary;
  savedAreaWeights?: Map<string, ResearchAreaWeight>;
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
  // Skip an empty chip row so the card doesn't carry stray spacing when a
  // race has no signals to show.
  const hasSignalChips =
    (election.followed_candidates?.length ?? 0) > 0 ||
    election.historical_competitiveness !== null ||
    election.has_results;
  return (
    <Link
      to={`/elections/${election.id}`}
      className="block rounded-xl border border-line bg-white p-4 shadow-sm transition hover:shadow-md"
    >
      {/* No per-card date: ElectionList's group heading carries it. The title
          row keeps vote power and the candidate count flush right, so every
          card answers "how much does my vote matter, and who's running?" on
          its first line. */}
      <div className="flex flex-wrap items-baseline justify-between gap-x-3 gap-y-1">
        <h3 className="font-semibold text-ink">{election.official_ballot_title}</h3>
        {/* The group wraps between chip and count on very narrow screens;
            nowrap sits on each label so neither breaks mid-phrase. */}
        <span className="flex flex-wrap items-baseline justify-end gap-x-2 gap-y-1">
          {election.vote_power.label !== "unknown" ? (
            <span
              className={`whitespace-nowrap rounded px-2 py-0.5 text-xs ${votePowerBadgeClass(election.vote_power.label)}`}
            >
              Vote power: {formatVotePowerLabel(election.vote_power.label)}
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
      <p className="mt-0.5 text-sm text-ink-soft">{election.district.name}</p>
      {hasSignalChips ? (
        <div className="mt-2 flex flex-wrap items-center gap-2 text-xs">
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
        // Visually one list: saved matches lead (all of them, in the user's
        // rank order), unsaved follow under the cap. Position is the only
        // sighted cue, so saved chips carry a screen-reader-only "(saved)"
        // to keep the distinction audible.
        <div className="mt-3 flex flex-wrap items-center gap-2 text-xs">
          {/* Quiet label: the chips carry the color; a boxed/tinted label
              competed with them for attention. */}
          <span className="font-medium text-ink-soft">Affected Areas:</span>
          {savedAreas.map((area) => (
            <span key={area.id} className={AREA_CHIP_CLASS}>
              {area.name}
              <span className="sr-only"> (saved)</span>
            </span>
          ))}
          {visibleOtherAreas.map((area) => (
            <span key={area.id} className={AREA_CHIP_CLASS}>
              {area.name}
            </span>
          ))}
          {hiddenAreaCount > 0 ? (
            // "areas", matching the row's own label — not "issues". Same green
            // as the area chips: the overflow count is part of the same list.
            <span className={AREA_CHIP_CLASS}>
              +{hiddenAreaCount} more area{hiddenAreaCount === 1 ? "" : "s"}
            </span>
          ) : null}
        </div>
      ) : null}
    </Link>
  );
}
