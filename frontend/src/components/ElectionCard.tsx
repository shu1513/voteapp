import { Link } from "react-router";
import type { ElectionSummary } from "@voteapp/api-client";
import {
  formatElectionDate,
  formatOutcome,
  formatRosterStatus,
  formatVotePowerLabel,
} from "@voteapp/api-client";

// Statewide races carry a dozen-plus research areas; rendering every one
// buried the card's actual signal (title, candidates, vote power) under a
// wall of identical chips. The card is a preview — saved-area matches all
// show (they are the personal signal), other areas cap out and the election
// page carries the full set.
const MAX_UNSAVED_AREA_CHIPS = 3;

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
  savedAreaIds,
}: {
  elections: ElectionSummary[];
  savedAreaIds?: Set<string>;
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
          <h2 className="text-sm font-semibold text-ink">{formatElectionDate(group.date)}</h2>
          <div className="mt-2 space-y-3">
            {group.elections.map((election) => (
              <ElectionCard key={election.id} election={election} savedAreaIds={savedAreaIds} />
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
 * and is shared between the anonymous and saved ballots. savedAreaIds
 * (verified users with saved research areas) highlights the matching area
 * chips so "affects what I care about" reads at a glance.
 */
function ElectionCard({
  election,
  savedAreaIds,
}: {
  election: ElectionSummary;
  savedAreaIds?: Set<string>;
}) {
  const savedAreas = election.research_areas.filter((area) => savedAreaIds?.has(area.id) ?? false);
  const otherAreas = election.research_areas.filter((area) => !(savedAreaIds?.has(area.id) ?? false));
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
      {/* No per-card date: ElectionList's group heading carries it. No
          district/office meta either — the ballot title already names the
          race, and the election page carries the full detail. The title row
          keeps vote power and the candidate count flush right, so every card
          answers "how much does my vote matter, and who's running?" on its
          first line. */}
      <div className="flex flex-wrap items-baseline justify-between gap-x-3 gap-y-1">
        <h3 className="font-semibold text-ink">{election.official_ballot_title}</h3>
        <span className="flex items-baseline gap-2 whitespace-nowrap">
          {election.vote_power.label !== "unknown" ? (
            <span className="rounded bg-rausch/10 px-2 py-0.5 text-xs text-rausch-dark">
              Vote power: {formatVotePowerLabel(election.vote_power.label)}
            </span>
          ) : null}
          <span className="text-sm text-ink-soft">
            {election.race_type === "ballot_measure"
              ? "Ballot measure"
              : election.candidate_count === 0 && election.candidate_roster_status
                ? formatRosterStatus(election.candidate_roster_status).short
                : `${election.candidate_count} candidate${election.candidate_count === 1 ? "" : "s"}`}
          </span>
        </span>
      </div>
      {hasSignalChips ? (
        <div className="mt-2 flex flex-wrap items-center gap-2 text-xs">
          {election.followed_candidates && election.followed_candidates.length > 0 ? (
            <span className="rounded bg-rausch px-2 py-0.5 font-medium text-white">
              You follow {election.followed_candidates.map((candidate) => candidate.display_name).join(", ")}
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
        // Saved-area matches lead the list (all of them, highlighted);
        // unsaved areas follow under the cap.
        <div className="mt-3 flex flex-wrap items-center gap-2 border-t border-line pt-3 text-xs">
          <span className="text-ink-soft">Affected areas:</span>
          {savedAreas.map((area) => (
            <span
              key={area.id}
              className="rounded border border-rausch/40 bg-rausch/10 px-2 py-0.5 font-medium text-rausch-dark"
            >
              {area.name}
            </span>
          ))}
          {visibleOtherAreas.map((area) => (
            <span key={area.id} className="rounded bg-surface px-2 py-0.5 text-ink-soft">
              {area.name}
            </span>
          ))}
          {hiddenAreaCount > 0 ? (
            // "areas", matching the row's own label — not "issues".
            <span className="rounded bg-surface px-2 py-0.5 text-ink-soft">
              +{hiddenAreaCount} more area{hiddenAreaCount === 1 ? "" : "s"}
            </span>
          ) : null}
        </div>
      ) : null}
    </Link>
  );
}
