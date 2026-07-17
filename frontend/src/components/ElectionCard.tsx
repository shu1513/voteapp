import { Link } from "react-router";
import type { ElectionSummary } from "@voteapp/api-client";
import {
  formatDistrictType,
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
 * Shared between the anonymous ballot and the saved (account) ballot.
 * savedAreaIds (verified users with saved research areas) highlights the
 * matching area chips so "affects what I care about" reads at a glance.
 */
export function ElectionCard({
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
  return (
    <Link
      to={`/elections/${election.id}`}
      className="block rounded-xl border border-line bg-white p-4 shadow-sm transition hover:shadow-md"
    >
      {/* No per-card date: ElectionList's group heading carries it. */}
      <h3 className="font-semibold text-ink">{election.official_ballot_title}</h3>
      <p className="mt-1 text-sm text-ink-soft">
        {election.district.name} · {formatDistrictType(election.district.district_type)}
        {election.office ? <> · {election.office.canonical_name}</> : null}
      </p>
      <div className="mt-2 flex flex-wrap items-center gap-2 text-xs">
        {election.followed_candidates && election.followed_candidates.length > 0 ? (
          <span className="rounded bg-rausch px-2 py-0.5 font-medium text-white">
            You follow {election.followed_candidates.map((candidate) => candidate.display_name).join(", ")}
          </span>
        ) : null}
        {election.race_type === "ballot_measure" ? (
          <span className="rounded bg-ink/10 px-2 py-0.5 text-ink">Ballot measure</span>
        ) : (
          <span className="rounded bg-surface px-2 py-0.5 text-ink-soft">
            {election.candidate_count === 0 && election.candidate_roster_status
              ? formatRosterStatus(election.candidate_roster_status).short
              : `${election.candidate_count} candidate${election.candidate_count === 1 ? "" : "s"}`}
          </span>
        )}
        {election.vote_power.label !== "unknown" ? (
          <span className="rounded bg-rausch/10 px-2 py-0.5 text-rausch-dark">
            Vote power: {formatVotePowerLabel(election.vote_power.label)}
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
          <span className="rounded bg-surface px-2 py-0.5 text-ink-soft">
            +{hiddenAreaCount} more issue{hiddenAreaCount === 1 ? "" : "s"}
          </span>
        ) : null}
      </div>
    </Link>
  );
}
