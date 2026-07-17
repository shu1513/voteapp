import { Link } from "react-router";
import type { ElectionSummary } from "@voteapp/api-client";
import {
  formatDistrictType,
  formatElectionDate,
  formatOutcome,
  formatRosterStatus,
  formatVotePowerLabel,
} from "@voteapp/api-client";

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
  return (
    <Link
      to={`/elections/${election.id}`}
      className="block rounded-xl border border-line bg-white p-4 shadow-sm transition hover:shadow-md"
    >
      <div className="flex items-start justify-between gap-3">
        <h3 className="font-semibold text-ink">{election.official_ballot_title}</h3>
        <span className="shrink-0 text-sm text-ink-soft">{formatElectionDate(election.election_date)}</span>
      </div>
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
        {election.research_areas.map((area) =>
          savedAreaIds?.has(area.id) ? (
            <span
              key={area.id}
              className="rounded border border-rausch/40 bg-rausch/10 px-2 py-0.5 font-medium text-rausch-dark"
            >
              {area.name}
            </span>
          ) : (
            <span key={area.id} className="rounded bg-surface px-2 py-0.5 text-ink-soft">
              {area.name}
            </span>
          )
        )}
      </div>
    </Link>
  );
}
