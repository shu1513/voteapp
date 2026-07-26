import type { ElectionSummary } from "@voteapp/api-client";
import {
  formatDistrictName,
  formatDistrictType,
  formatElectionDate,
  formatOutcome,
  formatRosterStatus,
  formatVotePowerLabel,
} from "@voteapp/api-client";
import { useRouter } from "expo-router";
import { Pressable, Text, View } from "react-native";

/**
 * Shared between the anonymous ballot and (later) the saved ballot.
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
  const router = useRouter();
  return (
    <Pressable
      onPress={() => router.push(`/elections/${election.id}`)}
      className="rounded-xl border border-line bg-white p-4 active:bg-surface"
      accessibilityRole="link"
    >
      <View className="flex-row items-start justify-between gap-3">
        <Text className="flex-1 font-semibold text-ink">{election.official_ballot_title}</Text>
        <Text className="shrink-0 text-sm text-ink-soft">{formatElectionDate(election.election_date)}</Text>
      </View>
      <Text className="mt-1 text-sm text-ink-soft">
        {formatDistrictName(election.district.name)} · {formatDistrictType(election.district.district_type)}
        {election.office ? <> · {election.office.canonical_name}</> : null}
      </Text>
      <View className="mt-2 flex-row flex-wrap items-center gap-2">
        {election.followed_candidates && election.followed_candidates.length > 0 ? (
          <Text className="rounded bg-rausch px-2 py-0.5 text-xs font-medium text-white">
            You follow {election.followed_candidates.map((candidate) => candidate.display_name).join(", ")}
          </Text>
        ) : null}
        {election.race_type === "ballot_measure" ? (
          <Text className="rounded bg-ink/10 px-2 py-0.5 text-xs text-ink">Ballot measure</Text>
        ) : (
          <Text className="rounded bg-surface px-2 py-0.5 text-xs text-ink-soft">
            {election.candidate_count === 0 && election.candidate_roster_status
              ? formatRosterStatus(election.candidate_roster_status).short
              : `${election.candidate_count} candidate${election.candidate_count === 1 ? "" : "s"}`}
          </Text>
        )}
        {election.vote_power.label !== "unknown" ? (
          <Text className="rounded bg-rausch/10 px-2 py-0.5 text-xs text-rausch-dark">
            Vote impact: {formatVotePowerLabel(election.vote_power.label)}
          </Text>
        ) : null}
        {election.historical_competitiveness ? (
          <Text className="rounded bg-surface px-2 py-0.5 text-xs text-ink-soft">
            {election.historical_competitiveness.display_label}
          </Text>
        ) : null}
        {election.has_results ? (
          <Text className="rounded bg-surface px-2 py-0.5 text-xs text-ink">
            {election.current_result_outcome
              ? `Result: ${formatOutcome(election.current_result_outcome)}`
              : "Results available"}
          </Text>
        ) : null}
        {election.research_areas.map((area) => (
          <Text
            key={area.id}
            className={
              savedAreaIds?.has(area.id)
                ? "rounded border border-rausch/40 bg-rausch/10 px-2 py-0.5 text-xs font-medium text-rausch-dark"
                : "rounded bg-surface px-2 py-0.5 text-xs text-ink-soft"
            }
          >
            {area.name}
          </Text>
        ))}
      </View>
    </Pressable>
  );
}
