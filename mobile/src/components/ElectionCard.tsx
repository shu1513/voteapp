import type { ElectionChoice, ElectionSummary } from "@voteapp/api-client";
import {
  formatChoiceLabel,
  formatDistrictName,
  formatDistrictType,
  formatElectionDate,
  formatResultChipLabel,
  formatRosterStatus,
  formatVotePowerLabel,
  resultChipTone,
  sortByResearchAreaPriority,
} from "@voteapp/api-client";
import type { ResultChipTone } from "@voteapp/api-client";
import { useRouter } from "expo-router";
import { Pressable, Text, View } from "react-native";
import { usLatestLocalDate } from "../lib/usLatestLocalDate";

// Same green/red as the election page's candidate result badges — one color
// language for "called" across surfaces. Mirrors the web ElectionCard.
const RESULT_CHIP_CLASSES: Record<ResultChipTone, string> = {
  positive: "rounded border border-green-700 bg-green-50 px-2 py-0.5 text-xs font-medium text-green-900",
  negative: "rounded border border-red-700 bg-red-50 px-2 py-0.5 text-xs font-medium text-red-900",
  neutral: "rounded bg-surface px-2 py-0.5 text-xs text-ink",
};

/**
 * Shared between the anonymous ballot and (later) the saved ballot.
 * savedAreaIds (verified users with saved research areas) highlights the
 * matching area chips so "affects what I care about" reads at a glance.
 */
export function ElectionCard({
  election,
  savedAreaIds,
  myChoice,
}: {
  election: ElectionSummary;
  savedAreaIds?: Set<string>;
  /** The viewer's planned vote for this election, when they have one. */
  myChoice?: ElectionChoice;
}) {
  const router = useRouter();
  // The viewer's planned vote, shown only on upcoming races: a past
  // election's choice is history. Races WITHOUT a pick show nothing — the
  // absence of a green chip already marks them. Same rule as the web card.
  const isUpcoming = election.election_date >= usLatestLocalDate();
  const choiceLabel = myChoice && isUpcoming ? formatChoiceLabel(myChoice) : null;
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
        {choiceLabel ? (
          // Leads the chip row: the voter's own decision outranks the other
          // signals. A "No" measure pick renders red to match the measure
          // screen's "A NO vote means" box — a green "My pick: No" read as
          // a contradiction. Same styling as the web card's chip.
          <Text
            className={
              myChoice?.measure_position === "no"
                ? "rounded border border-red-700 bg-red-50 px-2 py-0.5 text-xs font-medium text-red-900"
                : "rounded border border-green-700 bg-green-50 px-2 py-0.5 text-xs font-medium text-green-900"
            }
          >
            {choiceLabel}
          </Text>
        ) : null}
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
            My vote power: {formatVotePowerLabel(election.vote_power.label)}
          </Text>
        ) : null}
        {/* Current-cycle rating replaces the historic chip when present —
            the backend only sends it when the rating drove the grade, and
            showing both would contradict on races that flipped. */}
        {election.current_competitiveness ? (
          <Text className="rounded bg-surface px-2 py-0.5 text-xs text-ink-soft">
            {election.current_competitiveness.display_label}
          </Text>
        ) : election.historical_competitiveness ? (
          <Text className="rounded bg-surface px-2 py-0.5 text-xs text-ink-soft">
            {election.historical_competitiveness.display_label}
          </Text>
        ) : null}
        {election.has_results ? (
          // Called results get the badge colors from the election page
          // (green = decided forward, red = failed) so the answer stands out
          // from the neutral info chips; undecided rows stay neutral so
          // color always means "called".
          <Text className={RESULT_CHIP_CLASSES[resultChipTone(election.current_result_outcome)]}>
            {election.current_result_outcome
              ? formatResultChipLabel(election.current_result_outcome, election.current_result_winners ?? [])
              : "Results available"}
          </Text>
        ) : null}
        {sortByResearchAreaPriority(election.research_areas).map((area) => (
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
