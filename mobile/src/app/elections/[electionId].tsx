import type { ElectionDetail } from "@voteapp/api-client";
import {
  aggregateRecordAreaStances,
  ApiError,
  apiRequest,
  formatDistrictType,
  formatElectionDate,
  formatMoney,
  formatOutcome,
  formatRosterStatus,
  formatVotePowerLabel,
  hasFinanceContent,
  scoreStanceDirection,
  useFollows,
  useMyResearchAreas,
} from "@voteapp/api-client";
import { useQuery } from "@tanstack/react-query";
import { Stack, useLocalSearchParams, useRouter } from "expo-router";
import { useState } from "react";
import { Pressable, ScrollView, Text, View } from "react-native";
import { FinanceSummaryCard } from "../../components/FinanceSummaryCard";
import { FollowButton } from "../../components/FollowButton";
import { NotFoundNotice } from "../../components/NotFoundNotice";
import { SortChips } from "../../components/SortChips";
import { SourceLine } from "../../components/SourceLine";
import { ErrorNotice, LoadingNotice } from "../../components/Status";
import { openExternalUrl } from "../../lib/openExternalUrl";

// "alphabetical" is the payload's own order: the API sorts candidates by
// display name (there is no true ballot-position data). "my_issues" is the
// default for viewers with saved research areas. Same as the web.
type CandidateSort = "alphabetical" | "my_issues";

const CANDIDATE_SORT_OPTIONS = [
  { value: "my_issues", label: "My issues first" },
  { value: "alphabetical", label: "Alphabetical" },
] as const;

/**
 * Port of the web ElectionPage. The web's SSR loader becomes a plain
 * useQuery (no SEO concern on mobile); follow/report controls arrive with
 * the auth chunk.
 */
export default function ElectionScreen() {
  const { electionId } = useLocalSearchParams<{ electionId: string }>();
  const { savedAreaIds, weights, hasSaved } = useMyResearchAreas();
  // Election payload candidates carry no follow state; derive it from the
  // follows list (only fetched for verified users). Controls render only
  // once that list has loaded. Same as the web.
  const { follows, canFollow } = useFollows();
  const followedIds = new Set((follows ?? []).map((follow) => follow.candidate_id));
  // null = no explicit pick; viewers with saved areas default to "my
  // issues first", everyone else to the alphabetical payload order. A
  // picked "my_issues" is ignored while saved areas are empty and honored
  // again once areas are re-saved. Same as the web.
  const [chosenSort, setChosenSort] = useState<CandidateSort | null>(null);
  const effectiveChosenSort = chosenSort === "my_issues" && !hasSaved ? null : chosenSort;
  const candidateSort = effectiveChosenSort ?? (hasSaved ? "my_issues" : "alphabetical");

  const election = useQuery({
    queryKey: ["election", electionId],
    queryFn: () => apiRequest<ElectionDetail>(`/api/elections/${electionId}`),
    enabled: typeof electionId === "string" && electionId.length > 0,
    retry: false,
  });

  // A missing/empty id disables the query but leaves isPending true forever;
  // without this guard a malformed deep link traps the user on the spinner.
  if (typeof electionId !== "string" || electionId.length === 0) {
    return (
      <View className="flex-1 bg-white px-4 py-8">
        <Stack.Screen options={{ title: "Election" }} />
        <NotFoundNotice subject="Election" />
      </View>
    );
  }

  if (election.isPending) {
    return (
      <View className="flex-1 bg-white">
        <Stack.Screen options={{ title: "Election" }} />
        <LoadingNotice text="Loading election…" />
      </View>
    );
  }

  if (election.isError) {
    const notFound = election.error instanceof ApiError && election.error.status === 404;
    return (
      <View className="flex-1 bg-white px-4 py-8">
        <Stack.Screen options={{ title: "Election" }} />
        {notFound ? <NotFoundNotice subject="Election" /> : <ErrorNotice error={election.error} />}
      </View>
    );
  }

  const data = election.data;
  const measure = data.ballot_measure;

  return (
    <ScrollView className="flex-1 bg-white" contentContainerClassName="px-4 py-8">
      <Stack.Screen options={{ title: data.official_ballot_title }} />
      <Text className="text-2xl font-bold text-ink">{data.official_ballot_title}</Text>
      <Text className="mt-1 text-sm text-ink-soft">
        {formatElectionDate(data.election_date)} · {data.district.name} ·{" "}
        {formatDistrictType(data.district.district_type)}
        {data.election_stage ? <> · {data.election_stage}</> : null}
        {data.seats_to_fill != null && data.seats_to_fill > 1 ? <> · {data.seats_to_fill} seats</> : null}
      </Text>
      <View className="mt-2 flex-row flex-wrap gap-2">
        {data.vote_power.label !== "unknown" ? (
          <Text className="rounded bg-rausch/10 px-2 py-0.5 text-xs text-rausch-dark">
            Vote power: {formatVotePowerLabel(data.vote_power.label)}
          </Text>
        ) : null}
        {data.historical_competitiveness ? (
          <Text className="rounded bg-surface px-2 py-0.5 text-xs text-ink-soft">
            {data.historical_competitiveness.display_label}
          </Text>
        ) : null}
      </View>

      {measure ? (
        <View className="mt-6 rounded-xl border border-line bg-white p-4">
          <Text className="text-lg font-semibold text-ink">Ballot measure</Text>
          {measure.research_area_tags.length > 0 ? (
            <View className="mt-2 flex-row flex-wrap gap-2">
              {measure.research_area_tags.map((tag) => (
                <Text
                  key={tag.research_area_id}
                  className={
                    savedAreaIds.has(tag.research_area_id)
                      ? "rounded border border-rausch/40 bg-rausch/10 px-2 py-0.5 text-xs font-medium text-rausch-dark"
                      : "rounded bg-surface px-2 py-0.5 text-xs text-ink-soft"
                  }
                >
                  {tag.name}
                </Text>
              ))}
            </View>
          ) : null}
          {measure.summary ? <Text className="mt-2 text-sm text-ink">{measure.summary}</Text> : null}
          <View className="mt-3 gap-3">
            <View className="rounded border border-green-200 bg-green-50 p-3">
              <Text className="text-sm font-semibold text-green-900">A YES vote means</Text>
              <Text className="mt-1 text-sm text-green-900">{measure.what_yes_means}</Text>
            </View>
            <View className="rounded border border-red-200 bg-red-50 p-3">
              <Text className="text-sm font-semibold text-red-900">A NO vote means</Text>
              <Text className="mt-1 text-sm text-red-900">{measure.what_no_means}</Text>
            </View>
          </View>
          {measure.result ? (
            <Text className="mt-3 text-sm font-medium text-ink">
              Result:{" "}
              <Text className={measure.result === "passed" ? "text-green-700" : "text-red-700"}>
                {measure.result}
              </Text>
            </Text>
          ) : null}
          {measure.official_measure_url ? (
            <Text
              className="mt-3 text-sm font-medium text-rausch-dark underline" accessibilityRole="link"
              onPress={() => openExternalUrl(measure.official_measure_url as string)}
            >
              {isGovernmentUrl(measure.official_measure_url)
                ? `Read the official measure text${isPdfUrl(measure.official_measure_url) ? " (PDF)" : ""}`
                : "More about this measure"}
            </Text>
          ) : null}
          {measure.source_urls
            .filter((url) => url !== measure.official_measure_url)
            .map((url) => (
              <SourceLine key={url} url={url} />
            ))}
        </View>
      ) : null}

      {data.candidates.length > 0 ? (
        <View className="mt-6">
          <Text className="text-lg font-semibold text-ink">Candidates</Text>
          {hasSaved && data.candidates.length > 1 ? (
            <View className="mt-2">
              <SortChips
                options={CANDIDATE_SORT_OPTIONS}
                value={candidateSort}
                onChange={setChosenSort}
              />
            </View>
          ) : null}
          <View className="mt-3 gap-3">
            {sortCandidatesByStance(data.candidates, candidateSort, weights).map(({ candidate, stances }) => (
              <CandidateCard
                key={candidate.candidate_id}
                candidate={candidate}
                stances={stances}
                savedAreaIds={savedAreaIds}
                followButton={
                  canFollow && follows ? (
                    <FollowButton
                      candidateId={candidate.candidate_id}
                      isFollowing={followedIds.has(candidate.candidate_id)}
                      size="sm"
                    />
                  ) : null
                }
              />
            ))}
          </View>
        </View>
      ) : data.candidate_roster_status ? (
        // Empty office roster: say WHY instead of hiding the section (roster
        // awaiting certification, profiles being prepared, or unavailable).
        <View className="mt-6">
          <Text className="text-lg font-semibold text-ink">Candidates</Text>
          <View className="mt-3 rounded-xl border border-line bg-white p-4">
            <Text className="text-sm text-ink-soft">{formatRosterStatus(data.candidate_roster_status).long}</Text>
          </View>
        </View>
      ) : null}

      {data.results.length > 0 ? (
        <View className="mt-6 rounded-xl border border-line bg-white p-4">
          <Text className="text-lg font-semibold text-ink">Results</Text>
          <Text className="mt-1 text-xs text-ink-soft">
            Unofficial until certified by the relevant election authority.
          </Text>
          <View className="mt-2 gap-3">
            {data.results.map((result) => (
              <View key={result.id}>
                <Text className="text-sm text-ink">
                  <Text className="font-medium">{formatOutcome(result.outcome)}</Text>
                  {result.result_status ? (
                    <Text className="text-ink-soft"> · {formatOutcome(result.result_status)}</Text>
                  ) : null}
                </Text>
                {result.winners.length > 0 ? (
                  <Text className="text-sm text-ink-soft">
                    Winner{result.winners.length === 1 ? "" : "s"}:{" "}
                    {result.winners
                      .map((winner) =>
                        winner.party
                          ? `${winner.candidate_name ?? "Unknown"} (${winner.party})`
                          : (winner.candidate_name ?? "Unknown")
                      )
                      .join(", ")}
                  </Text>
                ) : null}
                <SourceLine url={result.source_url} researchedDate={result.retrieved_at.slice(0, 10)} />
              </View>
            ))}
          </View>
        </View>
      ) : null}

      {data.sources.length > 0 ? (
        <View className="mt-6">
          <Text className="text-sm font-semibold text-ink">Election sources</Text>
          {data.sources.map((url) => (
            <SourceLine key={url} url={url} />
          ))}
        </View>
      ) : null}
    </ScrollView>
  );
}

function CandidateCard({
  candidate,
  stances,
  savedAreaIds,
  followButton,
}: {
  candidate: ElectionDetail["candidates"][number];
  stances: ReturnType<typeof aggregateRecordAreaStances>;
  savedAreaIds: Set<string>;
  followButton?: React.ReactNode;
}) {
  const router = useRouter();
  const [financeOpen, setFinanceOpen] = useState(false);
  return (
    <View className="rounded-xl border border-line bg-white">
      <Pressable
        onPress={() => router.push(`/candidates/${candidate.candidate_id}`)}
        className="p-4 active:bg-surface"
        accessibilityRole="link"
      >
        <View className="flex-row items-start justify-between gap-3">
          <View className="flex-1">
            <Text className="font-semibold text-ink">{candidate.display_name}</Text>
            <Text className="text-sm text-ink-soft">
              {candidate.party}
              {candidate.is_incumbent ? " · Incumbent" : ""}
              {candidate.status !== "active" ? ` · ${candidate.status}` : ""}
            </Text>
          </View>
          <View className="shrink-0 items-end gap-2">
            {candidate.finance_summary?.direct_campaign.total_raised != null ? (
              <Text className="text-sm text-ink-soft">
                Raised {formatMoney(candidate.finance_summary.direct_campaign.total_raised)}
              </Text>
            ) : null}
            {/* Nested pressable: the follow tap wins over the card's
                navigation, mirroring the web's stopPropagation wrapper. */}
            {followButton}
          </View>
        </View>
        {candidate.summary ? (
          <Text className="mt-2 text-sm text-ink" numberOfLines={3}>
            {candidate.summary}
          </Text>
        ) : null}
        {stances.length > 0 ? (
          <View className="mt-2 flex-row flex-wrap gap-2">
            {stances.map((stance) => (
              <Text
                key={stance.research_area_id}
                className={
                  savedAreaIds.has(stance.research_area_id)
                    ? "rounded border border-rausch/40 bg-rausch/10 px-2 py-0.5 text-xs font-medium text-rausch-dark"
                    : "rounded bg-surface px-2 py-0.5 text-xs text-ink-soft"
                }
              >
                {stance.name} ·{" "}
                {[
                  stance.for_count > 0 ? `${stance.for_count} for` : null,
                  stance.against_count > 0 ? `${stance.against_count} against` : null,
                ]
                  .filter(Boolean)
                  .join(", ")}
              </Text>
            ))}
          </View>
        ) : null}
      </Pressable>
      {hasFinanceContent(candidate.finance_summary) ? (
        // Same per-card disclosure as the web page's <details>; a separate
        // pressable so opening finance never triggers the card's navigation.
        <View className="border-t border-line px-4 py-3">
          <Pressable
            onPress={() => setFinanceOpen((current) => !current)}
            accessibilityRole="button"
            accessibilityState={{ expanded: financeOpen }}
            accessibilityLabel={`Campaign finance for ${candidate.display_name}`}
          >
            {/* The web gets a free disclosure triangle from <details>;
                mirror that affordance for sighted users. */}
            <Text className="text-sm font-medium text-ink">
              {financeOpen ? "▾" : "▸"} Campaign finance
            </Text>
          </Pressable>
          {financeOpen ? (
            <View className="mt-2">
              <FinanceSummaryCard summary={candidate.finance_summary} />
            </View>
          ) : null}
        </View>
      ) : null}
    </View>
  );
}

function isPdfUrl(url: string): boolean {
  return /\.pdf($|[?#])/i.test(url);
}

// "Official" is a claim, not a style: only .gov links get the official
// label; anything else keeps neutral wording. .us is deliberately excluded —
// it is an open registry (individuals and businesses register ordinary .us
// domains), so it is not evidence of government hosting.
function isGovernmentUrl(url: string): boolean {
  try {
    return new URL(url).hostname.toLowerCase().endsWith(".gov");
  } catch {
    return false;
  }
}

// Client-side "my issues first" candidate ordering: weighted unique matched
// areas dominate, matching record volume breaks ties, and candidates that
// tie completely keep the payload's alphabetical order (stable sort over
// the original sequence). Same logic as the web ElectionPage.
function sortCandidatesByStance(
  candidates: ElectionDetail["candidates"],
  sort: CandidateSort,
  weights: ReturnType<typeof useMyResearchAreas>["weights"]
): {
  candidate: ElectionDetail["candidates"][number];
  stances: ReturnType<typeof aggregateRecordAreaStances>;
}[] {
  const entries = candidates.map((candidate) => ({
    candidate,
    stances: aggregateRecordAreaStances(candidate.records),
  }));
  if (sort === "alphabetical") {
    return entries;
  }
  return entries
    .map((entry, index) => ({ entry, index, score: scoreStanceDirection(entry.stances, weights, "for") }))
    .sort(
      (a, b) =>
        b.score.score - a.score.score || b.score.recordCount - a.score.recordCount || a.index - b.index
    )
    .map(({ entry }) => entry);
}
