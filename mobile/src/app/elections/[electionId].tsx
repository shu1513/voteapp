import type { ElectionDetail } from "@voteapp/api-client";
import {
  aggregateRecordAreaStances,
  ApiError,
  apiRequest,
  formatDistrictType,
  formatElectionDate,
  formatMoney,
  formatOutcome,
  formatVotePowerLabel,
  scoreStanceDirection,
  useMyResearchAreas,
} from "@voteapp/api-client";
import { useQuery } from "@tanstack/react-query";
import { Stack, useLocalSearchParams, useRouter } from "expo-router";
import { useState } from "react";
import { Pressable, ScrollView, Text, View } from "react-native";
import { AiBanner } from "../../components/AiBanner";
import { NotFoundNotice } from "../../components/NotFoundNotice";
import { SortChips } from "../../components/SortChips";
import { SourceLine } from "../../components/SourceLine";
import { ErrorNotice, LoadingNotice } from "../../components/Status";
import { openExternalUrl } from "../../lib/openExternalUrl";

type CandidateSort = "ballot" | "for_mine" | "against_mine";

const CANDIDATE_SORT_OPTIONS = [
  { value: "ballot", label: "Ballot order" },
  { value: "for_mine", label: "For my issues" },
  { value: "against_mine", label: "Against my issues" },
] as const;

/**
 * Port of the web ElectionPage. The web's SSR loader becomes a plain
 * useQuery (no SEO concern on mobile); follow/report controls arrive with
 * the auth chunk.
 */
export default function ElectionScreen() {
  const { electionId } = useLocalSearchParams<{ electionId: string }>();
  const { savedAreaIds, weights, hasSaved } = useMyResearchAreas();
  const [candidateSort, setCandidateSort] = useState<CandidateSort>("ballot");

  const election = useQuery({
    queryKey: ["election", electionId],
    queryFn: () => apiRequest<ElectionDetail>(`/api/elections/${electionId}`),
    enabled: typeof electionId === "string" && electionId.length > 0,
    retry: false,
  });

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
      <AiBanner />
      <Text className="text-2xl font-bold text-ink">{data.official_ballot_title}</Text>
      <Text className="mt-1 text-sm text-ink-soft">
        {formatElectionDate(data.election_date)} · {data.district.name} ·{" "}
        {formatDistrictType(data.district.district_type)}
        {data.election_stage ? <> · {data.election_stage}</> : null}
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
              className="mt-3 text-sm font-medium text-rausch-dark underline"
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
                onChange={setCandidateSort}
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
              />
            ))}
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
}: {
  candidate: ElectionDetail["candidates"][number];
  stances: ReturnType<typeof aggregateRecordAreaStances>;
  savedAreaIds: Set<string>;
}) {
  const router = useRouter();
  return (
    <Pressable
      onPress={() => router.push(`/candidates/${candidate.candidate_id}`)}
      className="rounded-xl border border-line bg-white p-4 active:bg-surface"
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
        {candidate.finance_summary?.direct_campaign.total_raised != null ? (
          <Text className="shrink-0 text-sm text-ink-soft">
            Raised {formatMoney(candidate.finance_summary.direct_campaign.total_raised)}
          </Text>
        ) : null}
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
  );
}

function isPdfUrl(url: string): boolean {
  return /\.pdf($|[?#])/i.test(url);
}

// "Official" is a claim, not a style: only government-hosted links get the
// official label; anything else keeps neutral wording.
function isGovernmentUrl(url: string): boolean {
  try {
    const host = new URL(url).hostname.toLowerCase();
    return host.endsWith(".gov") || host.endsWith(".us");
  } catch {
    return false;
  }
}

// Client-side "for/against my issues" candidate ordering: weighted unique
// matched areas dominate, matching record volume breaks ties, and candidates
// that tie completely keep their ballot order (stable sort over the
// payload's original sequence). Same logic as the web ElectionPage.
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
  if (sort === "ballot") {
    return entries;
  }
  const direction = sort === "for_mine" ? ("for" as const) : ("against" as const);
  return entries
    .map((entry, index) => ({ entry, index, score: scoreStanceDirection(entry.stances, weights, direction) }))
    .sort(
      (a, b) =>
        b.score.score - a.score.score || b.score.recordCount - a.score.recordCount || a.index - b.index
    )
    .map(({ entry }) => entry);
}
