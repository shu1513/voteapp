import type { CandidateDetail, CandidateRecord, ResearchAreaPreference } from "@voteapp/api-client";
import {
  ApiError,
  apiRequest,
  formatElectionDate,
  UNRANKED_RESEARCH_AREA_RANK,
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

type RecordView = "by_issue" | "my_issues" | "newest";

type RecordGroup = {
  /** null for the untagged "Other records" pseudo-group. */
  areaId: string | null;
  areaName: string;
  records: CandidateRecord[];
};

// Records grouped by research area (a record with several tags appears under
// each; untagged records fall into "Other records"). Same logic as the web
// CandidatePage.
function groupRecords(records: CandidateRecord[]): RecordGroup[] {
  const groups = new Map<string | null, RecordGroup>();
  for (const record of records) {
    const areas = record.research_area_tags.length
      ? record.research_area_tags.map((tag) => ({ areaId: tag.research_area_id, areaName: tag.name }))
      : [{ areaId: null, areaName: "Other records" }];
    for (const area of areas) {
      const group = groups.get(area.areaId) ?? { ...area, records: [] };
      group.records.push(record);
      groups.set(area.areaId, group);
    }
  }
  return [...groups.values()].sort((a, b) =>
    a.areaId === null ? 1 : b.areaId === null ? -1 : a.areaName.localeCompare(b.areaName)
  );
}

// "My issues first": saved-area groups move to the front ordered by the
// user's rank (unranked saved areas after ranked ones), everything else
// keeps the alphabetical order groupRecords produced.
function orderGroupsByPreference(
  groups: RecordGroup[],
  preferences: readonly ResearchAreaPreference[]
): RecordGroup[] {
  const rankByAreaId = new Map(
    preferences.map((preference) => [preference.research_area_id, preference.rank ?? UNRANKED_RESEARCH_AREA_RANK])
  );
  return groups
    .map((group, index) => ({
      group,
      index,
      rank: (group.areaId !== null ? rankByAreaId.get(group.areaId) : undefined) ?? Number.POSITIVE_INFINITY,
    }))
    .sort((a, b) => a.rank - b.rank || a.index - b.index)
    .map(({ group }) => group);
}

/**
 * Port of the web CandidatePage. SSR loader becomes plain useQuery;
 * follow/report controls arrive with the auth chunk.
 */
export default function CandidateScreen() {
  const { candidateId } = useLocalSearchParams<{ candidateId: string }>();
  const { hasSaved, preferences } = useMyResearchAreas();
  const [recordView, setRecordView] = useState<RecordView>("by_issue");

  const detail = useQuery({
    queryKey: ["candidate", candidateId],
    queryFn: () => apiRequest<CandidateDetail>(`/api/candidates/${candidateId}`),
    enabled: typeof candidateId === "string" && candidateId.length > 0,
    retry: false,
  });

  // A missing/empty id disables the query but leaves isPending true forever;
  // without this guard a malformed deep link traps the user on the spinner.
  if (typeof candidateId !== "string" || candidateId.length === 0) {
    return (
      <View className="flex-1 bg-white px-4 py-8">
        <Stack.Screen options={{ title: "Candidate" }} />
        <NotFoundNotice subject="Candidate" />
      </View>
    );
  }

  if (detail.isPending) {
    return (
      <View className="flex-1 bg-white">
        <Stack.Screen options={{ title: "Candidate" }} />
        <LoadingNotice text="Loading candidate…" />
      </View>
    );
  }

  if (detail.isError) {
    const notFound = detail.error instanceof ApiError && detail.error.status === 404;
    return (
      <View className="flex-1 bg-white px-4 py-8">
        <Stack.Screen options={{ title: "Candidate" }} />
        {notFound ? <NotFoundNotice subject="Candidate" /> : <ErrorNotice error={detail.error} />}
      </View>
    );
  }

  const candidate = detail.data.candidate;
  const baseGroups = groupRecords(candidate.records);
  const recordGroups =
    recordView === "my_issues" ? orderGroupsByPreference(baseGroups, preferences) : baseGroups;
  const viewOptions = [
    { value: "by_issue" as const, label: "By issue" },
    ...(hasSaved ? [{ value: "my_issues" as const, label: "My issues first" }] : []),
    { value: "newest" as const, label: "Newest first" },
  ];

  return (
    <ScrollView className="flex-1 bg-white" contentContainerClassName="px-4 py-8">
      <Stack.Screen options={{ title: candidate.display_name }} />
      <AiBanner />
      <Text className="text-2xl font-bold text-ink">{candidate.display_name}</Text>
      <Text className="mt-1 text-sm text-ink-soft">
        {candidate.party} · {candidate.state}
        {candidate.current_office ? <> · {candidate.current_office}</> : null}
      </Text>
      {candidate.official_website_url ? (
        <Text
          className="mt-1 text-sm text-ink underline" accessibilityRole="link"
          onPress={() => openExternalUrl(candidate.official_website_url as string)}
        >
          Official website
        </Text>
      ) : null}
      {candidate.summary ? <Text className="mt-3 text-ink">{candidate.summary}</Text> : null}

      {recordGroups.length > 0 ? (
        <View className="mt-6">
          <Text className="text-lg font-semibold text-ink">Record</Text>
          <View className="mt-2">
            <SortChips options={viewOptions} value={recordView} onChange={setRecordView} />
          </View>
          {recordView === "newest" ? (
            // Flat chronological view; the payload already arrives newest-first.
            <View className="mt-2 gap-3">
              {candidate.records.map((record) => (
                <RecordItem key={record.id} record={record} showTags />
              ))}
            </View>
          ) : (
            recordGroups.map((group) => (
              <View key={group.areaId ?? "other"} className="mt-4">
                <Text className="text-sm font-semibold uppercase tracking-wide text-ink-soft">
                  {group.areaName}
                </Text>
                <View className="mt-2 gap-3">
                  {group.records.map((record) => (
                    <RecordItem key={`${group.areaId ?? "other"}-${record.id}`} record={record} />
                  ))}
                </View>
              </View>
            ))
          )}
        </View>
      ) : null}

      {candidate.elections.length > 0 ? (
        <View className="mt-6">
          <Text className="text-lg font-semibold text-ink">Elections</Text>
          <View className="mt-2 rounded-xl border border-line bg-white">
            {candidate.elections.map((election, index) => (
              <ElectionRow key={election.candidate_election_id} election={election} first={index === 0} />
            ))}
          </View>
        </View>
      ) : null}

      {candidate.last_researched ? (
        <Text className="mt-6 text-xs text-ink-soft">
          Profile last researched {formatElectionDate(candidate.last_researched.slice(0, 10))}.
        </Text>
      ) : null}
    </ScrollView>
  );
}

function RecordItem({ record, showTags = false }: { record: CandidateRecord; showTags?: boolean }) {
  return (
    <View className="rounded-xl border border-line bg-white p-3">
      <Text className="text-sm text-ink">{record.description}</Text>
      <Text className="mt-1 text-xs text-ink-soft">
        {formatElectionDate(record.event_date)}
        {showTags && record.research_area_tags.length > 0
          ? ` · ${record.research_area_tags.map((tag) => tag.name).join(", ")}`
          : ""}
      </Text>
      <SourceLine url={record.source_url} researchedDate={record.created_at.slice(0, 10)} />
    </View>
  );
}

function ElectionRow({
  election,
  first,
}: {
  election: CandidateDetail["candidate"]["elections"][number];
  first: boolean;
}) {
  const router = useRouter();
  return (
    <Pressable
      onPress={() => router.push(`/elections/${election.election_id}`)}
      className={first ? "px-3 py-2 active:bg-surface" : "border-t border-line px-3 py-2 active:bg-surface"}
      accessibilityRole="link"
    >
      <Text className="text-sm">
        <Text className="text-ink underline">{election.official_ballot_title}</Text>{" "}
        <Text className="text-ink-soft">
          · {formatElectionDate(election.election_date)} · {election.district.name}
          {election.is_incumbent ? " · incumbent" : ""}
        </Text>
      </Text>
    </Pressable>
  );
}
