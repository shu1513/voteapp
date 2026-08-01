import type {
  CandidateDetail,
  CandidateElection,
  CandidateRecord,
  FinanceSummary,
  ResearchAreaPreference,
} from "@voteapp/api-client";
import {
  ApiError,
  apiRequest,
  formatDistrictName,
  formatElectionDate,
  hasFinanceContent,
  UNRANKED_RESEARCH_AREA_RANK,
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
import { ShareButton } from "../../components/ShareButton";
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

// Election dates are YYYY-MM-DD calendar strings; "today" is the last US
// clock still on a given date — Pacific/Honolulu, UTC-10, no DST — mirroring
// the backend's US_LATEST_LOCAL_DATE_SQL: an election counts as past only
// once the entire United States has finished that day. en-CA formats as
// YYYY-MM-DD. Same logic as the web CandidatePage.
function usLatestLocalDate(): string {
  return new Intl.DateTimeFormat("en-CA", { timeZone: "Pacific/Honolulu" }).format(new Date());
}

// Narrow per-candidate finance endpoint (mirrors the web CandidatePage):
// the full election payload would drag every opponent's records and finance
// along just to read one summary.
function useElectionFinance(electionId: string, candidateId: string, enabled: boolean) {
  const query = useQuery({
    queryKey: ["election-finance", electionId, candidateId],
    queryFn: () =>
      apiRequest<{ finance_summary: FinanceSummary | null }>(
        `/api/elections/${electionId}/candidates/${candidateId}/finance`
      ),
    enabled,
    staleTime: 60_000,
  });
  return { summary: query.data?.finance_summary ?? null, isPending: query.isPending, isError: query.isError };
}

// Eager finance for an election the candidate is currently in. Renders its
// own section so there is no orphan heading while the fetch is in flight or
// when the election has no finance coverage — a fetch failure also just
// leaves the profile without the section.
function OngoingElectionFinance({
  election,
  candidateId,
}: {
  election: CandidateElection;
  candidateId: string;
}) {
  const { summary } = useElectionFinance(election.election_id, candidateId, true);
  if (!hasFinanceContent(summary)) {
    return null;
  }
  return (
    <View className="mt-6">
      <Text
        className="text-lg font-semibold text-ink"
        accessibilityLabel={`Campaign finance — ${election.official_ballot_title}`}
      >
        Campaign finance
      </Text>
      <Text className="mt-1 text-sm text-ink-soft">
        {election.official_ballot_title} · {formatElectionDate(election.election_date)}
      </Text>
      <View className="mt-2 rounded-xl border border-line bg-white p-4">
        <FinanceSummaryCard summary={summary} />
      </View>
    </View>
  );
}

// Lazy finance for a past election-history row: nothing is fetched until
// the user opens the disclosure (opening is the explicit ask, so unlike the
// ongoing section this one states it when there is nothing to show).
function PastElectionFinance({
  election,
  candidateId,
}: {
  election: CandidateElection;
  candidateId: string;
}) {
  const [opened, setOpened] = useState(false);
  const [expanded, setExpanded] = useState(false);
  const { summary, isPending, isError } = useElectionFinance(election.election_id, candidateId, opened);
  return (
    <View className="mt-1">
      <Pressable
        onPress={() => {
          setExpanded((current) => !current);
          setOpened(true);
        }}
        accessibilityRole="button"
        accessibilityState={{ expanded }}
        accessibilityLabel={`Campaign finance for ${election.official_ballot_title}, ${formatElectionDate(election.election_date)}`}
      >
        <Text className="text-xs text-ink-soft underline">
          {expanded ? "▾" : "▸"} Campaign finance
        </Text>
      </Pressable>
      {expanded ? (
        <View className="mt-2">
          {isPending ? (
            <Text className="text-xs text-ink-soft">Loading…</Text>
          ) : isError ? (
            <Text className="text-xs text-ink-soft">Couldn’t load finance data for this election.</Text>
          ) : hasFinanceContent(summary) ? (
            <FinanceSummaryCard summary={summary} />
          ) : (
            <Text className="text-xs text-ink-soft">No finance data for this election.</Text>
          )}
        </View>
      ) : null}
    </View>
  );
}

/**
 * Port of the web CandidatePage. SSR loader becomes plain useQuery;
 * follow/report controls arrive with the auth chunk.
 */
export default function CandidateScreen() {
  const { candidateId } = useLocalSearchParams<{ candidateId: string }>();
  const { hasSaved, preferences } = useMyResearchAreas();
  // Follow state comes from the follows list (only fetched for verified
  // users); the button renders only once that list has loaded — before then
  // a followed candidate would briefly show as unfollowed. Same as the web.
  const { follows, canFollow } = useFollows();
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
  const today = usLatestLocalDate();
  const ongoingElections = candidate.elections.filter((election) => election.election_date >= today);
  const viewOptions = [
    { value: "by_issue" as const, label: "By issue" },
    ...(hasSaved ? [{ value: "my_issues" as const, label: "My issues first" }] : []),
    { value: "newest" as const, label: "Newest first" },
  ];

  return (
    <ScrollView className="flex-1 bg-white" contentContainerClassName="px-4 py-8">
      <Stack.Screen options={{ title: candidate.display_name }} />
      <View className="flex-row flex-wrap items-center justify-between gap-3">
        <Text className="flex-1 text-2xl font-bold text-ink">{candidate.display_name}</Text>
        <ShareButton
          path={`/candidates/${candidate.candidate_id}`}
          shareText={`${candidate.display_name} (${candidate.party}, ${candidate.state})`}
        />
        {canFollow && follows ? (
          <FollowButton
            candidateId={candidate.candidate_id}
            isFollowing={follows.some((follow) => follow.candidate_id === candidate.candidate_id)}
          />
        ) : null}
      </View>
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

      {ongoingElections.map((election) => (
        <OngoingElectionFinance
          key={election.candidate_election_id}
          election={election}
          candidateId={candidate.candidate_id}
        />
      ))}

      {recordGroups.length > 0 ? (
        <View className="mt-6">
          {/* "Track record", not "Record"/"Records" — same reasoning as the
              web CandidatePage heading. */}
          <Text className="text-lg font-semibold text-ink">Track record</Text>
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
      ) : (
        // Same empty-state distinction as the web candidate page: an empty
        // record list is ambiguous on its own — researched-and-none-found and
        // not-researched-yet must read differently. "Verified", not "found":
        // a search can finish with every discovered record dropped for
        // permanently failing source checks, and the checkpoint still
        // advances — the array only proves nothing verifiable was kept.
        <Text className="mt-6 text-sm text-ink-soft">
          {candidate.records_researched_through
            ? `No verified public records for this candidate — record history researched through ${formatElectionDate(candidate.records_researched_through)}.`
            : "This candidate's record history has not been researched yet."}
        </Text>
      )}

      {candidate.elections.length > 0 ? (
        <View className="mt-6">
          <Text className="text-lg font-semibold text-ink">Elections</Text>
          <View className="mt-2 rounded-xl border border-line bg-white">
            {candidate.elections.map((election, index) => (
              <ElectionRow
                key={election.candidate_election_id}
                election={election}
                first={index === 0}
                // Ongoing races already show finance eagerly above; past
                // rows offer it on demand.
                showPastFinance={election.election_date < today}
                candidateId={candidate.candidate_id}
              />
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
  showPastFinance,
  candidateId,
}: {
  election: CandidateElection;
  first: boolean;
  showPastFinance: boolean;
  candidateId: string;
}) {
  const router = useRouter();
  return (
    <View className={first ? "px-3 py-2" : "border-t border-line px-3 py-2"}>
      <Pressable
        onPress={() => router.push(`/elections/${election.election_id}`)}
        className="active:bg-surface"
        accessibilityRole="link"
      >
        <Text className="text-sm">
          <Text className="text-ink underline">{election.official_ballot_title}</Text>{" "}
          <Text className="text-ink-soft">
            · {formatElectionDate(election.election_date)} · {formatDistrictName(election.district.name)}
            {election.is_incumbent ? " · incumbent" : ""}
          </Text>
        </Text>
      </Pressable>
      {showPastFinance ? <PastElectionFinance election={election} candidateId={candidateId} /> : null}
    </View>
  );
}
