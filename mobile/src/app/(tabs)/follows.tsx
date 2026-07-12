import type { CandidateFollow } from "@voteapp/api-client";
import { formatElectionDate, useFollows, useFollowSaving, useSetFollow } from "@voteapp/api-client";
import { useRouter } from "expo-router";
import { useState } from "react";
import { Pressable, ScrollView, Text, View } from "react-native";
import { AccountGate } from "../../components/AccountGate";
import { Checkbox } from "../../components/Checkbox";
import { EmptyNotice, ErrorNotice, LoadingNotice } from "../../components/Status";

/** Port of the web FollowsPage. */
function FollowRow({ follow }: { follow: CandidateFollow }) {
  const router = useRouter();
  const setFollow = useSetFollow();
  const saving = useFollowSaving();
  // Optimistic overlay: two quick toggles must not build the second payload
  // from the pre-refetch prop (the PUT saves BOTH booleans, so a stale
  // spread silently reverts the first change). null = no pending edits.
  const [pendingNotify, setPendingNotify] = useState<{
    notify_elections: boolean;
    notify_updates: boolean;
  } | null>(null);

  const notify = pendingNotify ?? {
    notify_elections: follow.notify_elections,
    notify_updates: follow.notify_updates,
  };

  function update(fields: Partial<{ notify_elections: boolean; notify_updates: boolean }>) {
    const next = { ...notify, ...fields };
    setPendingNotify(next);
    setFollow.mutate(
      { candidate_id: follow.candidate_id, following: true, ...next },
      {
        onSettled: () => {
          // Server truth (refetched by the mutation's invalidate) takes over.
          setPendingNotify(null);
        },
      }
    );
  }

  return (
    <View className="rounded-xl border border-line bg-white p-4">
      <View className="flex-row items-start justify-between gap-3">
        <View className="flex-1">
          <Text
            className="font-semibold text-ink"
            accessibilityRole="link"
            onPress={() => router.push(`/candidates/${follow.candidate_id}`)}
          >
            {follow.display_name}
          </Text>
          <Text className="text-sm text-ink-soft">
            {follow.party} · {follow.state}
            {follow.current_office ? <> · {follow.current_office}</> : null}
          </Text>
        </View>
        <Pressable
          disabled={saving}
          onPress={() => setFollow.mutate({ candidate_id: follow.candidate_id, following: false })}
          accessibilityRole="button"
          className="rounded-lg border border-line bg-white px-3 py-1 active:border-rausch"
        >
          <Text className="text-xs font-semibold text-ink">Unfollow</Text>
        </Pressable>
      </View>
      {follow.active_election ? (
        <Text className="mt-2 text-sm text-ink-soft">
          On the ballot:{" "}
          <Text
            className="underline"
            accessibilityRole="link"
            onPress={() => router.push(`/elections/${follow.active_election?.election_id}`)}
          >
            {follow.active_election.official_ballot_title}
          </Text>{" "}
          · {formatElectionDate(follow.active_election.election_date)}
        </Text>
      ) : null}
      {follow.latest_record ? (
        <Text className="mt-1 text-sm text-ink-soft" numberOfLines={2}>
          Latest: {follow.latest_record.description}
        </Text>
      ) : null}
      <View className="mt-3 gap-2">
        <Checkbox
          label="Email me about their elections"
          checked={notify.notify_elections}
          disabled={saving}
          onChange={(next) => update({ notify_elections: next })}
        />
        <Checkbox
          label="Email me about record updates"
          checked={notify.notify_updates}
          disabled={saving}
          onChange={(next) => update({ notify_updates: next })}
        />
      </View>
      {setFollow.isError ? (
        <View className="mt-2">
          <ErrorNotice error={setFollow.error} />
        </View>
      ) : null}
    </View>
  );
}

function FollowsBody() {
  const { follows, isLoading, isError } = useFollows();

  return (
    <ScrollView className="flex-1 bg-white" contentContainerClassName="px-4 py-8">
      <Text className="text-2xl font-bold text-ink">Candidates you follow</Text>
      <Text className="mt-1 text-sm text-ink-soft">
        Followed candidates surface first on your ballot; the toggles control the daily email digest.
      </Text>
      {isLoading ? <LoadingNotice text="Loading follows…" /> : null}
      {isError ? (
        <View className="mt-4">
          <ErrorNotice error={new Error("Could not load follows")} />
        </View>
      ) : null}
      {follows && follows.length === 0 ? (
        <EmptyNotice text="You aren't following anyone yet. Use the Follow button on any candidate page." />
      ) : null}
      {follows && follows.length > 0 ? (
        <View className="mt-4 gap-3">
          {follows.map((follow) => (
            <FollowRow key={follow.candidate_id} follow={follow} />
          ))}
        </View>
      ) : null}
    </ScrollView>
  );
}

export default function FollowsScreen() {
  return (
    <AccountGate signedOutText="Log in to manage the candidates you follow.">
      {() => <FollowsBody />}
    </AccountGate>
  );
}
