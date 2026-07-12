import type { BallotPreferences, BallotSummary } from "@voteapp/api-client";
import {
  ApiError,
  apiRequest,
  BALLOT_SORT_DESCRIPTIONS,
  BALLOT_SORTS,
  PRIVACY_NOTICE,
  useMyResearchAreas,
} from "@voteapp/api-client";
import { useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useRouter } from "expo-router";
import { useEffect, useRef, useState } from "react";
import { Pressable, ScrollView, Text, View } from "react-native";
import { AccountGate } from "../../components/AccountGate";
import { AddressAutocomplete } from "../../components/AddressAutocomplete";
import { AiBanner } from "../../components/AiBanner";
import { Checkbox } from "../../components/Checkbox";
import { Collapsible } from "../../components/Collapsible";
import { ElectionCard } from "../../components/ElectionCard";
import { SortChips } from "../../components/SortChips";
import { EmptyNotice, ErrorNotice, LoadingNotice } from "../../components/Status";
import { VerifyPrompt } from "../../components/VerifyPrompt";
import { clearPendingDistrictIds, readPendingDistrictIds } from "../../lib/pendingDistricts";

type SavedBallot = BallotSummary & { matched_address?: string };

// Persisted ordering controls, ported from the web SavedBallotPage: unlike
// the anonymous ballot's local state, these save to the account and apply to
// every future visit.
function BallotPreferenceControls() {
  const queryClient = useQueryClient();
  // Optimistic overlay: consecutive changes must merge from the latest view,
  // not from a stale cache snapshot — the PUT saves the FULL object, so a
  // stale spread would revert the previous change.
  const [pending, setPending] = useState<BallotPreferences | null>(null);
  const prefs = useQuery({
    queryKey: ["me", "ballot-preferences"],
    queryFn: () => apiRequest<BallotPreferences>("/api/me/ballot-preferences"),
    staleTime: 60_000,
  });

  const update = useMutation({
    mutationKey: ["put-ballot-preferences"],
    mutationFn: (next: BallotPreferences) =>
      apiRequest<BallotPreferences>("/api/me/ballot-preferences", { method: "PUT", body: next }),
    onSuccess: (saved) => {
      queryClient.setQueryData(["me", "ballot-preferences"], saved);
      void queryClient.invalidateQueries({ queryKey: ["me", "ballot"] });
    },
    onSettled: () => {
      setPending(null);
    },
  });
  // Cross-mount in-flight guard: component-local isPending resets on
  // remount, but the mutation cache does not — a remounted control must stay
  // locked until the older full-object PUT settles, or two writes could
  // commit out of order.
  const saving = useIsMutating({ mutationKey: ["put-ballot-preferences"] }) > 0;

  if (prefs.isError) {
    return <ErrorNotice error={prefs.error} />;
  }
  if (!prefs.data) {
    return null;
  }
  const current = pending ?? prefs.data;

  function change(fields: Partial<BallotPreferences>) {
    const next = { ...current, ...fields };
    setPending(next);
    update.mutate(next);
  }

  return (
    <View className="mt-3 gap-2">
      {/* Disabled while a save is in flight: the PUT replaces the FULL
          object, so concurrent requests could commit out of order. SortChips
          has no disabled state; gate in the handler instead. */}
      <SortChips
        options={BALLOT_SORTS}
        value={current.sort}
        onChange={(sort) => {
          if (!saving) {
            change({ sort });
          }
        }}
      />
      <Checkbox
        label="Followed candidates first"
        checked={current.followed_first}
        disabled={saving}
        onChange={(followed_first) => change({ followed_first })}
      />
      {update.isError ? <ErrorNotice error={update.error} /> : null}
    </View>
  );
}

function AddressForm({ compact }: { compact: boolean }) {
  const [address, setAddress] = useState("");
  const queryClient = useQueryClient();

  const update = useMutation({
    mutationFn: () =>
      apiRequest<SavedBallot>("/api/me/address", { method: "PUT", body: { address: address.trim() } }),
    onSuccess: () => {
      // The PUT returns a plain district ballot, but GET /api/me/ballot
      // applies saved sort preferences and followed-candidate ordering —
      // refetch the canonical version instead of caching the PUT body.
      void queryClient.invalidateQueries({ queryKey: ["me", "ballot"] });
      setAddress("");
    },
  });

  const canSave = address.trim().length > 0 && !update.isPending;

  return (
    <View className={compact ? "mt-2 gap-3" : "mt-6 gap-3"}>
      <View>
        <Text className="text-sm font-medium text-ink">{compact ? "New address" : "Home address"}</Text>
        <AddressAutocomplete
          value={address}
          onChange={setAddress}
          placeholder="1600 Pennsylvania Avenue NW, Washington, DC 20500"
        />
        <Text className="mt-1 text-xs text-ink-soft">{PRIVACY_NOTICE}</Text>
      </View>
      <Pressable
        disabled={!canSave}
        onPress={() => update.mutate()}
        accessibilityRole="button"
        className={
          canSave ? "w-full rounded-md bg-rausch px-4 py-3 active:bg-rausch-dark" : "w-full rounded-md bg-line px-4 py-3"
        }
      >
        <Text className="text-center font-semibold text-white">
          {update.isPending ? "Saving…" : "Save address"}
        </Text>
      </Pressable>
      {update.isError ? <ErrorNotice error={update.error} /> : null}
    </View>
  );
}

// Anonymous-to-account handoff, ported from the web with one extra state:
// AsyncStorage reads are async, so "checking" covers the initial read that
// sessionStorage answered synchronously on the web.
type HandoffState = "checking" | "pending" | "done" | "failed";

function SavedBallotBody({ email }: { email: string }) {
  const router = useRouter();
  const queryClient = useQueryClient();
  const savedPrefs = useQuery({
    queryKey: ["me", "ballot-preferences"],
    queryFn: () => apiRequest<BallotPreferences>("/api/me/ballot-preferences"),
    staleTime: 60_000,
  });
  const { savedAreaIds } = useMyResearchAreas();
  const [handoffState, setHandoffState] = useState<HandoffState>("checking");
  const handoffFiredRef = useRef(false);

  useEffect(() => {
    if (handoffState !== "checking") {
      return;
    }
    void readPendingDistrictIds().then((ids) => {
      setHandoffState(ids.length === 0 ? "done" : "pending");
    });
  }, [handoffState]);

  // Initialize saved districts from the last anonymous search (this body
  // only renders verified — the endpoint is verified-email-gated).
  // Permanent rejections (4xx: stale/unknown ids) resolve to "the account's
  // ballot is the source of truth"; transient failures keep the ids and
  // surface an explicit retry instead of dropping the user onto the empty
  // set-address form with their search still queued.
  useEffect(() => {
    if (handoffState !== "pending" || handoffFiredRef.current) {
      return;
    }
    handoffFiredRef.current = true;
    void (async () => {
      const districtIds = await readPendingDistrictIds();
      try {
        await apiRequest("/api/me/districts/initialize", {
          method: "POST",
          body: { district_ids: districtIds },
        });
        await clearPendingDistrictIds();
        setHandoffState("done");
        void queryClient.invalidateQueries({ queryKey: ["me", "ballot"] });
      } catch (error) {
        // 429 is transient (rate limit), not a stale/unknown-ids rejection —
        // keep the queued ids and offer the retry UI.
        if (error instanceof ApiError && error.status < 500 && error.status !== 429) {
          await clearPendingDistrictIds();
          setHandoffState("done");
          void queryClient.invalidateQueries({ queryKey: ["me", "ballot"] });
        } else {
          setHandoffState("failed");
        }
      }
    })();
  }, [handoffState, queryClient]);

  const ballot = useQuery<SavedBallot>({
    queryKey: ["me", "ballot"],
    queryFn: () => apiRequest<SavedBallot>("/api/me/ballot"),
    enabled: handoffState === "done",
    retry: false,
  });

  if (handoffState === "failed") {
    return (
      <View className="items-center px-4 py-10">
        <Text className="text-center text-ink-soft">
          We couldn&apos;t save the districts from your recent address search to your account. Your search is
          still remembered.
        </Text>
        <Pressable
          onPress={() => {
            handoffFiredRef.current = false;
            setHandoffState("pending");
          }}
          accessibilityRole="button"
          className="mt-4 rounded-lg bg-rausch px-4 py-2 active:bg-rausch-dark"
        >
          <Text className="font-semibold text-white">Try again</Text>
        </Pressable>
      </View>
    );
  }

  if (handoffState !== "done" || ballot.isPending) {
    return <LoadingNotice text="Loading your ballot…" />;
  }

  if (ballot.isError) {
    // A 403 here means verification state changed server-side; anything else
    // is a real error.
    if (ballot.error instanceof ApiError && ballot.error.status === 403) {
      return <VerifyPrompt email={email} />;
    }
    return (
      <View className="px-4 py-8">
        <ErrorNotice error={ballot.error} />
      </View>
    );
  }

  const data = ballot.data;

  if (data.districts.length === 0) {
    return (
      <ScrollView className="flex-1 bg-white" contentContainerClassName="px-4 py-10" keyboardShouldPersistTaps="handled">
        <Text className="text-2xl font-bold text-ink">Set your address</Text>
        <Text className="mt-2 text-sm text-ink-soft">
          Save your home address once and your ballot will be waiting every time you come back.
        </Text>
        <AddressForm compact={false} />
      </ScrollView>
    );
  }

  return (
    <ScrollView className="flex-1 bg-white" contentContainerClassName="px-4 py-8" keyboardShouldPersistTaps="handled">
      <AiBanner />
      <Text className="text-2xl font-bold text-ink">Your saved ballot</Text>
      <Text className="mt-1 text-sm text-ink-soft">
        {data.elections.length} election{data.elections.length === 1 ? "" : "s"} across{" "}
        {data.districts.length} district{data.districts.length === 1 ? "" : "s"},{" "}
        {BALLOT_SORT_DESCRIPTIONS[savedPrefs.data?.sort ?? "vote_power"]}
      </Text>
      <BallotPreferenceControls />

      {data.elections.length === 0 ? (
        <EmptyNotice text="No upcoming elections found for your districts yet. Check back — new elections are added as they are announced." />
      ) : (
        <View className="mt-4 gap-3">
          {data.elections.map((election) => (
            <ElectionCard key={election.id} election={election} savedAreaIds={savedAreaIds} />
          ))}
        </View>
      )}

      <View className="mt-8 rounded-xl border border-line bg-surface p-4">
        <Collapsible summary="Change your address">
          <AddressForm compact />
        </Collapsible>
      </View>
      <Text className="mt-4 text-sm text-ink-soft">
        Looking somewhere else?{" "}
        <Text className="underline" accessibilityRole="link" onPress={() => router.push("/")}>
          Run a one-off address search
        </Text>{" "}
        without changing your saved address.
      </Text>
    </ScrollView>
  );
}

export default function MyBallotScreen() {
  return (
    <AccountGate signedOutText="Log in to see your saved ballot.">
      {(me) => <SavedBallotBody email={me.email} />}
    </AccountGate>
  );
}
