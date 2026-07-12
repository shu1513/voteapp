import Ionicons from "@expo/vector-icons/Ionicons";
import type { ResearchAreaCatalog, ResearchAreaPreferencesResult } from "@voteapp/api-client";
import { apiRequest, MAX_RESEARCH_AREA_RANK } from "@voteapp/api-client";
import { useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Stack } from "expo-router";
import { useState } from "react";
import { Pressable, ScrollView, Text, View } from "react-native";
import { AccountGate } from "../../components/AccountGate";
import { ErrorNotice, LoadingNotice } from "../../components/Status";

// Port of the web settings "Issues you care about" section. The web's
// drag-to-reorder becomes up/down rank buttons — the plan's v1 (dnd-kit's
// own keyboard path proves order buttons are acceptable UX); a draggable
// list can come later. Verified-only, same as the web.

function moveItem(ids: string[], from: number, to: number): string[] {
  const next = [...ids];
  const [moved] = next.splice(from, 1);
  next.splice(to, 0, moved);
  return next;
}

function ResearchAreasBody() {
  const queryClient = useQueryClient();
  // Optimistic overlay: the PUT replaces the whole ranked list, so quick
  // consecutive edits (reorder, add, remove) must merge from the latest view.
  const [pending, setPending] = useState<string[] | null>(null);
  const catalog = useQuery({
    queryKey: ["research-areas"],
    queryFn: () => apiRequest<ResearchAreaCatalog>("/api/research-areas"),
    staleTime: 5 * 60_000,
  });
  const prefs = useQuery({
    queryKey: ["me", "research-area-preferences"],
    queryFn: () => apiRequest<ResearchAreaPreferencesResult>("/api/me/research-area-preferences"),
    staleTime: 60_000,
  });
  const update = useMutation({
    mutationKey: ["put-research-area-preferences"],
    // List position is the rank: first = rank 1.
    mutationFn: (ids: string[]) =>
      apiRequest<ResearchAreaPreferencesResult>("/api/me/research-area-preferences", {
        method: "PUT",
        body: { preferences: ids.map((research_area_id, index) => ({ research_area_id, rank: index + 1 })) },
      }),
    onSuccess: (saved) => {
      queryClient.setQueryData(["me", "research-area-preferences"], saved);
      // The saved ballot is server-sorted by these preferences (my_areas),
      // and the ballot-preferences default can flip to my_areas when the
      // first area is saved — both must refetch, not wait out staleTime.
      void queryClient.invalidateQueries({ queryKey: ["me", "ballot"] });
      void queryClient.invalidateQueries({ queryKey: ["me", "ballot-preferences"] });
    },
    onSettled: () => {
      setPending(null);
    },
  });
  // Cross-mount in-flight guard, same as the other full-replace preference
  // writes: controls stay locked until the older PUT settles. (Render value —
  // save() re-checks the mutation cache imperatively to close the gap before
  // this re-renders.)
  const saving = useIsMutating({ mutationKey: ["put-research-area-preferences"] }) > 0;

  if (catalog.isPending || prefs.isPending) {
    return <LoadingNotice text="Loading…" />;
  }
  if (catalog.isError || prefs.isError) {
    return (
      <View className="px-4 py-8">
        <ErrorNotice error={catalog.error ?? prefs.error} />
      </View>
    );
  }

  const areaById = new Map(catalog.data.research_areas.map((area) => [area.id, area]));
  // Server order is rank ASC NULLS LAST, so it is the editor order directly.
  const orderedIds = pending ?? prefs.data.preferences.map((preference) => preference.research_area_id);
  const selectedSet = new Set(orderedIds);
  const atCapacity = orderedIds.length >= MAX_RESEARCH_AREA_RANK;

  function save(nextIds: string[]) {
    // Checked against the mutation cache, not the rendered `saving` value: a
    // handler created before the disabling re-render could otherwise slip
    // through in the same tick as another edit's mutate().
    if (queryClient.isMutating({ mutationKey: ["put-research-area-preferences"] }) > 0) {
      return;
    }
    setPending(nextIds);
    update.mutate(nextIds);
  }

  return (
    <ScrollView className="flex-1 bg-white" contentContainerClassName="px-4 py-8">
      <Text className="text-sm text-ink-soft">
        Put what matters most at the top — #1 counts the most in your ballot ordering.
      </Text>

      {orderedIds.length > 0 ? (
        <View className="mt-3 gap-1.5">
          {orderedIds.map((id, index) => {
            const name = areaById.get(id)?.name ?? "Unknown area";
            return (
              <View key={id} className="flex-row items-center gap-2 rounded-lg border border-line bg-white px-2 py-2">
                <Text className="w-6 shrink-0 text-center text-xs font-semibold text-rausch-dark">
                  #{index + 1}
                </Text>
                <Text className="flex-1 text-sm text-ink">{name}</Text>
                <Pressable
                  disabled={saving || index === 0}
                  onPress={() => save(moveItem(orderedIds, index, index - 1))}
                  accessibilityRole="button"
                  accessibilityLabel={`Move ${name} up to rank ${index}`}
                  className={saving || index === 0 ? "px-2 py-1 opacity-30" : "px-2 py-1"}
                >
                  <Ionicons name="chevron-up" size={18} color="#222222" />
                </Pressable>
                <Pressable
                  disabled={saving || index === orderedIds.length - 1}
                  onPress={() => save(moveItem(orderedIds, index, index + 1))}
                  accessibilityRole="button"
                  accessibilityLabel={`Move ${name} down to rank ${index + 2}`}
                  className={saving || index === orderedIds.length - 1 ? "px-2 py-1 opacity-30" : "px-2 py-1"}
                >
                  <Ionicons name="chevron-down" size={18} color="#222222" />
                </Pressable>
                <Pressable
                  disabled={saving}
                  onPress={() => save(orderedIds.filter((other) => other !== id))}
                  accessibilityRole="button"
                  accessibilityLabel={`Remove ${name}`}
                  className={saving ? "px-2 py-1 opacity-30" : "px-2 py-1"}
                >
                  <Ionicons name="close" size={18} color="#717171" />
                </Pressable>
              </View>
            );
          })}
        </View>
      ) : (
        <Text className="mt-3 text-sm text-ink-soft">
          Nothing selected yet — pick up to {MAX_RESEARCH_AREA_RANK} below.
        </Text>
      )}

      <Text className="mt-6 text-sm font-medium text-ink">
        Add issues{" "}
        <Text className="font-normal text-ink-soft">
          ({orderedIds.length}/{MAX_RESEARCH_AREA_RANK})
        </Text>
      </Text>
      <View className="mt-2 flex-row flex-wrap gap-2">
        {catalog.data.research_areas
          .filter((area) => !selectedSet.has(area.id))
          .map((area) => (
            <Pressable
              key={area.id}
              disabled={saving || atCapacity}
              onPress={() => save([...orderedIds, area.id])}
              accessibilityRole="button"
              className={
                saving || atCapacity
                  ? "rounded-lg border border-line bg-white px-3 py-1.5 opacity-50"
                  : "rounded-lg border border-line bg-white px-3 py-1.5 active:border-rausch"
              }
            >
              <Text className="text-xs text-ink">{area.name}</Text>
            </Pressable>
          ))}
      </View>

      {update.isError ? (
        <View className="mt-4">
          <ErrorNotice error={update.error} />
        </View>
      ) : null}
    </ScrollView>
  );
}

export default function ResearchAreasScreen() {
  return (
    <>
      <Stack.Screen options={{ title: "Issues you care about" }} />
      <AccountGate signedOutText="Log in to manage your account.">{() => <ResearchAreasBody />}</AccountGate>
    </>
  );
}
