import Ionicons from "@expo/vector-icons/Ionicons";
import type { RankedResearchArea, ResearchAreaCatalog, ResearchAreaPreferencesResult } from "@voteapp/api-client";
import {
  apiRequest,
  INTEGRITY_ETHICS_SLUG,
  newRankedResearchArea,
  sortByResearchAreaPriority,
  toPreferenceInputs,
  toRankedResearchAreas,
} from "@voteapp/api-client";
import { useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Stack } from "expo-router";
import { useState } from "react";
import { Pressable, ScrollView, Text, View } from "react-native";
import { AccountGate } from "../../components/AccountGate";
import { ErrorNotice, LoadingNotice } from "../../components/Status";

// Port of the web settings "Issues you care about" section. The web's
// drag-to-reorder becomes up/down rank buttons — the plan's v1 (dnd-kit's
// own keyboard path proves order buttons are acceptable UX); a draggable
// list can come later. Each row also carries the web picker's Support/Oppose
// direction pair and the "Must" hard-veto toggle (Phase 4) — auto-pick's
// inputs, so mobile-first users never run it on defaults they never chose.
// Verified-only, same as the web.

function moveItem<T>(rows: T[], from: number, to: number): T[] {
  const next = [...rows];
  const [moved] = next.splice(from, 1);
  next.splice(to, 0, moved);
  return next;
}

function ResearchAreasBody() {
  const queryClient = useQueryClient();
  // Optimistic overlay: the PUT replaces the whole ranked list, so quick
  // consecutive edits (reorder, add, remove) must merge from the latest view.
  const [pending, setPending] = useState<RankedResearchArea[] | null>(null);
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
    // List position is the rank: first = rank 1. Direction/veto ride on
    // every row now that the editor owns them (the backend would preserve
    // omitted fields, but an editor that shows a value must send it).
    mutationFn: (ranked: RankedResearchArea[]) =>
      apiRequest<ResearchAreaPreferencesResult>("/api/me/research-area-preferences", {
        method: "PUT",
        body: { preferences: toPreferenceInputs(ranked) },
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
  const ranked = pending ?? toRankedResearchAreas(prefs.data.preferences);
  const selectedSet = new Set(ranked.map((row) => row.research_area_id));

  function save(next: RankedResearchArea[]) {
    // Checked against the mutation cache, not the rendered `saving` value: a
    // handler created before the disabling re-render could otherwise slip
    // through in the same tick as another edit's mutate().
    if (queryClient.isMutating({ mutationKey: ["put-research-area-preferences"] }) > 0) {
      return;
    }
    setPending(next);
    update.mutate(next);
  }

  function updateRow(id: string, patch: Partial<Omit<RankedResearchArea, "research_area_id">>) {
    save(ranked.map((row) => (row.research_area_id === id ? { ...row, ...patch } : row)));
  }

  return (
    <ScrollView className="flex-1 bg-white" contentContainerClassName="px-4 py-8">
      <Text className="text-sm text-ink-soft">
        Put your issues in the order of your priorities, and set your position on each. Choose
        &ldquo;Must&rdquo; if you will absolutely not accept a candidate or ballot measure that takes
        the opposite stance from yours.
      </Text>

      {ranked.length > 0 ? (
        <View className="mt-3 gap-1.5">
          {ranked.map((row, index) => {
            const id = row.research_area_id;
            const area = areaById.get(id);
            const name = area?.name ?? "Unknown area";
            const isEthics = area?.slug === INTEGRITY_ETHICS_SLUG;
            // Direction-neutral on purpose: with Oppose selected, the veto
            // fires on records that SUPPORT the goal, so "who opposes this"
            // would read backwards. Same copy as the web picker.
            const vetoLabel = isEthics
              ? "Skip candidates with a negative ethics record (a violation, sanction, or conviction)"
              : "Must: never pick a candidate or measure that goes against my position on this";
            return (
              <View key={id} className="rounded-lg border border-line bg-white px-2 py-2">
                <View className="flex-row items-center gap-2">
                  <Text className="w-6 shrink-0 text-center text-xs font-semibold text-rausch-dark">
                    #{index + 1}
                  </Text>
                  <Text className="flex-1 text-sm text-ink">{name}</Text>
                  <Pressable
                    disabled={saving || index === 0}
                    onPress={() => save(moveItem(ranked, index, index - 1))}
                    accessibilityRole="button"
                    accessibilityLabel={`Move ${name} up to rank ${index}`}
                    className={saving || index === 0 ? "px-2 py-1 opacity-30" : "px-2 py-1"}
                  >
                    <Ionicons name="chevron-up" size={18} color="#222222" />
                  </Pressable>
                  <Pressable
                    disabled={saving || index === ranked.length - 1}
                    onPress={() => save(moveItem(ranked, index, index + 1))}
                    accessibilityRole="button"
                    accessibilityLabel={`Move ${name} down to rank ${index + 2}`}
                    className={saving || index === ranked.length - 1 ? "px-2 py-1 opacity-30" : "px-2 py-1"}
                  >
                    <Ionicons name="chevron-down" size={18} color="#222222" />
                  </Pressable>
                  <Pressable
                    disabled={saving}
                    onPress={() => save(ranked.filter((other) => other.research_area_id !== id))}
                    accessibilityRole="button"
                    accessibilityLabel={`Remove ${name}`}
                    className={saving ? "px-2 py-1 opacity-30" : "px-2 py-1"}
                  >
                    <Ionicons name="close" size={18} color="#717171" />
                  </Pressable>
                </View>
                {/* Second line, not more chips on the first: a phone row
                    can't fit rank + name + 3 icon buttons + 3 toggles. */}
                <View className="mt-1.5 flex-row flex-wrap items-center gap-2 pl-8">
                  {isEthics ? null : (
                    <View
                      className="flex-row overflow-hidden rounded-md border border-line"
                      accessibilityRole="radiogroup"
                      accessibilityLabel={`Your position on ${name}`}
                    >
                      {(["support", "oppose"] as const).map((direction) => (
                        <Pressable
                          key={direction}
                          disabled={saving}
                          onPress={() => updateRow(id, { direction })}
                          accessibilityRole="radio"
                          accessibilityState={{ checked: row.direction === direction, disabled: saving }}
                          accessibilityLabel={`${direction === "support" ? "Support" : "Oppose"} ${name}`}
                          className={`px-2.5 py-1${saving ? " opacity-50" : ""} ${
                            row.direction === direction
                              ? direction === "support"
                                ? "bg-green-700"
                                : "bg-red-700"
                              : "bg-white"
                          }`}
                        >
                          <Text
                            className={
                              row.direction === direction ? "text-xs text-white" : "text-xs text-ink-soft"
                            }
                          >
                            {direction === "support" ? "Support" : "Oppose"}
                          </Text>
                        </Pressable>
                      ))}
                    </View>
                  )}
                  <Pressable
                    disabled={saving}
                    onPress={() => updateRow(id, { hard_veto: !row.hard_veto })}
                    accessibilityRole="button"
                    accessibilityState={{ selected: row.hard_veto, disabled: saving }}
                    accessibilityLabel={`${vetoLabel} (${name})`}
                    className={`rounded-md border px-2.5 py-1${saving ? " opacity-50" : ""} ${
                      row.hard_veto ? "border-rausch-dark bg-rausch-dark" : "border-line bg-white"
                    }`}
                  >
                    <Text className={row.hard_veto ? "text-xs text-white" : "text-xs text-ink-soft"}>
                      {isEthics ? "Skip candidate if negative record" : "Must"}
                    </Text>
                  </Pressable>
                </View>
              </View>
            );
          })}
        </View>
      ) : (
        <Text className="mt-3 text-sm text-ink-soft">Nothing selected yet — pick below.</Text>
      )}

      <Text className="mt-6 text-sm font-medium text-ink">
        Add issues{" "}
        <Text className="font-normal text-ink-soft">({ranked.length} selected)</Text>
      </Text>
      <View className="mt-2 flex-row flex-wrap gap-2">
        {sortByResearchAreaPriority(catalog.data.research_areas.filter((area) => !selectedSet.has(area.id))).map(
          (area) => (
            <Pressable
              key={area.id}
              disabled={saving}
              onPress={() => save([...ranked, newRankedResearchArea(area.id)])}
              accessibilityRole="button"
              className={
                saving
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
