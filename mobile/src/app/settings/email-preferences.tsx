import type { EmailPreferences } from "@voteapp/api-client";
import { apiRequest } from "@voteapp/api-client";
import { useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Stack } from "expo-router";
import { useState } from "react";
import { ScrollView, Text, View } from "react-native";
import { AccountGate } from "../../components/AccountGate";
import { Checkbox } from "../../components/Checkbox";
import { ErrorNotice, LoadingNotice } from "../../components/Status";

// Port of the web settings Email notifications section. Verified-only —
// the endpoint is verified-email-gated, and AccountGate's default ladder
// enforces the same rule if someone lands here unverified.

const LABELS: { key: keyof EmailPreferences; label: string; description?: string }[] = [
  { key: "email_digest", label: "Daily digest about candidates you follow" },
  { key: "email_new_election_alerts", label: "New elections in your districts" },
  {
    key: "email_election_reminders",
    label: "Remind me the day before each election",
    description: "One email covering everything on your ballot that day.",
  },
  {
    key: "email_issue_updates",
    label: "Updates about the issues you saved",
    description: "Occasional emails when there is something worth knowing about your issues.",
  },
];

function EmailPreferencesBody() {
  const queryClient = useQueryClient();
  // Optimistic overlay: the PUT saves all the flags, so consecutive quick
  // toggles must merge from the latest view, not a stale cache snapshot.
  const [pending, setPending] = useState<EmailPreferences | null>(null);
  const prefs = useQuery({
    queryKey: ["me", "email-preferences"],
    queryFn: () => apiRequest<EmailPreferences>("/api/me/email-preferences"),
    staleTime: 60_000,
  });
  const update = useMutation({
    mutationKey: ["put-email-preferences"],
    mutationFn: (next: EmailPreferences) =>
      apiRequest<EmailPreferences>("/api/me/email-preferences", { method: "PUT", body: next }),
    onSuccess: (saved) => {
      queryClient.setQueryData(["me", "email-preferences"], saved);
    },
    onSettled: () => {
      setPending(null);
    },
  });
  // Cross-mount in-flight guard: local isPending resets if the user leaves
  // and returns mid-save, but the mutation cache does not — the toggles stay
  // locked until the older full-object PUT settles.
  const saving = useIsMutating({ mutationKey: ["put-email-preferences"] }) > 0;

  return (
    <ScrollView className="flex-1 bg-white" contentContainerClassName="px-4 py-8">
      <Text className="text-sm text-ink-soft">
        Choose which emails you get. Unchecking everything stops all non-account email.
      </Text>
      {prefs.isPending ? <LoadingNotice text="Loading…" /> : null}
      {prefs.isError ? (
        <View className="mt-4">
          <ErrorNotice error={prefs.error} />
        </View>
      ) : null}
      {prefs.data ? (
        <View className="mt-4 gap-3">
          {LABELS.map(({ key, label, description }) => {
            const current = pending ?? prefs.data;
            return (
              <Checkbox
                key={key}
                label={label}
                description={description}
                checked={current[key]}
                // Disabled while a save is in flight (cross-mount): the PUT
                // replaces all the flags, so concurrent requests could commit
                // out of order and the earlier write would win.
                disabled={saving}
                onChange={(next) => {
                  // `saving` above is from the last render; a tap landing
                  // before the disabling re-render commits would start a
                  // second overlapping PUT. Same guard as research-areas.
                  if (queryClient.isMutating({ mutationKey: ["put-email-preferences"] }) > 0) {
                    return;
                  }
                  const nextPrefs = { ...current, [key]: next };
                  setPending(nextPrefs);
                  update.mutate(nextPrefs);
                }}
              />
            );
          })}
        </View>
      ) : null}
      {update.isError ? (
        <View className="mt-4">
          <ErrorNotice error={update.error} />
        </View>
      ) : null}
    </ScrollView>
  );
}

export default function EmailPreferencesScreen() {
  return (
    <>
      <Stack.Screen options={{ title: "Email notifications" }} />
      <AccountGate signedOutText="Log in to manage your account.">{() => <EmailPreferencesBody />}</AccountGate>
    </>
  );
}
