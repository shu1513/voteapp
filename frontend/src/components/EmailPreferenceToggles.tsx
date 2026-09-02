import { useState } from "react";
import { useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { apiRequest } from "@voteapp/api-client";
import type { EmailPreferences } from "@voteapp/api-client";
import { ErrorNotice, LoadingNotice } from "./Status";

// The email-notification opt-in checkboxes, shared by Settings (all of them)
// and the Mission page (a filtered pair under "subscribe to our emails").
// The endpoint is verified-only — callers must gate on me.email_verified.
// Both mounts share the query key and mutation key, so the cache is one and
// the in-flight save guard spans pages.

const ALL_LABELS: Array<{ key: keyof EmailPreferences; label: string; description?: string }> = [
  {
    key: "email_digest",
    // This one opt-in gates two senders: the candidate-follow digest AND
    // district election-result alerts (sendElectionResultAlerts rides
    // email_digest), so the copy must name both. Frequency-free on
    // purpose: both jobs are event-gated with no enforced cadence, and
    // result alerts cluster right after election days.
    label: "Updates about my candidates and election results",
    description:
      "Occasional emails when candidates I follow take new actions, and when elections in my districts have results.",
  },
  { key: "email_new_election_alerts", label: "Notify me about new elections coming up in my districts" },
  {
    key: "email_election_reminders",
    label: "Election reminder the day before election day",
    description: "One email reminder to vote the day before election day.",
  },
  {
    key: "email_issue_updates",
    label: "Updates about the issues you saved",
    description: "Occasional emails when there is something important about the issues that matter most to you.",
  },
  {
    key: "email_member_newsletter",
    label: "Member newsletter",
    description:
      "Our member-only analysis reports and newsletters, for supporting members. Turning this off does not affect your membership.",
  },
];

export function EmailPreferenceToggles({ only }: { only?: Array<keyof EmailPreferences> }) {
  const queryClient = useQueryClient();
  // Optimistic overlay: the PUT saves all flags, so consecutive quick
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

  const labels = only ? ALL_LABELS.filter(({ key }) => only.includes(key)) : ALL_LABELS;

  return (
    <>
      {prefs.isPending ? <LoadingNotice text="Loading…" /> : null}
      {prefs.isError ? (
        <div className="mt-2">
          <ErrorNotice error={prefs.error} />
        </div>
      ) : null}
      {prefs.data ? (
        <div className="mt-3 space-y-2">
          {labels.map(({ key, label, description }) => {
            const current = pending ?? prefs.data;
            return (
              <label key={key} className="flex cursor-pointer items-start gap-2 text-sm text-ink">
                <input
                  type="checkbox"
                  checked={current[key]}
                  // Disabled while a save is in flight (cross-mount): the PUT
                  // replaces all flags, so concurrent requests could commit
                  // out of order and the earlier write would win. A filtered
                  // mount still PUTs the full object — untouched flags ride
                  // along unchanged.
                  disabled={saving}
                  onChange={(event) => {
                    const next = { ...current, [key]: event.target.checked };
                    setPending(next);
                    update.mutate(next);
                  }}
                  className="mt-0.5 h-4 w-4 accent-rausch"
                />
                <span>
                  {label}
                  {description ? <span className="block text-xs text-ink-soft">{description}</span> : null}
                </span>
              </label>
            );
          })}
        </div>
      ) : null}
      {update.isError ? (
        <div className="mt-2">
          <ErrorNotice error={update.error} />
        </div>
      ) : null}
    </>
  );
}
