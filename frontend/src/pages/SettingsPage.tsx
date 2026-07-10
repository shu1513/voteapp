import { useState } from "react";
import { Link, useNavigate } from "react-router";
import { useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  closestCenter,
  DndContext,
  KeyboardSensor,
  MouseSensor,
  TouchSensor,
  useSensor,
  useSensors,
  type DragEndEvent,
} from "@dnd-kit/core";
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { apiRequest } from "@voteapp/api-client";
import type { EmailPreferences, ResearchAreaCatalog, ResearchAreaPreferencesResult } from "@voteapp/api-client";
import { ErrorNotice, LoadingNotice } from "../components/Status";
import { purgeAccountScopedQueries, useMe, type Me } from "@voteapp/api-client";
import { MAX_RESEARCH_AREA_RANK } from "@voteapp/api-client";
import { useDocumentTitle } from "../lib/useDocumentTitle";

// Account settings. Sections mirror the backend's gating: profile, password,
// email change, sessions, and delete work for unverified users too (fixing a
// typo or leaving must not require a verified inbox); email preferences and
// research areas are verified-only and hidden until then.

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="rounded-xl border border-line bg-white p-4">
      <h2 className="text-lg font-semibold">{title}</h2>
      {children}
    </section>
  );
}

const inputClass =
  "mt-1 w-full rounded-md border border-line px-3 py-2 shadow-sm focus:border-ink focus:outline-none";
const buttonClass =
  "rounded-lg bg-rausch px-4 py-2 text-sm font-semibold text-white transition hover:bg-rausch-dark disabled:cursor-not-allowed disabled:bg-line";

function ProfileSection({ me }: { me: Me }) {
  const [firstName, setFirstName] = useState(me.first_name);
  const queryClient = useQueryClient();
  const update = useMutation({
    mutationFn: () =>
      apiRequest<{ user: Me }>("/api/me", { method: "PUT", body: { first_name: firstName.trim() } }),
    onSuccess: (response) => {
      queryClient.setQueryData(["me"], response.user);
    },
  });

  return (
    <Section title="Profile">
      <p className="mt-1 text-sm text-ink-soft">Signed in as {me.email}</p>
      <form
        onSubmit={(event) => {
          event.preventDefault();
          if (firstName.trim() && !update.isPending) {
            update.mutate();
          }
        }}
        className="mt-3 flex items-end gap-3"
      >
        <div className="grow">
          <label htmlFor="settings-first-name" className="block text-sm font-medium text-ink">
            First name
          </label>
          <input
            id="settings-first-name"
            type="text"
            value={firstName}
            maxLength={80}
            onChange={(event) => setFirstName(event.target.value)}
            className={inputClass}
          />
        </div>
        <button type="submit" disabled={!firstName.trim() || update.isPending} className={buttonClass}>
          {update.isSuccess && firstName === me.first_name ? "Saved" : "Save"}
        </button>
      </form>
      {update.isError ? (
        <div className="mt-2">
          <ErrorNotice error={update.error} />
        </div>
      ) : null}
    </Section>
  );
}

function PasswordSection() {
  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const change = useMutation({
    mutationFn: () =>
      apiRequest<{ status: string }>("/api/me/password", {
        method: "POST",
        body: { current_password: currentPassword, new_password: newPassword },
      }),
    onSuccess: () => {
      setCurrentPassword("");
      setNewPassword("");
    },
  });

  return (
    <Section title="Change password">
      <p className="mt-1 text-sm text-ink-soft">
        Changing your password signs you out everywhere else; this device stays logged in.
      </p>
      <form
        onSubmit={(event) => {
          event.preventDefault();
          if (currentPassword && newPassword && !change.isPending) {
            change.mutate();
          }
        }}
        className="mt-3 space-y-3"
      >
        <div>
          <label htmlFor="settings-current-password" className="block text-sm font-medium text-ink">
            Current password
          </label>
          <input
            id="settings-current-password"
            type="password"
            value={currentPassword}
            autoComplete="current-password"
            onChange={(event) => setCurrentPassword(event.target.value)}
            className={inputClass}
          />
        </div>
        <div>
          <label htmlFor="settings-new-password" className="block text-sm font-medium text-ink">
            New password
          </label>
          <input
            id="settings-new-password"
            type="password"
            value={newPassword}
            minLength={12}
            autoComplete="new-password"
            onChange={(event) => setNewPassword(event.target.value)}
            className={inputClass}
          />
          <p className="mt-1 text-xs text-ink-soft">At least 12 characters.</p>
        </div>
        <button type="submit" disabled={!currentPassword || !newPassword || change.isPending} className={buttonClass}>
          {change.isPending ? "Changing…" : change.isSuccess ? "Password changed" : "Change password"}
        </button>
      </form>
      {change.isError ? (
        <div className="mt-2">
          <ErrorNotice error={change.error} />
        </div>
      ) : null}
    </Section>
  );
}

function EmailSection({ me }: { me: Me }) {
  const [newEmail, setNewEmail] = useState("");
  const [password, setPassword] = useState("");
  const request = useMutation({
    mutationFn: () =>
      apiRequest<{ status: string }>("/api/me/email", {
        method: "POST",
        body: { new_email: newEmail.trim(), password },
      }),
  });

  if (request.isSuccess) {
    return (
      <Section title="Change email">
        <p className="mt-2 text-sm text-ink-soft">
          If <strong className="text-ink">{newEmail.trim()}</strong> is available, we sent it a confirmation
          link. Your address stays <strong className="text-ink">{me.email}</strong> until you open it.
        </p>
      </Section>
    );
  }

  return (
    <Section title="Change email">
      <form
        onSubmit={(event) => {
          event.preventDefault();
          if (newEmail.trim() && password && !request.isPending) {
            request.mutate();
          }
        }}
        className="mt-3 space-y-3"
      >
        <div>
          <label htmlFor="settings-new-email" className="block text-sm font-medium text-ink">
            New email
          </label>
          <input
            id="settings-new-email"
            type="email"
            value={newEmail}
            autoComplete="email"
            onChange={(event) => setNewEmail(event.target.value)}
            className={inputClass}
          />
        </div>
        <div>
          <label htmlFor="settings-email-password" className="block text-sm font-medium text-ink">
            Confirm with your password
          </label>
          <input
            id="settings-email-password"
            type="password"
            value={password}
            autoComplete="current-password"
            onChange={(event) => setPassword(event.target.value)}
            className={inputClass}
          />
        </div>
        <button type="submit" disabled={!newEmail.trim() || !password || request.isPending} className={buttonClass}>
          {request.isPending ? "Sending…" : "Send confirmation"}
        </button>
      </form>
      {request.isError ? (
        <div className="mt-2">
          <ErrorNotice error={request.error} />
        </div>
      ) : null}
    </Section>
  );
}

function EmailPreferencesSection() {
  const queryClient = useQueryClient();
  // Optimistic overlay: the PUT saves all three flags, so consecutive quick
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

  const labels: Array<{ key: keyof EmailPreferences; label: string; description?: string }> = [
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

  return (
    <Section title="Email notifications">
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
                  // replaces all three flags, so concurrent requests could
                  // commit out of order and the earlier write would win.
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
    </Section>
  );
}

function ResearchAreasSection() {
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
  // Mouse: drag starts after 4px of movement, so the remove button stays a
  // plain click. Touch: press-and-hold (200ms) then drag, so the list does
  // not hijack page scrolling. Keyboard sorting stays for accessibility.
  // MouseSensor + TouchSensor deliberately, not PointerSensor: PointerSensor
  // also claims touch input, which would bypass the press-and-hold delay.
  const sensors = useSensors(
    useSensor(MouseSensor, { activationConstraint: { distance: 4 } }),
    useSensor(TouchSensor, { activationConstraint: { delay: 200, tolerance: 5 } }),
    useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates })
  );

  if (catalog.isPending || prefs.isPending) {
    return (
      <Section title="Issues you care about">
        <LoadingNotice text="Loading…" />
      </Section>
    );
  }
  if (catalog.isError || prefs.isError) {
    return (
      <Section title="Issues you care about">
        <div className="mt-2">
          <ErrorNotice error={catalog.error ?? prefs.error} />
        </div>
      </Section>
    );
  }

  const areaById = new Map(catalog.data.research_areas.map((area) => [area.id, area]));
  // Server order is rank ASC NULLS LAST, so it is the editor order directly.
  const orderedIds = pending ?? prefs.data.preferences.map((preference) => preference.research_area_id);
  const selectedSet = new Set(orderedIds);
  const atCapacity = orderedIds.length >= MAX_RESEARCH_AREA_RANK;

  function save(nextIds: string[]) {
    // Controls disable while a PUT is in flight, but a drag that was already
    // in progress when the save started can still drop; committing it would
    // race the full-list replace, so it is discarded like any other locked
    // edit. Checked against the mutation cache, not the rendered `saving`
    // value: a handler created before the disabling re-render could otherwise
    // slip through in the same tick as another edit's mutate().
    if (queryClient.isMutating({ mutationKey: ["put-research-area-preferences"] }) > 0) {
      return;
    }
    setPending(nextIds);
    update.mutate(nextIds);
  }

  function onDragEnd(event: DragEndEvent) {
    const { active, over } = event;
    if (!over || active.id === over.id) {
      return;
    }
    const from = orderedIds.indexOf(String(active.id));
    const to = orderedIds.indexOf(String(over.id));
    if (from < 0 || to < 0) {
      return;
    }
    save(arrayMove(orderedIds, from, to));
  }

  return (
    <Section title="Issues you care about">
      <p className="mt-1 text-sm text-ink-soft">
        Drag to put what matters most at the top — #1 counts the most in your ballot ordering.
      </p>
      {orderedIds.length > 0 ? (
        <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={onDragEnd}>
          <SortableContext items={orderedIds} strategy={verticalListSortingStrategy}>
            <ol className="mt-3 space-y-1.5">
              {orderedIds.map((id, index) => (
                <SortableAreaRow
                  key={id}
                  id={id}
                  index={index}
                  name={areaById.get(id)?.name ?? "Unknown area"}
                  disabled={saving}
                  onRemove={() => save(orderedIds.filter((other) => other !== id))}
                />
              ))}
            </ol>
          </SortableContext>
        </DndContext>
      ) : (
        <p className="mt-3 text-sm text-ink-soft">Nothing selected yet — pick up to {MAX_RESEARCH_AREA_RANK} below.</p>
      )}
      <p className="mt-4 text-sm font-medium text-ink">
        Add issues{" "}
        <span className="font-normal text-ink-soft">
          ({orderedIds.length}/{MAX_RESEARCH_AREA_RANK})
        </span>
      </p>
      <div className="mt-2 flex flex-wrap gap-2">
        {catalog.data.research_areas
          .filter((area) => !selectedSet.has(area.id))
          .map((area) => (
            <button
              key={area.id}
              type="button"
              disabled={saving || atCapacity}
              onClick={() => save([...orderedIds, area.id])}
              title={area.description ?? undefined}
              className="rounded-lg border border-line bg-white px-3 py-1.5 text-xs text-ink transition hover:border-rausch disabled:cursor-not-allowed disabled:opacity-50"
            >
              {area.name}
            </button>
          ))}
      </div>
      {update.isError ? (
        <div className="mt-2">
          <ErrorNotice error={update.error} />
        </div>
      ) : null}
    </Section>
  );
}

function SortableAreaRow({
  id,
  index,
  name,
  disabled,
  onRemove,
}: {
  id: string;
  index: number;
  name: string;
  disabled: boolean;
  onRemove: () => void;
}) {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({ id, disabled });
  // The whole row is the drag surface — grab it anywhere with a mouse, or
  // press and hold on touch. The remove button still clicks normally thanks
  // to the sensors' activation constraints.
  return (
    <li
      ref={setNodeRef}
      {...attributes}
      {...listeners}
      style={{ transform: CSS.Transform.toString(transform), transition }}
      aria-label={`${name}, rank ${index + 1}. Drag to reorder.`}
      // touch-manipulation, not touch-none: the TouchSensor prevents
      // scrolling itself once its press-and-hold delay activates, so plain
      // touches on the list still scroll the page.
      className={`flex cursor-grab touch-manipulation items-center gap-2 rounded-lg border border-line bg-white px-2 py-2 text-sm select-none ${
        isDragging ? "z-10 shadow-md" : ""
      }`}
    >
      <span aria-hidden className="px-1 text-ink-soft">
        ⠿
      </span>
      <span className="w-6 shrink-0 text-center text-xs font-semibold text-rausch-dark">#{index + 1}</span>
      <span className="flex-1 text-ink">{name}</span>
      <button
        type="button"
        disabled={disabled}
        onClick={onRemove}
        aria-label={`Remove ${name}`}
        className="px-2 text-ink-soft hover:text-rausch-dark disabled:opacity-30"
      >
        ×
      </button>
    </li>
  );
}

function SessionsSection() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const logoutAll = useMutation({
    mutationFn: () => apiRequest<{ status: string }>("/api/auth/logout-all", { method: "POST", body: {} }),
    onSuccess: () => {
      queryClient.setQueryData(["me"], null);
      purgeAccountScopedQueries(queryClient);
      navigate("/");
    },
  });

  return (
    <Section title="Sessions">
      <p className="mt-1 text-sm text-ink-soft">Log out of every device, including this one.</p>
      <button
        type="button"
        disabled={logoutAll.isPending}
        onClick={() => logoutAll.mutate()}
        className="mt-3 rounded-lg border border-line bg-white px-4 py-2 text-sm font-semibold text-ink transition hover:border-rausch"
      >
        {logoutAll.isPending ? "Logging out…" : "Log out everywhere"}
      </button>
      {logoutAll.isError ? (
        <div className="mt-2">
          <ErrorNotice error={logoutAll.error} />
        </div>
      ) : null}
    </Section>
  );
}

function DangerSection() {
  const [password, setPassword] = useState("");
  const [confirming, setConfirming] = useState(false);
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const deleteAccount = useMutation({
    mutationFn: () => apiRequest<{ status: string }>("/api/me", { method: "DELETE", body: { password } }),
    onSuccess: () => {
      queryClient.setQueryData(["me"], null);
      purgeAccountScopedQueries(queryClient);
      navigate("/");
    },
  });

  return (
    <section className="rounded-xl border border-rausch/40 bg-rausch/5 p-4">
      <h2 className="text-lg font-semibold text-rausch-dark">Delete account</h2>
      <p className="mt-1 text-sm text-ink-soft">
        Permanently deletes your account, saved districts, follows, and preferences. This cannot be undone.
      </p>
      {!confirming ? (
        <button
          type="button"
          onClick={() => setConfirming(true)}
          className="mt-3 rounded-lg border border-rausch/60 bg-white px-4 py-2 text-sm font-semibold text-rausch-dark transition hover:bg-rausch/10"
        >
          Delete my account…
        </button>
      ) : (
        <form
          onSubmit={(event) => {
            event.preventDefault();
            if (password && !deleteAccount.isPending) {
              deleteAccount.mutate();
            }
          }}
          className="mt-3 space-y-3"
        >
          <div>
            <label htmlFor="settings-delete-password" className="block text-sm font-medium text-ink">
              Confirm with your password
            </label>
            <input
              id="settings-delete-password"
              type="password"
              value={password}
              autoComplete="current-password"
              onChange={(event) => setPassword(event.target.value)}
              className={inputClass}
            />
          </div>
          <div className="flex gap-3">
            <button
              type="submit"
              disabled={!password || deleteAccount.isPending}
              className="rounded-lg bg-rausch-dark px-4 py-2 text-sm font-semibold text-white transition hover:bg-rausch disabled:cursor-not-allowed disabled:bg-line"
            >
              {deleteAccount.isPending ? "Deleting…" : "Permanently delete"}
            </button>
            <button
              type="button"
              onClick={() => {
                setConfirming(false);
                setPassword("");
              }}
              className="rounded-lg border border-line bg-white px-4 py-2 text-sm font-semibold text-ink"
            >
              Cancel
            </button>
          </div>
        </form>
      )}
      {deleteAccount.isError ? (
        <div className="mt-2">
          <ErrorNotice error={deleteAccount.error} />
        </div>
      ) : null}
    </section>
  );
}

export function SettingsPage() {
  useDocumentTitle("Settings");
  const { me, isLoading } = useMe();

  if (isLoading || me === undefined) {
    return <LoadingNotice text="Loading…" />;
  }
  if (me === null) {
    return (
      <div className="mx-auto max-w-md px-4 py-10 text-center">
        <p className="text-ink-soft">Log in to manage your account.</p>
        <p className="mt-4">
          <Link
            to="/login"
            className="rounded-lg bg-rausch px-4 py-2 font-semibold text-white transition hover:bg-rausch-dark"
          >
            Log in
          </Link>
        </p>
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-2xl space-y-4 px-4 py-8">
      <h1 className="text-2xl font-bold">Settings</h1>
      <ProfileSection me={me} />
      {me.email_verified ? (
        <>
          <EmailPreferencesSection />
          <ResearchAreasSection />
        </>
      ) : (
        <p className="rounded-xl border border-line bg-surface p-4 text-sm text-ink-soft">
          Verify your email to manage notifications and issue preferences.
        </p>
      )}
      <PasswordSection />
      <EmailSection me={me} />
      <SessionsSection />
      <DangerSection />
    </div>
  );
}

export default SettingsPage;
