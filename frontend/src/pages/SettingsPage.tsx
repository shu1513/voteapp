import { useState } from "react";
import { Link, useNavigate } from "react-router";
import { useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { apiRequest } from "@voteapp/api-client";
import type { EmailPreferences } from "@voteapp/api-client";
import { ErrorNotice, LoadingNotice } from "../components/Status";
import { SavedAddressForm } from "../components/SavedAddressForm";
import { purgeAccountScopedQueries, useMe, type Me } from "@voteapp/api-client";
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
            First Name
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

function HomeAddressSection() {
  return (
    <Section title="Your address">
      <p className="mt-1 text-sm text-ink-soft">
        Saving a new address replaces the districts on your{" "}
        <Link to="/me/ballot" className="underline hover:text-ink">
          saved ballot
        </Link>
        .
      </p>
      <SavedAddressForm inputId="settings-address" label="New address" />
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
          <HomeAddressSection />
          <EmailPreferencesSection />
          {/* The "issues you care about" editor lives on My Picks now,
              beside the pick cards it reorders. */}
          <p className="rounded-xl border border-line bg-surface p-4 text-sm text-ink-soft">
            Looking for your issue priorities? They moved to{" "}
            <Link to="/me/picks" className="underline hover:text-ink">
              My Picks
            </Link>
            .
          </p>
        </>
      ) : (
        <p className="rounded-xl border border-line bg-surface p-4 text-sm text-ink-soft">
          Verify your email to manage your address and notifications.
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
