import { useEffect, useState } from "react";
import { useNavigate } from "react-router";
import type { MetaFunction } from "react-router";
import { useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { APP_NAME, apiRequest, useMe } from "@voteapp/api-client";
import type { ResearchAreaCatalog, ResearchAreaPreferencesResult } from "@voteapp/api-client";
import { ResearchAreaPicker } from "../components/ResearchAreaPicker";
import { ErrorNotice, LoadingNotice } from "../components/Status";
import { markWelcomeSeen } from "../lib/welcomeSeen";
import { useDocumentTitle } from "../lib/useDocumentTitle";

export const meta: MetaFunction = () => [{ title: `Welcome · ${APP_NAME}` }];

// Post-signup onboarding step: pick the issues that drive ballot ordering.
// Login routes verified users here once, when they have no saved areas and
// have not skipped. Unlike settings, edits stay local and save as one PUT on
// "Save and continue" — a brand-new user exploring the list shouldn't fire a
// network write per tap.

export function WelcomePage() {
  useDocumentTitle("Welcome");
  const { me, isLoading, isError: meError, refetch: refetchMe } = useMe();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [orderedIds, setOrderedIds] = useState<string[]>([]);

  const catalog = useQuery({
    queryKey: ["research-areas"],
    queryFn: () => apiRequest<ResearchAreaCatalog>("/api/research-areas"),
    staleTime: 5 * 60_000,
  });

  const save = useMutation({
    // Same mutation key as the settings editor: both are full-list replaces
    // of the same resource, so their in-flight guards must see each other.
    mutationKey: ["put-research-area-preferences"],
    mutationFn: (ids: string[]) =>
      apiRequest<ResearchAreaPreferencesResult>("/api/me/research-area-preferences", {
        method: "PUT",
        body: { preferences: ids.map((research_area_id, index) => ({ research_area_id, rank: index + 1 })) },
      }),
    onSuccess: (saved) => {
      queryClient.setQueryData(["me", "research-area-preferences"], saved);
      // Saving completes the step just as firmly as skipping does: without
      // the flag, clearing every preference in settings later would make
      // the next login mistake this user for a brand-new one.
      if (me) {
        markWelcomeSeen(me.email);
      }
      // The saved ballot is server-sorted by these preferences, and the
      // ballot-preferences default can flip to my_areas on first save.
      void queryClient.invalidateQueries({ queryKey: ["me", "ballot"] });
      void queryClient.invalidateQueries({ queryKey: ["me", "ballot-preferences"] });
      // replace, not push: the step is transient — Back from the ballot
      // must not reopen a blank welcome screen.
      navigate("/me/ballot", { replace: true });
    },
  });
  // Cross-mount in-flight guard, same as the settings editor: these PUTs are
  // full-list replaces sharing one mutation key, so controls stay locked
  // until any older write settles. (Render value — saveAndContinue re-checks
  // the mutation cache imperatively to close the gap before this re-renders.)
  const saving = useIsMutating({ mutationKey: ["put-research-area-preferences"] }) > 0;

  function saveAndContinue() {
    // Checked against the mutation cache, not the rendered `saving` value: a
    // second click in the same tick as the first — or an in-flight settings
    // PUT from another mount — would otherwise race this full-list replace.
    if (queryClient.isMutating({ mutationKey: ["put-research-area-preferences"] }) > 0) {
      return;
    }
    save.mutate(orderedIds);
  }

  // The step needs a verified session (the preferences endpoint is
  // verified-only). Anyone else lands somewhere sensible instead of a wall:
  // logged-out visitors at login, unverified users at their ballot, where
  // the existing verification interstitial takes over.
  useEffect(() => {
    if (me === null) {
      navigate("/login", { replace: true });
    } else if (me && !me.email_verified) {
      navigate("/me/ballot", { replace: true });
    }
  }, [me, navigate]);

  if (meError) {
    // /api/me failed for a non-auth reason (network, 5xx): without this the
    // !me guard below would spin forever. Same recovery as the saved ballot.
    return (
      <div className="mx-auto max-w-md px-4 py-10 space-y-4 text-center">
        <p className="text-ink-soft">We couldn't check your session. Please try again.</p>
        <button
          type="button"
          onClick={() => void refetchMe()}
          className="rounded-lg bg-rausch px-4 py-2 font-semibold text-white transition hover:bg-rausch-dark"
        >
          Retry
        </button>
      </div>
    );
  }
  if (isLoading || !me || !me.email_verified) {
    return (
      <div className="mx-auto max-w-md px-4 py-10">
        <LoadingNotice text="Loading…" />
      </div>
    );
  }

  function skip() {
    if (me) {
      markWelcomeSeen(me.email);
    }
    navigate("/me/ballot", { replace: true });
  }

  return (
    <div className="mx-auto max-w-2xl px-4 py-10">
      <h1 className="text-2xl font-bold">
        {me.first_name ? `Welcome, ${me.first_name}!` : "Welcome!"}
      </h1>
      <p className="mt-2 text-sm text-ink-soft">
        Pick the issues you care about and we'll put the elections and candidates that touch them first.
        Drag to rank them in order of priority. You can change this any time in Settings.
      </p>

      {catalog.isPending ? <LoadingNotice text="Loading issues…" /> : null}
      {catalog.isError ? (
        <div className="mt-4">
          <ErrorNotice error={catalog.error} />
        </div>
      ) : null}
      {catalog.isSuccess ? (
        <ResearchAreaPicker
          areas={catalog.data.research_areas}
          orderedIds={orderedIds}
          disabled={saving}
          onChange={setOrderedIds}
        />
      ) : null}

      <div className="mt-8 flex items-center gap-4">
        <button
          type="button"
          disabled={orderedIds.length === 0 || saving}
          onClick={saveAndContinue}
          className="rounded-lg bg-rausch px-4 py-2 font-semibold text-white transition hover:bg-rausch-dark disabled:cursor-not-allowed disabled:bg-line"
        >
          {save.isPending ? "Saving…" : "Save and continue"}
        </button>
        <button type="button" onClick={skip} className="text-sm text-ink-soft underline hover:text-ink">
          Skip for now
        </button>
      </div>
      {save.isError ? (
        <div className="mt-4">
          <ErrorNotice error={save.error} />
        </div>
      ) : null}
    </div>
  );
}

export default WelcomePage;
