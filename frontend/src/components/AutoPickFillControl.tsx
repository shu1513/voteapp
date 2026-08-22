import { useState } from "react";
import { Link } from "react-router";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import {
  ApiError,
  apiRequest,
  useElectionChoiceSaving,
  useMyResearchAreas,
} from "@voteapp/api-client";
import type {
  AutoPickElectionResult,
  AutoPickReason,
  AutoPicksClearResult,
  AutoPicksResult,
  ElectionChoice,
  ElectionSummary,
} from "@voteapp/api-client";
import { MIN_AUTO_PICK_ISSUES } from "./AutoPickControl";

// Per-date auto-pick controls for the My Picks page
// (docs/plans/auto-pick-by-issues.md): each election-date card (and ballot
// sheet) gets its own "Auto-pick my empty picks by my issues" button that
// runs POST /api/me/auto-picks in fill_empty mode over THAT date's undecided
// races only, and — once that date has engine-owned rows — a "Clear auto
// picks" button that DELETEs with ?election_date= so other dates' auto picks
// survive. No result list here: the caller gets the per-election results via
// onResults and annotates its own race rows ("auto pick: not enough
// evidence"); per-race "why" details live on each election page's panel.

// Server-side cap on election_ids per request (MAX_AUTO_PICK_ELECTION_IDS);
// larger ballots run in sequential chunks.
const MAX_IDS_PER_REQUEST = 200;

// A choice that still renders a decision — the same predicate PicksPage's
// cards use to label a race decided vs "no pick yet".
function hasPick(choice: ElectionChoice | undefined): boolean {
  return choice !== undefined && (choice.picks.length > 0 || choice.measure_position !== null);
}

// Whether the engine has rows left to clear ON THIS DATE — display gating
// only. The clear itself is one server-side DELETE scoped to origin =
// 'auto' and this election date, so stale cache here can never unpick a
// row the user has since re-picked manually in another tab.
function hasClearableAutoPicks(choices: ElectionChoice[], date: string): boolean {
  return choices.some(
    (choice) =>
      choice.election_date === date &&
      (choice.picks.some((pick) => pick.origin === "auto") ||
        (choice.measure_position !== null && choice.measure_origin === "auto"))
  );
}

const REASON_LABELS: Record<AutoPickReason, string> = {
  insufficient_evidence: "not enough evidence",
  only_negative_evidence: "only unknowns left",
  tie: "a tie",
  all_vetoed: "all crossed your line",
  veto: "crossed your line",
  by_elimination: "picked by elimination",
  too_few_issues: `fewer than ${MIN_AUTO_PICK_ISSUES} ranked issues`,
  election_closed: "no longer open",
};

export function reasonLabel(reason: AutoPickReason | null): string {
  return reason === null ? "not enough evidence" : REASON_LABELS[reason];
}

export function AutoPickFillControl({
  date,
  elections,
  choices,
  choiceByElectionId,
  onResults,
}: {
  /** The election date this control owns. */
  date: string;
  /** This date's carded races only. */
  elections: ElectionSummary[];
  /** Every stored choice, for the date-scoped Clear gating. */
  choices: ElectionChoice[];
  choiceByElectionId: Map<string, ElectionChoice> | undefined;
  /** Fill results for this date, keyed by election id — null after a clear.
   * The caller annotates its race rows from this; nothing renders here. */
  onResults?: (byElectionId: Map<string, AutoPickElectionResult> | null) => void;
}) {
  const queryClient = useQueryClient();
  const { preferences, isLoading: preferencesLoading, isError: preferencesError } = useMyResearchAreas();
  const saving = useElectionChoiceSaving();
  const [prompt, setPrompt] = useState(false);

  const emptyElectionIds = elections
    .filter((election) => !hasPick(choiceByElectionId?.get(election.id)))
    .map((election) => election.id);
  const clearable = hasClearableAutoPicks(choices, date);

  const fill = useMutation({
    // Shares the choice-write key so every pick control disables together.
    mutationKey: ["set-election-choice"],
    // Drop the previous run's annotations up front: during the request (and
    // after a failure, whose partial writes the onSettled refetch surfaces)
    // an old "not enough evidence" must not read as the latest result.
    onMutate: () => onResults?.(null),
    mutationFn: async (electionIds: string[]) => {
      const all: AutoPickElectionResult[] = [];
      for (let start = 0; start < electionIds.length; start += MAX_IDS_PER_REQUEST) {
        const response = await apiRequest<AutoPicksResult>("/api/me/auto-picks", {
          method: "POST",
          body: { election_ids: electionIds.slice(start, start + MAX_IDS_PER_REQUEST), mode: "fill_empty" },
        });
        all.push(...response.results);
      }
      return all;
    },
    // onSettled, not onSuccess: the batch commits election by election, so a
    // failure partway through can leave real writes behind.
    onSettled: () => queryClient.invalidateQueries({ queryKey: ["me", "election-choices"] }),
    onSuccess: (all) => onResults?.(new Map(all.map((result) => [result.election_id, result]))),
  });

  const clear = useMutation({
    mutationKey: ["set-election-choice"],
    mutationFn: () =>
      apiRequest<AutoPicksClearResult>(
        `/api/me/auto-picks?election_date=${encodeURIComponent(date)}`,
        { method: "DELETE" }
      ),
    onSettled: () => queryClient.invalidateQueries({ queryKey: ["me", "election-choices"] }),
    onSuccess: () => onResults?.(null),
  });

  function onFill() {
    // Same rule as AutoPickControl: the issue-floor prompt only fires on a
    // LOADED list — on a failed fetch the backend's per-result
    // too_few_issues is the authority.
    if (!preferencesError && preferences.length < MIN_AUTO_PICK_ISSUES) {
      setPrompt(true);
      return;
    }
    setPrompt(false);
    fill.mutate(emptyElectionIds);
  }

  if (emptyElectionIds.length === 0 && !clearable) {
    return null;
  }

  return (
    <div className="mt-2 print:hidden">
      <div className="flex flex-wrap items-center gap-2">
        {emptyElectionIds.length > 0 ? (
          <button
            type="button"
            disabled={saving || preferencesLoading}
            onClick={onFill}
            className="rounded-lg border border-autopick-border bg-autopick px-3 py-1.5 text-sm font-semibold text-autopick-ink transition hover:bg-autopick-dark disabled:opacity-50"
          >
            {fill.isPending ? "Picking…" : "Auto-pick my empty picks by my issues"}
          </button>
        ) : null}
        {clearable ? (
          <button
            type="button"
            disabled={saving}
            onClick={() => clear.mutate()}
            className="rounded-lg border border-line bg-white px-3 py-1.5 text-sm font-medium text-ink transition hover:border-ink disabled:opacity-50"
          >
            {clear.isPending ? "Clearing…" : "Clear auto picks"}
          </button>
        ) : null}
      </div>
      <p className="mt-1 text-xs text-ink-soft">
        Picks the best match for your ranked issues in each race you haven't decided. Your own picks
        are never changed.
      </p>
      {prompt ? (
        <p role="status" className="mt-2 rounded-lg border border-line bg-surface px-3 py-2 text-sm text-ink-mid">
          Rank at least {MIN_AUTO_PICK_ISSUES} issues first, so the picks reflect what matters to you.{" "}
          <Link
            to="/me/settings"
            className="font-medium underline decoration-dotted underline-offset-2 hover:text-ink"
          >
            Rank your issues
          </Link>
        </p>
      ) : null}
      {fill.isError && !fill.isPending ? (
        // The partial-write warning applies to API errors too: the server
        // commits election by election (and the client sends chunks), so a
        // 429 or 500 partway through follows real writes. The rows below
        // were refetched onSettled and show the truth.
        <p role="alert" className="mt-2 text-sm font-medium text-red-800">
          {fill.error instanceof ApiError ? fill.error.message : "Couldn't finish filling."}{" "}
          Some races may already be filled — check the rows below.
        </p>
      ) : null}
      {clear.isError && !clear.isPending ? (
        // One atomic server-side DELETE: it either cleared everything or
        // nothing, so no partial-state warning here.
        <p role="alert" className="mt-2 text-sm font-medium text-red-800">
          Couldn't clear your auto picks — try again.
        </p>
      ) : null}
    </div>
  );
}
