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
  AutoPicksResult,
  ElectionChoice,
  ElectionSummary,
} from "@voteapp/api-client";
import { MIN_AUTO_PICK_ISSUES } from "./AutoPickControl";

// My Picks batch controls for the auto-pick engine
// (docs/plans/auto-pick-by-issues.md): "Fill my empty picks" runs
// POST /api/me/auto-picks in fill_empty mode over every upcoming carded race
// without a pick, then reports what was filled and, race by race, why the
// rest were left open. "Clear auto picks" unpicks the rows the engine wrote
// (origin = 'auto') through the existing choice PUT — manual picks are never
// touched. Per-race "why" details live on each election page's panel; this
// control keeps to the batch summary.

// Server-side cap on election_ids per request (MAX_AUTO_PICK_ELECTION_IDS);
// larger ballots run in sequential chunks.
const MAX_IDS_PER_REQUEST = 200;

// A choice that still renders a decision — the same predicate PicksPage's
// cards use to label a race decided vs "no pick yet".
function hasPick(choice: ElectionChoice | undefined): boolean {
  return choice !== undefined && (choice.picks.length > 0 || choice.measure_position !== null);
}

// One row the engine wrote, addressed the way the choice PUT unpicks it.
type AutoRow =
  | { election_id: string; candidate_id: string }
  | { election_id: string; measure: true };

function autoRows(choices: ElectionChoice[], today: string): AutoRow[] {
  const rows: AutoRow[] = [];
  // Upcoming only: the choice write path refuses closed elections, so a
  // past auto pick is history now — clearing must not try (and fail) to
  // touch it.
  for (const choice of choices.filter((entry) => entry.election_date >= today)) {
    for (const pick of choice.picks) {
      if (pick.origin === "auto") {
        rows.push({ election_id: choice.election_id, candidate_id: pick.candidate_id });
      }
    }
    if (choice.measure_position !== null && choice.measure_origin === "auto") {
      rows.push({ election_id: choice.election_id, measure: true });
    }
  }
  return rows;
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

function reasonLabel(reason: AutoPickReason | null): string {
  return reason === null ? "not enough evidence" : REASON_LABELS[reason];
}

// "Filled 6 · 31 left open — 27 not enough evidence, 3 a tie, 1 all crossed
// your line." skipped_existing only appears when a pick landed between the
// page load and the click; it reads as already picked.
function summaryLine(results: AutoPickElectionResult[]): string {
  const filled = results.filter((result) => result.outcome === "picked").length;
  const existing = results.filter((result) => result.outcome === "skipped_existing").length;
  const open = results.filter((result) => result.outcome === "no_pick");
  const parts = [`Filled ${filled}`];
  if (existing > 0) {
    parts.push(`${existing} already picked`);
  }
  if (open.length === 0) {
    return parts.join(" · ") + ".";
  }
  const byReason = new Map<string, number>();
  for (const result of open) {
    const label = reasonLabel(result.reason);
    byReason.set(label, (byReason.get(label) ?? 0) + 1);
  }
  const breakdown = [...byReason.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([label, count]) => `${count} ${label}`)
    .join(", ");
  parts.push(`${open.length} left open — ${breakdown}`);
  return parts.join(" · ") + ".";
}

export function AutoPickFillControl({
  elections,
  choices,
  choiceByElectionId,
  today,
}: {
  /** Upcoming carded races (the ballot payload filtered to today or later). */
  elections: ElectionSummary[];
  /** Every stored choice, for the auto rows to clear. */
  choices: ElectionChoice[];
  choiceByElectionId: Map<string, ElectionChoice> | undefined;
  today: string;
}) {
  const queryClient = useQueryClient();
  const { preferences, isLoading: preferencesLoading, isError: preferencesError } = useMyResearchAreas();
  const saving = useElectionChoiceSaving();
  const [prompt, setPrompt] = useState(false);
  const [results, setResults] = useState<AutoPickElectionResult[] | null>(null);

  const emptyElectionIds = elections
    .filter((election) => !hasPick(choiceByElectionId?.get(election.id)))
    .map((election) => election.id);
  const titleByElectionId = new Map(
    elections.map((election) => [election.id, election.official_ballot_title])
  );
  const clearableRows = autoRows(choices, today);

  const fill = useMutation({
    // Shares the choice-write key so every pick control disables together.
    mutationKey: ["set-election-choice"],
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
    onSuccess: (all) => setResults(all),
  });

  const clear = useMutation({
    mutationKey: ["set-election-choice"],
    mutationFn: async (rows: AutoRow[]) => {
      for (const row of rows) {
        await apiRequest("/api/me/election-choices", {
          method: "PUT",
          body:
            "measure" in row
              ? { election_id: row.election_id, measure_position: null }
              : { election_id: row.election_id, candidate_id: row.candidate_id, chosen: false },
        });
      }
    },
    onSettled: () => queryClient.invalidateQueries({ queryKey: ["me", "election-choices"] }),
    onSuccess: () => setResults(null),
  });

  function onFill() {
    setResults(null);
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

  if (emptyElectionIds.length === 0 && clearableRows.length === 0 && results === null) {
    return null;
  }

  const openRaces = (results ?? []).filter((result) => result.outcome === "no_pick");
  const partialTies = (results ?? []).filter(
    (result) => result.outcome === "picked" && result.reason === "tie"
  );

  return (
    <section aria-label="Fill my empty picks" className="mt-4 rounded-xl border border-line bg-white p-4">
      <div className="flex flex-wrap items-center gap-2">
        {emptyElectionIds.length > 0 ? (
          <button
            type="button"
            disabled={saving || preferencesLoading}
            onClick={onFill}
            className="rounded-lg border border-line bg-white px-3 py-1.5 text-sm font-semibold text-ink transition hover:border-ink disabled:opacity-50"
          >
            {fill.isPending
              ? "Picking…"
              : `Fill my empty picks (${emptyElectionIds.length})`}
          </button>
        ) : null}
        {clearableRows.length > 0 ? (
          <button
            type="button"
            disabled={saving}
            onClick={() => clear.mutate(clearableRows)}
            className="rounded-lg border border-line bg-white px-3 py-1.5 text-sm font-medium text-ink-soft transition hover:border-ink hover:text-ink disabled:opacity-50"
          >
            {clear.isPending ? "Clearing…" : "Clear auto picks"}
          </button>
        ) : null}
        <span className="text-xs text-ink-soft">
          Picks the best match for your ranked issues in each race you haven't decided. Your own picks
          are never changed.
        </span>
      </div>
      {prompt ? (
        <p className="mt-2 text-sm text-ink-soft">
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
        <p role="alert" className="mt-2 text-sm font-medium text-red-800">
          {fill.error instanceof ApiError
            ? fill.error.message
            : "Couldn't finish filling — some races may have been picked. Check the cards below."}
        </p>
      ) : null}
      {clear.isError && !clear.isPending ? (
        <p role="alert" className="mt-2 text-sm font-medium text-red-800">
          Couldn't clear every auto pick — check the cards below and try again.
        </p>
      ) : null}
      {results !== null ? (
        <div className="mt-2 text-sm text-ink">
          <p className="font-medium">{summaryLine(results)}</p>
          {openRaces.length > 0 || partialTies.length > 0 ? (
            // The skipped races, each with its one-line reason; the election
            // page's own "Pick for me" panel carries the full evidence.
            <ul className="mt-1 space-y-1">
              {partialTies.map((result) => (
                <li key={result.election_id} className="text-ink-soft">
                  <Link
                    to={`/elections/${result.election_id}`}
                    className="underline decoration-dotted underline-offset-2 hover:text-ink"
                  >
                    {titleByElectionId.get(result.election_id) ?? "A race"}
                  </Link>{" "}
                  — filled some seats; the rest are a tie, your call.
                </li>
              ))}
              {openRaces.map((result) => (
                <li key={result.election_id} className="text-ink-soft">
                  <Link
                    to={`/elections/${result.election_id}`}
                    className="underline decoration-dotted underline-offset-2 hover:text-ink"
                  >
                    {titleByElectionId.get(result.election_id) ?? "A race"}
                  </Link>{" "}
                  — {reasonLabel(result.reason)}
                </li>
              ))}
            </ul>
          ) : null}
        </div>
      ) : null}
    </section>
  );
}
