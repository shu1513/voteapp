import {
  ApiError,
  hasClearableAutoPicks,
  isDecidedChoice,
  joinNames,
  MIN_AUTO_PICK_ISSUES,
  summarizeAutoPick,
  useAutoPick,
  useAutoPickFill,
  useClearAutoPicks,
  useElectionChoiceSaving,
  useMe,
  useMyResearchAreas,
} from "@voteapp/api-client";
import type {
  AutoPickCandidateReport,
  AutoPickElectionResult,
  ElectionChoice,
  ElectionSummary,
} from "@voteapp/api-client";
import { useRouter } from "expo-router";
import { useState } from "react";
import { Pressable, Text, View } from "react-native";
import { Collapsible } from "./Collapsible";

// "Pick by my issues", ported from the web AutoPickControl /
// AutoPickFillControl pair. The engine copy, issue floor, and fill/clear
// mutations are shared via @voteapp/api-client; this file is the RN widgets.
// Mobile difference: the "Why this pick" panel keeps its headline sentence
// always visible (the honest answer must never hide) but folds the
// per-candidate detail into the existing Collapsible — the full report runs
// long on a phone screen. Callers gate rendering on showChoiceControls, so
// only signed-in viewers who can pick in the race ever see these.

function runErrorMessage(error: unknown, fallback: string): string {
  return error instanceof ApiError ? error.message : fallback;
}

// The issue-floor prompt both controls share: rank at least N issues first,
// with the link into the mobile issue editor.
function RankIssuesPrompt({ plural }: { plural: boolean }) {
  const router = useRouter();
  return (
    <View accessibilityLiveRegion="polite" className="rounded-lg border border-line bg-surface px-3 py-2">
      <Text className="text-sm text-ink">
        Rank at least {MIN_AUTO_PICK_ISSUES} issues first, so the {plural ? "picks reflect" : "pick reflects"} what
        matters to you.{" "}
        <Text
          className="font-medium underline"
          accessibilityRole="link"
          onPress={() => router.push("/settings/research-areas")}
        >
          Rank your issues
        </Text>
      </Text>
    </View>
  );
}

type AutoPickControlProps = {
  electionId: string;
  /** elections.seats_to_fill — null renders as a single seat (office races);
   * pass null for measures. Lets the panel flag a partial fill: "picked"
   * with fewer names than seats must not read as a finished race. */
  seatsToFill: number | null;
  /** Smaller pill (measure section's mid-page placement) — the Yes/No pair
   * on the footer card stays the screen's loud control. */
  compact?: boolean;
};

/**
 * Race-level "Auto-pick by my issues": one button that runs the engine for
 * this election (mode replace — a re-run refreshes the pick) and opens the
 * "Why this pick" panel built from the response.
 */
export function AutoPickControl({ electionId, seatsToFill, compact = false }: AutoPickControlProps) {
  const { me } = useMe();
  const { preferences, isLoading: preferencesLoading, isError: preferencesError } = useMyResearchAreas();
  const autoPick = useAutoPick();
  const saving = useElectionChoiceSaving();
  const [prompt, setPrompt] = useState<"rank_issues" | null>(null);
  const [result, setResult] = useState<AutoPickElectionResult | null>(null);

  const areaNames = new Map(preferences.map((preference) => [preference.research_area_id, preference.name]));
  const areaName = (researchAreaId: string) => areaNames.get(researchAreaId) ?? "one of your issues";
  // Highest priority first (explicit ranks, then legacy unranked) — the
  // same order the engine scores in.
  const issueOrder = [...preferences]
    .sort((a, b) => (a.rank ?? Number.MAX_SAFE_INTEGER) - (b.rank ?? Number.MAX_SAFE_INTEGER))
    .map((preference) => preference.research_area_id);

  // Signed-out (null) and still-resolving (undefined) sessions render
  // nothing — same rule as the web control (callers also gate on
  // showChoiceControls, so this is belt and suspenders).
  if (me == null) {
    return null;
  }

  function onPress() {
    setResult(null);
    // The issue-floor prompt only fires on a LOADED list: a failed fetch
    // returns the same empty array, and telling a user with five ranked
    // issues to go rank issues would be wrong — on error the backend's
    // per-result too_few_issues is the authority (the panel renders it).
    if (!preferencesError && preferences.length < MIN_AUTO_PICK_ISSUES) {
      setPrompt("rank_issues");
      return;
    }
    setPrompt(null);
    autoPick.mutate(
      { election_ids: [electionId], mode: "replace" },
      { onSuccess: (response) => setResult(response.results[0] ?? null) }
    );
  }

  // Disabled while the preferences load: pressing then would hit the
  // issue-floor check against a still-empty list and misdirect a ready user
  // to the issue editor.
  const disabled = saving || preferencesLoading;
  return (
    <View className="gap-2">
      <Pressable
        disabled={disabled}
        onPress={onPress}
        accessibilityRole="button"
        accessibilityState={{ disabled }}
        className={`self-start rounded-full border border-autopick-border bg-autopick active:bg-autopick-dark ${
          compact ? "px-2.5 py-1" : "px-3 py-1.5"
        }${disabled ? " opacity-50" : ""}`}
      >
        <Text className={`font-semibold text-autopick-ink ${compact ? "text-xs" : "text-sm"}`}>
          {autoPick.isPending ? "Picking…" : "Auto-pick by my issues"}
        </Text>
      </Pressable>
      {prompt === "rank_issues" ? <RankIssuesPrompt plural={false} /> : null}
      {autoPick.isError && !autoPick.isPending ? (
        <Text accessibilityLiveRegion="polite" className="text-sm font-medium text-red-800">
          {runErrorMessage(autoPick.error, "Couldn't run the pick — check your connection and try again.")}
        </Text>
      ) : null}
      {result !== null ? (
        <WhyThisPickPanel
          result={result}
          seatsToFill={seatsToFill}
          areaName={areaName}
          issueOrder={issueOrder}
          onDismiss={() => setResult(null)}
        />
      ) : null}
    </View>
  );
}

/** Up to this many aligned issues are named in the headline itself. */
const INLINE_ISSUE_LIMIT = 3;

// Per-issue alignment, summarized (port of the web IssueAlignment): "aligned
// on 13 of your 16 issues", the exceptions (conflicts / mixed) named right
// away because that is what a voter needs to check, and the full grouped
// list behind a toggle. The old form listed every issue with its own
// "· aligned" — sixteen repeats of the same word was a wall. Every list
// keeps the user's priority order. Text can only nest Text in React Native,
// so the headline toggle is a Pressable Text rather than a button with an
// svg chevron; the glyph flips to show state.
function IssueAlignment({
  perIssue,
  issueOrder,
  areaName,
}: {
  perIssue: { research_area_id: string; net: number }[];
  issueOrder: string[];
  areaName: (id: string) => string;
}) {
  const [open, setOpen] = useState(false);
  const rank = new Map(issueOrder.map((id, index) => [id, index]));
  const byRank = (a: { research_area_id: string }, b: { research_area_id: string }) =>
    (rank.get(a.research_area_id) ?? issueOrder.length) - (rank.get(b.research_area_id) ?? issueOrder.length);
  const names = (issues: { research_area_id: string }[]) =>
    [...issues].sort(byRank).map((issue) => areaName(issue.research_area_id));
  const aligned = names(perIssue.filter((issue) => issue.net > 0));
  const conflicts = names(perIssue.filter((issue) => issue.net < 0));
  const mixed = names(perIssue.filter((issue) => issue.net === 0));
  const total = issueOrder.length;
  // Few aligned issues (1–3): name them in the headline — shorter than a
  // count, and nothing is left to expand since conflicts/mixed are always
  // named below. More: the count, with the names behind the chevron.
  const nameInline = aligned.length > 0 && aligned.length <= INLINE_ISSUE_LIMIT;
  const headline = nameInline
    ? `aligned on ${joinNames(aligned)}`
    : total === 0
      ? `aligned on ${aligned.length} issue${aligned.length === 1 ? "" : "s"}`
      : aligned.length === total
        ? `aligned on all ${total} of your issues`
        : `aligned on ${aligned.length} of your ${total} issues`;
  // Same rule for the exceptions: a short list is named, a long one is
  // counted with the names behind the chevron.
  const inline = (label: string, names: string[]) =>
    names.length <= INLINE_ISSUE_LIMIT ? `${label}: ${joinNames(names)}` : `${label} on ${names.length} issues`;
  const expandable = [aligned, conflicts, mixed].some((names) => names.length > INLINE_ISSUE_LIMIT);
  return (
    <>
      {/* The headline is the toggle: tap it to see the issue names. One
          target, no orphan "Show issues" line. */}
      {expandable ? (
        <Text
          accessibilityRole="button"
          accessibilityState={{ expanded: open }}
          onPress={() => setOpen((previous) => !previous)}
          className="font-semibold text-green-900"
        >
          {headline} {open ? "▴" : "▾"}
        </Text>
      ) : (
        <Text className="font-semibold text-green-900">{headline}</Text>
      )}
      {conflicts.length > 0 || mixed.length > 0 ? (
        <Text>
          {"\n"}
          {conflicts.length > 0 ? (
            <Text className="font-medium text-red-900">{inline("Conflicts", conflicts)}</Text>
          ) : null}
          {conflicts.length > 0 && mixed.length > 0 ? <Text className="text-ink-soft"> · </Text> : null}
          {mixed.length > 0 ? <Text className="font-medium text-amber-900">{inline("Mixed", mixed)}</Text> : null}
        </Text>
      ) : null}
      {expandable && open ? (
        // Each group in its own color, same tier as the headline — the names
        // are the payload here, not a footnote.
        <Text className="font-medium">
          {aligned.length > 0 ? (
            <Text className="text-green-700">
              {"\n"}
              <Text className="font-semibold text-green-900">Aligned:</Text> {aligned.join(", ")}
            </Text>
          ) : null}
          {conflicts.length > 0 ? (
            <Text className="text-red-700">
              {"\n"}
              <Text className="font-semibold text-red-900">Conflicts:</Text> {conflicts.join(", ")}
            </Text>
          ) : null}
          {mixed.length > 0 ? (
            <Text className="text-amber-700">
              {"\n"}
              <Text className="font-semibold text-amber-900">Mixed:</Text> {mixed.join(", ")}
            </Text>
          ) : null}
        </Text>
      ) : null}
    </>
  );
}

function WhyThisPickPanel({
  result,
  seatsToFill,
  areaName,
  issueOrder,
  onDismiss,
}: {
  result: AutoPickElectionResult;
  seatsToFill: number | null;
  areaName: (researchAreaId: string) => string;
  /** The user's ranked issue ids, highest priority first. Orders every
   * list in the panel and supplies the "of your N issues" denominator. */
  issueOrder: string[];
  onDismiss: () => void;
}) {
  const pickedReports = result.picked_candidate_ids
    .map((id) => result.candidates.find((report) => report.candidate_id === id))
    .filter((report): report is AutoPickCandidateReport => report !== undefined);
  const vetoedReports = result.candidates.filter((report) => report.vetoed_by.length > 0);
  const hasDetails =
    result.measure_per_issue.length > 0 ||
    pickedReports.length > 0 ||
    vetoedReports.length > 0 ||
    result.unresearched.length > 0;

  return (
    <View
      accessibilityLabel="Why this pick"
      className="rounded-xl border border-line bg-surface/50 p-4"
    >
      <View className="flex-row items-start justify-between gap-2">
        <Text className="flex-1 text-sm font-medium text-ink">{summarizeAutoPick(result, seatsToFill)}</Text>
        <Pressable onPress={onDismiss} accessibilityRole="button">
          <Text className="text-xs font-medium text-ink-soft underline">Hide</Text>
        </Pressable>
      </View>
      {hasDetails ? (
        // The web panel shows the full report inline; on a phone it can run
        // several paragraphs, so the detail folds behind the shared
        // disclosure. Only the headline above is load-bearing.
        <Collapsible summary="Details">
          <View className="mt-2 gap-2">
            {result.race_type === "ballot_measure" && result.measure_per_issue.length > 0 ? (
              <Text className="text-sm text-ink">
                <Text className="font-medium text-ink-soft">On your issues: </Text>
                <IssueAlignment perIssue={result.measure_per_issue} issueOrder={issueOrder} areaName={areaName} />
              </Text>
            ) : null}
            {pickedReports.map((report) => (
              <Text key={report.candidate_id} className="text-sm text-ink">
                <Text className="font-medium">{report.display_name}</Text>
                {report.per_issue.length > 0 ? (
                  <>
                    {" — "}
                    <IssueAlignment perIssue={report.per_issue} issueOrder={issueOrder} areaName={areaName} />
                  </>
                ) : (
                  <Text className="text-ink-soft"> — no records on your issues (picked by elimination)</Text>
                )}
              </Text>
            ))}
            {vetoedReports.map((report) => (
              <Text key={report.candidate_id} className="text-sm text-red-900">
                <Text className="font-medium">{report.display_name}</Text> excluded — crossed your line on{" "}
                {joinNames([...new Set(report.vetoed_by.map((veto) => areaName(veto.research_area_id)))])}:{" "}
                <Text className="text-ink-soft">
                  {report.vetoed_by[0]?.description}
                  {report.vetoed_by.length > 1 ? ` (and ${report.vetoed_by.length - 1} more)` : ""}
                </Text>
              </Text>
            ))}
            {result.unresearched.length > 0 ? (
              // Transparency requirement: the comparison was partial, and the
              // user must see who was missing and why (never researched vs
              // researched with nothing found on their issues).
              <Text className="text-sm text-ink-soft">
                <Text className="font-medium">Not compared: </Text>
                {result.unresearched.map((entry, index) => (
                  <Text key={entry.candidate_id}>
                    {entry.display_name} ({entry.never_researched ? "not researched yet" : "no records on your issues"})
                    {index < result.unresearched.length - 1 ? ", " : null}
                  </Text>
                ))}
              </Text>
            ) : null}
          </View>
        </Collapsible>
      ) : null}
    </View>
  );
}

const FILL_DESCRIPTION =
  "Picks the best match for your ranked issues in each race you haven't decided. Your own picks are never changed.";

/**
 * Per-date fill + clear for the My Draft date cards: "Auto-fill empty picks
 * by my issues" runs fill_empty over THAT date's undecided races only. Once
 * the date has engine-owned rows the fill button gives way to a "Clear auto
 * picks" button that removes them (date-scoped, so other dates' auto picks
 * survive) — never both at once: a rerun over the races the engine already
 * left open repeats the same "not enough evidence", so Clear → fill again is
 * the useful path (same rule as the web control). No result list
 * here: the caller gets the per-election results via onResults and
 * renders its own one-line run summary; per-race "why" details live on each
 * election screen's panel.
 */
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
  /** Fill results for this date, keyed by election id — null after a clear. */
  onResults?: (byElectionId: Map<string, AutoPickElectionResult> | null) => void;
}) {
  const { preferences, isLoading: preferencesLoading, isError: preferencesError } = useMyResearchAreas();
  const saving = useElectionChoiceSaving();
  const [prompt, setPrompt] = useState(false);
  const fill = useAutoPickFill(onResults);
  const clear = useClearAutoPicks(onResults);

  const emptyElectionIds = elections
    .filter((election) => !isDecidedChoice(choiceByElectionId?.get(election.id)))
    .map((election) => election.id);
  const clearable = hasClearableAutoPicks(choices, date);
  const fillable = emptyElectionIds.length > 0 && !clearable;

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

  if (!fillable && !clearable) {
    return null;
  }

  const fillDisabled = saving || preferencesLoading;
  return (
    <View className="mt-2">
      <View className="flex-row flex-wrap items-center gap-2">
        {fillable ? (
          // The one-line explanation rides as the button's accessibility
          // hint, not visible copy (the web moved it to a tooltip +
          // sr-only): the label already says what the button does, and the
          // card stays uncluttered. The Auto chips on the rows say what
          // Clear removes.
          <Pressable
            disabled={fillDisabled}
            onPress={onFill}
            accessibilityRole="button"
            accessibilityState={{ disabled: fillDisabled }}
            accessibilityHint={FILL_DESCRIPTION}
            className={`rounded-full border border-autopick-border bg-autopick px-3 py-1.5 active:bg-autopick-dark${fillDisabled ? " opacity-50" : ""}`}
          >
            <Text className="text-sm font-semibold text-autopick-ink">
              {fill.isPending ? "Picking…" : "Auto-fill empty picks by my issues"}
            </Text>
          </Pressable>
        ) : null}
        {clearable ? (
          <Pressable
            disabled={saving}
            onPress={() => clear.mutate(date)}
            accessibilityRole="button"
            accessibilityState={{ disabled: saving }}
            className={`rounded-lg border border-line bg-white px-3 py-1.5 active:border-ink${saving ? " opacity-50" : ""}`}
          >
            <Text className="text-sm font-medium text-ink">
              {clear.isPending ? "Clearing…" : "Clear auto picks"}
            </Text>
          </Pressable>
        ) : null}
      </View>
      {prompt ? (
        <View className="mt-2">
          <RankIssuesPrompt plural />
        </View>
      ) : null}
      {fill.isError && !fill.isPending ? (
        // The partial-write warning applies to API errors too: the server
        // commits election by election (and the client sends chunks), so a
        // 429 or 500 partway through follows real writes. The rows below
        // were refetched onSettled and show the truth.
        <Text accessibilityLiveRegion="polite" className="mt-2 text-sm font-medium text-red-800">
          {runErrorMessage(fill.error, "Couldn't finish filling.")} Some races may already be filled — check the rows
          below.
        </Text>
      ) : null}
      {clear.isError && !clear.isPending ? (
        // One atomic server-side DELETE: it either cleared everything or
        // nothing, so no partial-state warning here.
        <Text accessibilityLiveRegion="polite" className="mt-2 text-sm font-medium text-red-800">
          Couldn&apos;t clear your auto picks — try again.
        </Text>
      ) : null}
    </View>
  );
}
