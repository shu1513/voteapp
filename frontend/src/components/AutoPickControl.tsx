import { useState } from "react";
import { Link } from "react-router";
import {
  ApiError,
  useAutoPick,
  useElectionChoiceSaving,
  useMe,
  useMyResearchAreas,
} from "@voteapp/api-client";
import type { AutoPickCandidateReport, AutoPickElectionResult } from "@voteapp/api-client";

// "Pick by my issues": one button that runs the auto-pick engine for this election
// (POST /api/me/auto-picks, mode replace) and opens a "Why this pick" panel
// built from the response — winner, per-issue alignment, vetoed and
// unresearched candidates, and the honest "no pick" reason when nothing
// qualified. Spec: docs/plans/auto-pick-by-issues.md.
//
// Mirrors the server's UX floor (MIN_AUTO_PICK_ISSUES): below 3 ranked
// issues the button explains what to do instead of calling the API. Guests
// get a sign-in prompt — issue preferences are account-only.
export const MIN_AUTO_PICK_ISSUES = 3;

type AutoPickControlProps = {
  electionId: string;
  /** elections.seats_to_fill — null renders as a single seat (office races);
   * pass null for measures. Lets the panel flag a partial fill: "picked"
   * with fewer names than seats must not read as a finished race. */
  seatsToFill: number | null;
  /** Render as `display: contents` so the button joins the parent's flex row
   * (the measure card's "My pick: Yes/No" row) while the prompts and the
   * "Why this pick" panel — all w-full — wrap onto their own lines. */
  inline?: boolean;
};

function joinNames(names: string[]): string {
  if (names.length <= 1) {
    return names[0] ?? "";
  }
  return `${names.slice(0, -1).join(", ")} and ${names[names.length - 1]}`;
}

export function AutoPickControl({ electionId, seatsToFill, inline = false }: AutoPickControlProps) {
  const { me } = useMe();
  const { preferences, isLoading: preferencesLoading, isError: preferencesError } = useMyResearchAreas();
  const autoPick = useAutoPick();
  const saving = useElectionChoiceSaving();
  const [prompt, setPrompt] = useState<"sign_in" | "rank_issues" | null>(null);
  const [result, setResult] = useState<AutoPickElectionResult | null>(null);

  const areaNames = new Map(preferences.map((preference) => [preference.research_area_id, preference.name]));
  const areaName = (researchAreaId: string) => areaNames.get(researchAreaId) ?? "one of your issues";

  function onClick() {
    setResult(null);
    if (me === null) {
      setPrompt("sign_in");
      return;
    }
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

  return (
    <div className={inline ? "contents" : "flex flex-col gap-2"}>
      <span>
        <button
          type="button"
          title="Picks the candidate whose record best aligns with my issues, in the order I ranked them"
          // Disabled while the preferences load: clicking then would hit the
          // issue-floor check against a still-empty list and misdirect a
          // ready user to the issue editor.
          disabled={saving || preferencesLoading}
          onClick={onClick}
          className="rounded-full border border-autopick-border bg-autopick px-3 py-1.5 text-sm font-semibold text-autopick-ink transition hover:bg-autopick-dark disabled:opacity-50"
        >
          {autoPick.isPending ? "Picking…" : "Auto-pick by my issues"}
        </button>
      </span>
      {prompt === "sign_in" ? (
        <p role="status" className="w-full rounded-lg border border-line bg-surface px-3 py-2 text-sm text-ink-mid">
          Sign up for free to see which candidates align with the issues important to me.{" "}
          <Link to="/register" className="font-medium underline decoration-dotted underline-offset-2 hover:text-ink">
            Sign up
          </Link>{" "}
          or{" "}
          <Link to="/login" className="font-medium underline decoration-dotted underline-offset-2 hover:text-ink">
            sign in
          </Link>
        </p>
      ) : null}
      {prompt === "rank_issues" ? (
        <p role="status" className="w-full rounded-lg border border-line bg-surface px-3 py-2 text-sm text-ink-mid">
          Rank at least {MIN_AUTO_PICK_ISSUES} issues first, so the pick reflects what matters to you.{" "}
          <Link
            to="/me/settings"
            className="font-medium underline decoration-dotted underline-offset-2 hover:text-ink"
          >
            Rank your issues
          </Link>
        </p>
      ) : null}
      {autoPick.isError && !autoPick.isPending ? (
        <p role="alert" className="w-full text-sm font-medium text-red-800">
          {autoPick.error instanceof ApiError
            ? autoPick.error.message
            : "Couldn't run the pick — check your connection and try again."}
        </p>
      ) : null}
      {result !== null ? (
        <WhyThisPickPanel
          result={result}
          seatsToFill={seatsToFill}
          areaName={areaName}
          onDismiss={() => setResult(null)}
        />
      ) : null}
    </div>
  );
}

type WhyThisPickPanelProps = {
  result: AutoPickElectionResult;
  seatsToFill: number | null;
  areaName: (researchAreaId: string) => string;
  onDismiss: () => void;
};

function candidateName(result: AutoPickElectionResult, candidateId: string): string {
  return (
    result.candidates.find((report) => report.candidate_id === candidateId)?.display_name ?? "a candidate"
  );
}

// Headline sentence per outcome/reason — the honest summary the spec
// requires ("no pick" is a normal outcome, and saying why is the feature).
function summarize(result: AutoPickElectionResult, seatsToFill: number | null): string {
  // Race-type-independent "couldn't run" reasons come before the fork: a
  // measure result carries them too (with an empty per-issue list), and
  // letting the measure branch see one would mislabel a rank-your-issues
  // problem as a tagging gap.
  if (result.reason === "too_few_issues") {
    return `Rank at least ${MIN_AUTO_PICK_ISSUES} issues first, so the pick reflects what matters to you.`;
  }
  if (result.reason === "election_closed") {
    return "This election is no longer open for picks.";
  }
  const shortlist = joinNames(result.shortlist_candidate_ids.map((id) => candidateName(result, id)));
  if (result.race_type === "ballot_measure") {
    if (result.reason === "veto") {
      return "Vote No — this measure goes against an issue you drew a line on.";
    }
    if (result.measure_position === "yes") {
      return "Vote Yes — this measure supports your issues overall.";
    }
    if (result.measure_position === "no") {
      return "Vote No — this measure goes against your issues overall.";
    }
    // Two distinct "no answer" cases, and the user deserves to know which:
    // an empty per-issue list means the measure shares no tags with their
    // ranked issues; a non-empty list with no position means the weighted
    // sides cancelled out (the chips below show the split).
    if (result.measure_per_issue.length === 0) {
      return "No answer — this measure isn't tagged with any of your issues yet.";
    }
    return "No answer — this measure helps some of your issues and hurts others about equally, so it's your call.";
  }
  if (result.outcome === "picked") {
    const picked = joinNames(result.picked_candidate_ids.map((id) => candidateName(result, id)));
    // Multi-seat races can fill fewer seats than they have (a tie or a lack
    // of evidence for the rest): a "picked" summary that hides the open
    // seats would read as a finished race.
    const openSeats = (seatsToFill ?? 1) - result.picked_candidate_ids.length;
    if (result.reason === "by_elimination") {
      return `Picked ${picked} by elimination — the rest have records against your issues, and nothing known counts against ${picked}.`;
    }
    if (result.reason === "tie") {
      return `Picked ${picked}; the ${openSeats === 1 ? "last seat is" : `remaining ${openSeats} seats are`} a tie between ${shortlist} — that part is your call.`;
    }
    if (openSeats > 0) {
      return `Picked ${picked} — the best match for your issues. ${openSeats === 1 ? "One seat is" : `${openSeats} seats are`} still open: nothing known separates the other candidates, so those picks are yours to make.`;
    }
    return `Picked ${picked} — the best match for your issues.`;
  }
  switch (result.reason) {
    case "tie":
      return `It's a tie between ${shortlist} on your issues — your call between them.`;
    case "only_negative_evidence":
      return result.shortlist_candidate_ids.length > 0
        ? `Couldn't pick one — narrowed to ${shortlist}: nothing known against them, but nothing for them either.`
        : "No pick: every candidate with a record here works against your issues.";
    case "all_vetoed":
      return "No pick: every candidate goes against one of your musts.";
    default:
      return "No pick: none of these candidates has a record on your issues yet.";
  }
}

function PerIssueChips({
  perIssue,
  areaName,
}: {
  perIssue: { research_area_id: string; net: number }[];
  areaName: (id: string) => string;
}) {
  if (perIssue.length === 0) {
    return null;
  }
  return (
    <span>
      {perIssue.map((issue, index) => (
        <span key={issue.research_area_id}>
          <span
            className={
              issue.net > 0
                ? "font-medium text-green-900"
                : issue.net < 0
                  ? "font-medium text-red-900"
                  : "font-medium text-amber-900"
            }
          >
            {areaName(issue.research_area_id)}{" "}
            {issue.net > 0 ? "· aligned" : issue.net < 0 ? "· conflicts" : "· mixed"}
          </span>
          {index < perIssue.length - 1 ? ", " : null}
        </span>
      ))}
    </span>
  );
}

function WhyThisPickPanel({ result, seatsToFill, areaName, onDismiss }: WhyThisPickPanelProps) {
  const pickedReports = result.picked_candidate_ids
    .map((id) => result.candidates.find((report) => report.candidate_id === id))
    .filter((report): report is AutoPickCandidateReport => report !== undefined);
  const vetoedReports = result.candidates.filter((report) => report.vetoed_by.length > 0);

  return (
    <section
      aria-label="Why this pick"
      className="w-full rounded-xl border border-line bg-surface/50 p-4 text-sm text-ink"
    >
      <div className="flex items-start justify-between gap-2">
        <p className="font-medium">{summarize(result, seatsToFill)}</p>
        <button
          type="button"
          onClick={onDismiss}
          className="text-xs font-medium text-ink-soft underline decoration-dotted underline-offset-2 hover:text-ink"
        >
          Hide
        </button>
      </div>
      {result.race_type === "ballot_measure" && result.measure_per_issue.length > 0 ? (
        <p className="mt-2">
          <span className="font-medium text-ink-soft">On your issues:</span>{" "}
          <PerIssueChips perIssue={result.measure_per_issue} areaName={areaName} />
        </p>
      ) : null}
      {pickedReports.map((report) => (
        <p key={report.candidate_id} className="mt-2">
          <span className="font-medium">{report.display_name}</span>
          {report.per_issue.length > 0 ? (
            <>
              {" — "}
              <PerIssueChips perIssue={report.per_issue} areaName={areaName} />
            </>
          ) : (
            <span className="text-ink-soft"> — no records on your issues (picked by elimination)</span>
          )}
        </p>
      ))}
      {vetoedReports.map((report) => (
        <p key={report.candidate_id} className="mt-2 text-red-900">
          <span className="font-medium">{report.display_name}</span> excluded — crossed your line on{" "}
          {joinNames([...new Set(report.vetoed_by.map((veto) => areaName(veto.research_area_id)))])}:{" "}
          <span className="text-ink-soft">
            {report.vetoed_by[0]?.description}
            {report.vetoed_by.length > 1 ? ` (and ${report.vetoed_by.length - 1} more)` : ""}
          </span>
        </p>
      ))}
      {result.unresearched.length > 0 ? (
        // Transparency requirement: the comparison was partial, and the user
        // must see who was missing and why (never researched vs researched
        // with nothing found on their issues).
        <p className="mt-2 text-ink-soft">
          <span className="font-medium">Not compared:</span>{" "}
          {result.unresearched.map((entry, index) => (
            <span key={entry.candidate_id}>
              {entry.display_name} ({entry.never_researched ? "not researched yet" : "no records on your issues"})
              {index < result.unresearched.length - 1 ? ", " : null}
            </span>
          ))}
        </p>
      ) : null}
    </section>
  );
}
