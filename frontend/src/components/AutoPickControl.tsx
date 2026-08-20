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

// "Pick for me": one button that runs the auto-pick engine for this election
// (POST /api/me/auto-picks, mode replace) and opens a "Why this pick" panel
// built from the response — winner, per-issue alignment, vetoed and
// unresearched candidates, and the honest "no pick" reason when nothing
// qualified. Spec: docs/plans/auto-pick-by-issues.md.
//
// Mirrors the server's UX floor (MIN_AUTO_PICK_ISSUES): below 3 ranked
// issues the button explains what to do instead of calling the API. Guests
// get a sign-in prompt — issue preferences are account-only.
const MIN_ISSUES = 3;

type AutoPickControlProps = {
  electionId: string;
};

function joinNames(names: string[]): string {
  if (names.length <= 1) {
    return names[0] ?? "";
  }
  return `${names.slice(0, -1).join(", ")} and ${names[names.length - 1]}`;
}

export function AutoPickControl({ electionId }: AutoPickControlProps) {
  const { me } = useMe();
  const { preferences } = useMyResearchAreas();
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
    if (preferences.length < MIN_ISSUES) {
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
    <div className="flex flex-col gap-2">
      <span>
        <button
          type="button"
          disabled={saving}
          onClick={onClick}
          className="rounded-lg border border-line bg-white px-3 py-1.5 text-sm font-semibold text-ink transition hover:border-ink disabled:opacity-50"
        >
          {autoPick.isPending ? "Picking…" : "Pick for me"}
        </button>
      </span>
      {prompt === "sign_in" ? (
        <p className="text-sm text-ink-soft">
          Pick for me matches candidates to the issues you rank, which needs an account.{" "}
          <Link to="/login" className="font-medium underline decoration-dotted underline-offset-2 hover:text-ink">
            Sign in
          </Link>
        </p>
      ) : null}
      {prompt === "rank_issues" ? (
        <p className="text-sm text-ink-soft">
          Rank at least {MIN_ISSUES} issues first, so the pick reflects what matters to you.{" "}
          <Link
            to="/me/settings"
            className="font-medium underline decoration-dotted underline-offset-2 hover:text-ink"
          >
            Rank your issues
          </Link>
        </p>
      ) : null}
      {autoPick.isError && !autoPick.isPending ? (
        <p role="alert" className="text-sm font-medium text-red-800">
          {autoPick.error instanceof ApiError
            ? autoPick.error.message
            : "Couldn't run the pick — check your connection and try again."}
        </p>
      ) : null}
      {result !== null ? <WhyThisPickPanel result={result} areaName={areaName} onDismiss={() => setResult(null)} /> : null}
    </div>
  );
}

type WhyThisPickPanelProps = {
  result: AutoPickElectionResult;
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
function summarize(result: AutoPickElectionResult): string {
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
    return "No answer: this measure isn't tagged with your issues, or the sides balance out.";
  }
  if (result.outcome === "picked") {
    const picked = joinNames(result.picked_candidate_ids.map((id) => candidateName(result, id)));
    if (result.reason === "by_elimination") {
      return `Picked ${picked} by elimination — the rest have records against your issues, and nothing known counts against ${picked}.`;
    }
    if (result.reason === "tie") {
      return `Picked ${picked}; the last seat is a tie between ${shortlist} — that one is your call.`;
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
      return "No pick: every candidate crossed one of your lines in the sand.";
    case "too_few_issues":
      return `Rank at least ${MIN_ISSUES} issues first, so the pick reflects what matters to you.`;
    case "election_closed":
      return "This election is no longer open for picks.";
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

function WhyThisPickPanel({ result, areaName, onDismiss }: WhyThisPickPanelProps) {
  const pickedReports = result.picked_candidate_ids
    .map((id) => result.candidates.find((report) => report.candidate_id === id))
    .filter((report): report is AutoPickCandidateReport => report !== undefined);
  const vetoedReports = result.candidates.filter((report) => report.vetoed_by.length > 0);

  return (
    <section
      aria-label="Why this pick"
      className="rounded-xl border border-line bg-surface/50 p-4 text-sm text-ink"
    >
      <div className="flex items-start justify-between gap-2">
        <p className="font-medium">{summarize(result)}</p>
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
