import { useState } from "react";
import type { ElectionCandidate } from "@voteapp/api-client";
import { partyColorClass } from "@voteapp/api-client";
import { AutoPickControl } from "./AutoPickControl";
import { FinanceSummaryCard, hasFinanceContent } from "./FinanceSummaryCard";
import { RecordItem } from "./RecordItem";
import { sourceLinkProps, track } from "../lib/usage";

// Same cut-off as the candidate page's newest-first view: a long-serving
// judge can carry dozens of records, and the Yes/No card must stay reachable.
const INITIAL_RECORDS = 20;

type RetentionJudgeSectionProps = {
  electionId: string;
  candidate: ElectionCandidate;
  /** Same gate as the roster's pick controls (district match, upcoming,
   * choices loaded): the auto-pick control renders only then. */
  showAutoPick: boolean;
  reporterEmail?: string | null;
  /** section_exposed marker for the "candidates" section. */
  headingRef?: (node: Element | null) => void;
};

// A judicial retention race is one Yes/No question about one person, so the
// election page shows the judge inline — name, site, summary, finance, and
// the full track record — instead of a one-card "Candidates" list that only
// linked to the profile. The Yes/No answer itself stays on the page's
// sticky card (ElectionPage), so this section carries no pick button.
export function RetentionJudgeSection({
  electionId,
  candidate,
  showAutoPick,
  reporterEmail,
  headingRef,
}: RetentionJudgeSectionProps) {
  const [showAll, setShowAll] = useState(false);
  const website = candidate.official_website_url ?? null;
  const records = candidate.records;
  const shownRecords = showAll ? records : records.slice(0, INITIAL_RECORDS);
  return (
    <section className="mt-6">
      <h2 ref={headingRef} className="text-heading font-semibold">
        {candidate.display_name}
      </h2>
      <p className="text-sm text-ink-soft">
        <span className={partyColorClass(candidate.party) || undefined}>{candidate.party}</span>
        {candidate.is_incumbent ? " · Incumbent" : ""}
        {candidate.status !== "active" ? ` · ${candidate.status}` : ""}
        {website ? (
          <>
            {" · "}
            <a
              href={website}
              target="_blank"
              rel="noopener noreferrer"
              onClick={() => track("official_source_click", { kind: "profile_link", ...sourceLinkProps(website) })}
              className="text-ink underline hover:text-rausch"
            >
              Official site
            </a>
          </>
        ) : null}
      </p>
      {candidate.summary ? <p className="mt-2 text-body text-ink">{candidate.summary}</p> : null}
      {showAutoPick ? (
        // "Does this candidate align with my values?": the engine answers
        // the Yes/No from the judge's records (see decideRetentionRace).
        <div className="mt-3">
          <AutoPickControl key={electionId} electionId={electionId} seatsToFill={null} retention />
        </div>
      ) : null}
      {hasFinanceContent(candidate.finance_summary) ? (
        // Collapsed like the candidate page's disclosure: reference material
        // that would otherwise push the record below the fold.
        <details
          className="mt-4"
          onToggle={(event) =>
            track("detail_control", { control: "finance_toggle", value: event.currentTarget.open ? "open" : "close" })
          }
        >
          <summary className="cursor-pointer select-none">
            <span className="text-lg font-semibold text-green-600" aria-hidden="true">
              ${" "}
            </span>
            <span className="text-lg font-semibold">Campaign Finance Information</span>
          </summary>
          <div className="mt-2 rounded-xl border border-line bg-surface p-4">
            <FinanceSummaryCard summary={candidate.finance_summary} />
          </div>
        </details>
      ) : null}
      <h3 className="mt-4 text-subheading font-semibold">Track record</h3>
      {records.length > 0 ? (
        <>
          <ul className="mt-2 space-y-3">
            {shownRecords.map((record) => (
              <RecordItem key={record.id} record={record} showTags reporterEmail={reporterEmail} />
            ))}
          </ul>
          {!showAll && records.length > INITIAL_RECORDS ? (
            <button
              type="button"
              onClick={() => {
                track("detail_control", { control: "records_show_all", value: "none" });
                setShowAll(true);
              }}
              className="mt-3 rounded-lg border border-line bg-white px-3 py-1.5 text-sm font-medium text-ink transition hover:border-ink"
            >
              Show all {records.length} records
            </button>
          ) : null}
        </>
      ) : (
        <p className="mt-2 text-sm text-ink-soft">No public records on file yet.</p>
      )}
    </section>
  );
}
