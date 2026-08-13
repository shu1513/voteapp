import type { ElectionChoice, ElectionPreviewCandidate, ElectionSummary } from "@voteapp/api-client";
import { formatElectionDate } from "@voteapp/api-client";

// Ballot view of My Picks / My Ballot Draft: a paper-ballot-shaped render of
// the same races the date cards list — contest boxes in state-baseline ballot
// order (the payload must be fetched with include=preview &
// sort=state_baseline & followed_first=false), fill-in ovals, the user's
// picks pre-filled — so a voter can copy marks onto the real ballot instead
// of hunting for each race.
//
// Honesty rules (docs/plans/ballot-facsimile.md):
//   - never present this as an official ballot; the footer disclaimer is
//     load-bearing, not decoration
//   - measure summaries are VoteApp explanations, labeled as such, never
//     styled as ballot text
//   - withdrawn candidates render struck-through instead of vanishing (they
//     may still be printed on the paper ballot)
//   - a pick is marked by the filled oval AND the "Your pick" text — never
//     color alone

function BallotOval({ filled, label }: { filled: boolean; label?: string }) {
  return (
    <span
      aria-hidden="true"
      className={`mt-1 inline-block h-3.5 w-6 shrink-0 rounded-full border-2 border-ink ${filled ? "bg-ink" : "bg-white"}`}
      title={label}
    />
  );
}

function voteInstruction(seatsToFill: number | null): string {
  return seatsToFill !== null && seatsToFill > 1 ? `Vote for up to ${seatsToFill}` : "Vote for One";
}

function CandidateRow({ candidate, picked }: { candidate: ElectionPreviewCandidate; picked: boolean }) {
  const withdrawn = candidate.status === "withdrawn";
  return (
    <li className="flex items-start gap-2 border-t border-line px-3 py-1.5">
      <BallotOval filled={picked} />
      <span className="min-w-0 text-sm leading-snug">
        <span className={withdrawn ? "text-ink-soft line-through" : picked ? "font-bold text-ink" : "text-ink"}>
          {candidate.display_name}
          {candidate.running_mate ? ` and ${candidate.running_mate.display_name}` : null}
        </span>
        {candidate.party ? <span className="text-ink-soft"> · {candidate.party}</span> : null}
        {withdrawn ? <span className="text-xs text-ink-soft"> (withdrew — votes may not count)</span> : null}
        {picked ? (
          <span className="ml-1.5 rounded bg-ink px-1.5 py-0.5 text-xs font-semibold text-white">Your pick</span>
        ) : null}
      </span>
    </li>
  );
}

function ContestBox({ election, choice }: { election: ElectionSummary; choice: ElectionChoice | undefined }) {
  const preview = election.preview;
  const pickedIds = new Set((choice?.picks ?? []).map((pick) => pick.candidate_id));
  const isMeasure = election.race_type === "ballot_measure";
  return (
    <section className="break-inside-avoid border border-ink">
      <header className="border-b-2 border-ink bg-surface px-3 py-1.5">
        <h4 className="text-sm font-bold uppercase leading-snug tracking-wide text-ink">
          {election.official_ballot_title}
        </h4>
        {/* The seat gate: county/place rows carry every ward seat, so this
            race may not be on the reader's own ballot (subDistrictSeat.ts). */}
        {election.sub_district_seat ? (
          <p className="mt-0.5 text-xs text-ink-soft">
            Covers {election.sub_district_seat} — may not be on your ballot.
          </p>
        ) : null}
        <p className="mt-0.5 text-xs font-semibold text-ink">
          {isMeasure ? "Vote Yes or No" : voteInstruction(preview?.seats_to_fill ?? null)}
        </p>
      </header>
      {isMeasure ? (
        <>
          {preview?.measure?.summary ? (
            // VoteApp's explanation, visually set apart and labeled — never
            // styled as the printed ballot question, which we don't store.
            <p className="border-t border-line bg-surface/50 px-3 py-1.5 text-xs italic text-ink-soft">
              VoteApp summary (not the printed ballot text): {preview.measure.summary}
            </p>
          ) : null}
          <ul>
            {(["yes", "no"] as const).map((position) => {
              const picked = choice?.measure_position === position;
              return (
                <li key={position} className="flex items-start gap-2 border-t border-line px-3 py-1.5">
                  <BallotOval filled={picked} />
                  <span className="text-sm">
                    <span className={picked ? "font-bold text-ink" : "text-ink"}>
                      {position === "yes" ? "Yes" : "No"}
                    </span>
                    {picked ? (
                      <span className="ml-1.5 rounded bg-ink px-1.5 py-0.5 text-xs font-semibold text-white">
                        Your pick
                      </span>
                    ) : null}
                  </span>
                </li>
              );
            })}
          </ul>
        </>
      ) : (preview?.candidates.length ?? 0) > 0 ? (
        <ul>
          {preview?.candidates.map((candidate) => (
            <CandidateRow
              key={candidate.candidate_election_id}
              candidate={candidate}
              picked={pickedIds.has(candidate.candidate_id)}
            />
          ))}
        </ul>
      ) : (
        <p className="border-t border-line px-3 py-1.5 text-xs text-ink-soft">Candidate list not final.</p>
      )}
    </section>
  );
}

function BallotSheet({
  date,
  elections,
  choiceByElectionId,
}: {
  date: string;
  elections: ElectionSummary[];
  choiceByElectionId: Map<string, ElectionChoice> | undefined;
}) {
  return (
    <section className="rounded-sm border border-line bg-white p-4 shadow-sm">
      <header className="border-b-2 border-ink pb-2 text-center">
        <h3 className="text-base font-bold text-ink">Ballot preview — {formatElectionDate(date)}</h3>
        <p className="text-xs text-ink-soft">Not an official ballot</p>
      </header>
      <div className="mt-3 space-y-3">
        {elections.map((election) => (
          <ContestBox key={election.id} election={election} choice={choiceByElectionId?.get(election.id)} />
        ))}
      </div>
      <footer className="mt-3 border-t border-line pt-2 text-xs text-ink-soft">
        <p>
          Races appear in the approximate order of your state's general ballot rules. This preview may include
          nearby district races that aren't on your ballot, or miss local ones — and candidate order and
          instructions on your printed ballot may differ. Compare with your official sample ballot.
        </p>
        <p className="mt-1">Polling-place device rules vary — print this page or check your local rules.</p>
      </footer>
    </section>
  );
}

// Groups the preview payload's elections by date (payload order preserved
// within each date — that IS the ballot order) and renders one sheet per
// upcoming date. Past races are excluded on purpose: the preview is a
// take-it-voting tool, not a results view.
export function BallotPreviewSheets({
  elections,
  choiceByElectionId,
  today,
}: {
  elections: ElectionSummary[];
  choiceByElectionId: Map<string, ElectionChoice> | undefined;
  today: string;
}) {
  const byDate = new Map<string, ElectionSummary[]>();
  for (const election of elections) {
    if (election.election_date < today) {
      continue;
    }
    const group = byDate.get(election.election_date) ?? [];
    group.push(election);
    byDate.set(election.election_date, group);
  }
  const dates = [...byDate.keys()].sort();
  if (dates.length === 0) {
    return <p className="mt-3 text-sm text-ink-soft">No upcoming elections to preview.</p>;
  }
  return (
    <div className="mt-4 space-y-4">
      {dates.map((date) => (
        <BallotSheet key={date} date={date} elections={byDate.get(date) ?? []} choiceByElectionId={choiceByElectionId} />
      ))}
      <p className="print:hidden">
        <button
          type="button"
          onClick={() => window.print()}
          className="rounded-lg border border-line bg-white px-3 py-1.5 text-sm font-medium text-ink transition hover:border-ink"
        >
          Print this preview
        </button>
      </p>
    </div>
  );
}

// The List / Ballot view switch shared by /me/picks and /draft. Plain
// buttons, not a router concern: the view is page-local state.
export function BallotViewToggle({
  view,
  onChange,
}: {
  view: "list" | "ballot";
  onChange: (view: "list" | "ballot") => void;
}) {
  const buttonClass = (active: boolean) =>
    `rounded-md px-2.5 py-1 text-sm font-medium transition ${
      active ? "bg-white text-ink shadow-sm" : "text-ink-soft hover:text-ink"
    }`;
  return (
    <div className="inline-flex items-center gap-0.5 rounded-lg bg-surface p-0.5 print:hidden" role="group" aria-label="Picks view">
      <button type="button" aria-pressed={view === "list"} className={buttonClass(view === "list")} onClick={() => onChange("list")}>
        List
      </button>
      <button type="button" aria-pressed={view === "ballot"} className={buttonClass(view === "ballot")} onClick={() => onChange("ballot")}>
        Ballot view
      </button>
    </div>
  );
}
