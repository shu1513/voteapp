import { useState } from "react";
import { Link } from "react-router";
import { useMutation, useQuery } from "@tanstack/react-query";
import { apiRequest, formatElectionDate, useElectionChoices, useMe } from "@voteapp/api-client";
import type { BallotSummary, ElectionChoice, ElectionSummary, PickCardShare } from "@voteapp/api-client";
import { BallotPreviewSheets, BallotViewToggle } from "../components/BallotPreview";
import { ErrorNotice, LoadingNotice } from "../components/Status";
import type { ElectionNavState } from "../lib/detailNavContext";
import { ShareButton } from "../components/ShareButton";
import { VerifyPrompt } from "../components/VerifyPrompt";
import { SITE_ORIGIN } from "../lib/pageMeta";
import { useDocumentTitle } from "../lib/useDocumentTitle";
import { usLatestLocalDate } from "../lib/usLatestLocalDate";

// My Picks (the header calls it "My Draft" until the nearest election day is
// fully decided): the pick cards — fight-card view of each upcoming election
// day: what's on the ballot, who I picked, what's still undecided. Followed
// candidates live on /me/follows and the ranked issue editor on Settings,
// each restored to its own home after a stint on this page.

// Election links on this page hand the election page its back destination.
// No contest list: these cards are pick summaries, not a ballot sequence.
const PICKS_NAV_STATE: ElectionNavState = {
  backTo: { path: "/me/picks", label: "My Picks" },
};

// One race row on a date card. "Pick chips" per candidate so a multi-seat
// race reads name-by-name, with the outcome carried per candidacy. The
// certified writer projects advanced/runoff onto winners (everyone else
// becomes lost), so all five terminal statuses need a chip — a certified
// "advanced" with no chip would read as no outcome at all once the race
// leaves the carded window and the result fallback with it. Labels match
// the election page's badges (Won / Advanced / In runoff / Lost). Each chip
// leads with a real space: margin is only visual, and without the space the
// copy/accessible text runs the name into the label ("Jane SmithWon").
function pickStatusChip(status: string) {
  if (status === "won" || status === "advanced" || status === "runoff") {
    return (
      <>
        {" "}
        <span className="rounded bg-green-700 px-1.5 py-0.5 text-xs font-semibold text-white">
          {status === "won" ? "Won" : status === "advanced" ? "Advanced" : "In runoff"}
        </span>
      </>
    );
  }
  if (status === "lost") {
    return (
      <>
        {" "}
        <span className="rounded bg-surface px-1.5 py-0.5 text-xs font-medium text-ink-soft">Lost</span>
      </>
    );
  }
  if (status === "withdrawn") {
    return (
      <>
        {" "}
        <span className="text-xs text-ink-soft">(withdrew)</span>
      </>
    );
  }
  return null;
}

// Outcome chip for a measure pick — the measure counterpart of the won/lost
// candidacy chips. The word states the FACT ("Passed"/"Failed"); the color
// says how it landed for the owner, mirroring Won-green/Lost-muted: green
// when the outcome matches their vote, muted when it went the other way.
// Anything but the writer's two canonical values (including a pre-field
// backend during deploy skew) renders nothing. Mirrored on the public card
// page.
function measureOutcomeChip(position: "yes" | "no", result: string | null | undefined) {
  if (result !== "passed" && result !== "failed") {
    return null;
  }
  const matchedPick = (result === "passed") === (position === "yes");
  const label = result === "passed" ? "Passed" : "Failed";
  return (
    <>
      {" "}
      {matchedPick ? (
        <span className="rounded bg-green-700 px-1.5 py-0.5 text-xs font-semibold text-white">{label}</span>
      ) : (
        <span className="rounded bg-surface px-1.5 py-0.5 text-xs font-medium text-ink-soft">{label}</span>
      )}
    </>
  );
}

// Result-derived chip for a pick the candidacy pipeline hasn't labeled yet:
// election-night calls arrive as result rows (outcome + winner ids) long
// before candidate_elections.status flips to won/lost. Id-only matching and
// decisive outcomes only, the same conservatism as the ballot card's
// "My pick won ✓" marker — and the same silence when the pick isn't among
// the winners: the card announces the payoff, it doesn't rub in the loss.
function pickResultChip(
  outcome: string | null | undefined,
  winners: readonly { candidate_id?: string }[] | undefined,
  candidateId: string
) {
  if (outcome !== "won" && outcome !== "advanced" && outcome !== "runoff") {
    return null;
  }
  if (!(winners ?? []).some((winner) => winner.candidate_id === candidateId)) {
    return null;
  }
  // Same vocabulary as the election page's badges and pickStatusChip: a
  // runoff berth is its own state, not a generic "Advanced".
  return (
    <>
      {" "}
      <span className="rounded bg-green-700 px-1.5 py-0.5 text-xs font-semibold text-white">
        {outcome === "won" ? "Won" : outcome === "advanced" ? "Advanced" : "In runoff"}
      </span>
    </>
  );
}

function PickedLine({ choice, election }: { choice: ElectionChoice; election?: ElectionSummary }) {
  // The canonical result reaches this line two ways: via the ballot summary
  // while the race is still carded, and via the choice itself afterwards
  // (attached on the choices list read) — so PastPicks keeps showing an
  // election-night call during the weeks before certification.
  const resultOutcome = election?.current_result_outcome ?? choice.current_result_outcome;
  const resultWinners = election?.current_result_winners ?? choice.current_result_winners;
  if (choice.measure_position !== null) {
    return (
      <span className={choice.measure_position === "yes" ? "font-semibold text-green-900" : "font-semibold text-red-900"}>
        {choice.measure_position === "yes" ? "Yes" : "No"}
        {/* Certified measure result first; before it lands, the canonical
            result row's election-night passed/failed fills in. Anything
            else (too_close, unknown) renders nothing, as the chip demands. */}
        {measureOutcomeChip(choice.measure_position, choice.measure_result ?? resultOutcome)}
      </span>
    );
  }
  return (
    <span className="font-semibold text-green-900">
      {choice.picks.map((pick, index) => (
        <span key={pick.candidate_id}>
          {index > 0 ? ", " : null}
          {pick.display_name}
          {/* candidacy_status (certified won/lost, withdrawn) outranks the
              result-derived chip — never both. */}
          {pickStatusChip(pick.candidacy_status) ??
            pickResultChip(resultOutcome, resultWinners, pick.candidate_id)}
        </span>
      ))}
    </span>
  );
}

// The label-worthiness rule ElectionCard uses: a choice whose only pick lost
// its candidate (deleted/merged) renders nothing and counts as undecided.
function hasRenderablePick(choice: ElectionChoice | undefined): choice is ElectionChoice {
  return choice !== undefined && (choice.picks.length > 0 || choice.measure_position !== null);
}

function ShareCardControl({ electionDate }: { electionDate: string }) {
  const mint = useMutation({
    mutationFn: () =>
      apiRequest<{ share: PickCardShare }>("/api/me/pick-card-shares", {
        method: "POST",
        body: { election_date: electionDate },
      }),
  });

  if (mint.isSuccess) {
    const path = `/picks/${mint.data.share.token}`;
    return (
      <span className="flex flex-wrap items-center gap-2">
        {/* The minted URL is the deliverable — it renders here, visibly,
            the moment it exists. A bare "Share" button next to "anyone with
            the link…" reads as broken when no link is anywhere in sight.
            The anchor opens the public card so the sharer can see exactly
            what recipients will.

            href is deliberately the RELATIVE path while the text shows the
            canonical host: the token only exists in the environment that
            minted it, so an absolute SITE_ORIGIN href would 404 from dev or
            staging, while in production the two are the same page anyway.
            The protocol is trimmed from the display only. */}
        <a
          href={path}
          target="_blank"
          rel="noopener noreferrer"
          className="break-all text-xs text-ink underline hover:text-rausch"
        >
          {`${SITE_ORIGIN.replace(/^https?:\/\//, "")}${path}`}
        </a>
        {/* pageMeta on the public page supplies the og card; ShareButton
            handles native share / copy / X / Facebook / WhatsApp / email. */}
        <ShareButton
          path={path}
          shareText={`My ${formatElectionDate(electionDate)} election picks`}
          affirmative
        />
        {/* Names the name: the public page shows the owner's first name, and
            the sharer must learn that HERE, before posting the link — not
            from a recipient. */}
        <span className="text-xs text-ink-soft">
          Anyone with the link can see this card and your first name.
        </span>
      </span>
    );
  }
  return (
    <span className="flex flex-wrap items-center gap-2">
      <button
        type="button"
        disabled={mint.isPending}
        onClick={() => mint.mutate()}
        className="rounded-lg border border-line bg-white px-3 py-1.5 text-sm font-medium text-ink transition hover:border-ink disabled:opacity-50"
      >
        {mint.isPending ? "…" : "Share my picks"}
      </button>
      {mint.isError ? (
        <span role="alert" className="text-xs font-medium text-red-800">
          Couldn't create the share link — try again.
        </span>
      ) : null}
    </span>
  );
}

// Exported for the guest /draft page, which renders the same card from the
// localStorage ballot draft: share off (the share API is account-only and
// the card would leak a mintable URL promise it can't keep), and its own
// back-link state so races return to /draft, not /me/picks.
export function PickDateCard({
  date,
  elections,
  choiceByElectionId,
  share = true,
  navState = PICKS_NAV_STATE,
}: {
  date: string;
  elections: ElectionSummary[];
  choiceByElectionId: Map<string, ElectionChoice> | undefined;
  share?: boolean;
  navState?: ElectionNavState;
}) {
  const pickedCount = elections.filter((election) =>
    hasRenderablePick(choiceByElectionId?.get(election.id))
  ).length;
  // Cards outlive their election day (the ballot keeps finished races for a
  // few days so results can land on them); once the date passes, "no pick
  // yet" would invite an action that's no longer possible.
  const isPast = date < usLatestLocalDate();
  return (
    <section className="rounded-xl border border-line bg-white p-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h3 className="text-lg font-semibold text-ink">My {formatElectionDate(date)} Election Picks</h3>
        {/* Mint-on-demand: no share row (and no live public URL) exists until
            the user asks for one. Hidden entirely while the card has zero
            picks — the backend refuses to mint for an empty card anyway. */}
        {share && pickedCount > 0 ? <ShareCardControl electionDate={date} /> : null}
      </div>
      <p className="mt-0.5 text-xs text-ink-soft">
        {pickedCount} of {elections.length} race{elections.length === 1 ? "" : "s"} decided
      </p>
      <ul className="mt-3 space-y-2">
        {elections.map((election) => {
          const choice = choiceByElectionId?.get(election.id);
          return (
            <li key={election.id} className="text-sm">
              {hasRenderablePick(choice) ? (
                <>
                  <Link to={`/elections/${election.id}`} state={navState} className="text-ink hover:text-rausch">
                    {election.official_ballot_title}
                  </Link>
                  <span className="text-ink-soft"> — </span>
                  <PickedLine choice={choice} election={election} />
                </>
              ) : (
                // Undecided: the whole line is the quiet call to action —
                // grey, clickable, straight to the race.
                <Link
                  to={`/elections/${election.id}`}
                  state={navState}
                  className="text-ink-soft underline decoration-dotted underline-offset-2 hover:text-ink"
                >
                  {election.official_ballot_title} — {isPast ? "no pick" : "no pick yet"}
                </Link>
              )}
            </li>
          );
        })}
      </ul>
    </section>
  );
}

// Past picks come from the choices payload alone (it carries title + date),
// not the ballot: the saved ballot only keeps recently finished elections,
// while picks history should survive indefinitely. Races still carded above
// (the ballot's just-finished window) are excluded — their picks are already
// on display, with results; they fall in here when the ballot drops them.
function PastPicks({
  choices,
  today,
  cardedElectionIds,
}: {
  choices: ElectionChoice[];
  today: string;
  cardedElectionIds: Set<string>;
}) {
  const past = choices
    .filter((choice) => choice.election_date < today && !cardedElectionIds.has(choice.election_id))
    .filter((choice) => choice.picks.length > 0 || choice.measure_position !== null)
    .sort((a, b) => (a.election_date < b.election_date ? 1 : a.election_date > b.election_date ? -1 : 0));
  if (past.length === 0) {
    return null;
  }
  return (
    <details className="mt-4">
      <summary className="cursor-pointer select-none text-sm font-medium text-ink-soft hover:text-ink">
        Past elections ({past.length})
      </summary>
      <ul className="mt-3 space-y-2">
        {past.map((choice) => (
          <li key={choice.election_id} className="text-sm">
            <span className="text-ink-soft">{formatElectionDate(choice.election_date)} · </span>
            <Link to={`/elections/${choice.election_id}`} state={PICKS_NAV_STATE} className="text-ink hover:text-rausch">
              {choice.official_ballot_title}
            </Link>
            <span className="text-ink-soft"> — </span>
            <PickedLine choice={choice} />
          </li>
        ))}
      </ul>
    </details>
  );
}

export function PicksPage() {
  useDocumentTitle("My Picks");
  const { me, isLoading } = useMe();
  const verified = me?.email_verified === true;
  const [view, setView] = useState<"list" | "ballot">("list");
  const {
    choices,
    choiceByElectionId,
    isLoading: choicesLoading,
    isError: choicesError,
  } = useElectionChoices();
  // Same source as the saved ballot page: the user's districts decide which
  // races belong on their cards.
  const ballot = useQuery({
    queryKey: ["me", "ballot"],
    queryFn: () => apiRequest<BallotSummary>("/api/me/ballot"),
    enabled: verified,
    retry: false,
  });
  // Ballot view payload, fetched lazily on first toggle: rosters + measure
  // text (include=preview) in paper-ballot contest order. Explicit sort and
  // followed_first — the user's saved list preferences must never reorder a
  // ballot sheet.
  const ballotPreview = useQuery({
    queryKey: ["me", "ballot", "preview"],
    queryFn: () =>
      apiRequest<BallotSummary>("/api/me/ballot?include=preview&sort=state_baseline&followed_first=false"),
    enabled: verified && view === "ballot",
    retry: false,
  });

  if (isLoading || me === undefined) {
    return <LoadingNotice text="Loading…" />;
  }
  if (me === null) {
    return (
      <div className="mx-auto max-w-md px-4 py-10 text-center">
        <p className="text-ink-soft">Log in to plan your votes and manage the candidates you follow.</p>
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
  if (!me.email_verified) {
    return <VerifyPrompt email={me.email} />;
  }

  const today = usLatestLocalDate();
  // Strict date grouping regardless of the ballot's saved sort: cards are
  // "everything you face on this day", so an issue- or impact-based order
  // must not interleave dates here. No date filter of our own: the ballot
  // payload already keeps just-finished elections for a few days
  // (BALLOT_PAST_ELECTION_VISIBILITY_DAYS), and the card should live exactly
  // as long — results land right on it before it retires to Past elections.
  const byDate = new Map<string, ElectionSummary[]>();
  for (const election of ballot.data?.elections ?? []) {
    const group = byDate.get(election.election_date) ?? [];
    group.push(election);
    byDate.set(election.election_date, group);
  }
  const dates = [...byDate.keys()].sort();
  const cardedElectionIds = new Set((ballot.data?.elections ?? []).map((election) => election.id));

  // Cards are meaningless without the choices: rendering them from an
  // unloaded map claims "no pick yet" on races the user already decided
  // (same no-flash rule ElectionCard documents for its chip). One reveal:
  // nothing below the heading until BOTH queries settle, and no cards at
  // all when the choices fetch failed — a visible error beats confidently
  // wrong "0 of N decided" cards.
  const choicesReady = choiceByElectionId !== undefined;
  const picksSettled = ballot.isSuccess && choicesReady;

  return (
    <div className="mx-auto max-w-3xl px-4 py-8">
      <section>
        <h1 className="text-2xl font-bold">My Election Picks</h1>
        {ballot.isPending || (choicesLoading && !choicesError) ? (
          <LoadingNotice text="Loading your elections…" />
        ) : null}
        {ballot.isError ? (
          <div className="mt-4">
            <ErrorNotice error={ballot.error} />
          </div>
        ) : null}
        {choicesError ? (
          // Not ErrorNotice: it renders generic copy for non-ApiError values,
          // and this page has two failure slots (ballot, picks) that must
          // stay tellable apart. Same visual shell, specific words.
          <p className="mt-4 rounded-lg border border-rausch/40 bg-rausch/5 px-3 py-2 text-sm text-rausch-dark">
            Could not load your picks — refresh to try again.
          </p>
        ) : null}
        {picksSettled && dates.length === 0 ? (
          <p className="mt-3 text-sm text-ink-soft">
            No upcoming elections on your ballot yet.{" "}
            <Link to="/me/ballot" className="underline hover:text-ink">
              Set your address
            </Link>{" "}
            to see your races.
          </p>
        ) : null}
        {picksSettled ? (
          <>
            {dates.length > 0 ? (
              <div className="mt-4">
                <BallotViewToggle view={view} onChange={setView} />
              </div>
            ) : null}
            {view === "ballot" ? (
              <>
                {ballotPreview.isPending ? <LoadingNotice text="Loading your ballot preview…" /> : null}
                {ballotPreview.isError ? (
                  <div className="mt-4">
                    <ErrorNotice error={ballotPreview.error} />
                  </div>
                ) : null}
                {ballotPreview.isSuccess ? (
                  <BallotPreviewSheets
                    elections={ballotPreview.data.elections}
                    choiceByElectionId={choiceByElectionId}
                    today={today}
                  />
                ) : null}
              </>
            ) : (
              <div className="mt-4 space-y-4">
                {dates.map((date) => (
                  <PickDateCard
                    key={date}
                    date={date}
                    elections={byDate.get(date) ?? []}
                    choiceByElectionId={choiceByElectionId}
                  />
                ))}
              </div>
            )}
            <PastPicks choices={choices ?? []} today={today} cardedElectionIds={cardedElectionIds} />
          </>
        ) : null}
      </section>
    </div>
  );
}

export default PicksPage;
