import { Link } from "react-router";
import { useMutation, useQuery } from "@tanstack/react-query";
import { apiRequest, formatElectionDate, useElectionChoices, useMe } from "@voteapp/api-client";
import type { BallotSummary, ElectionChoice, ElectionSummary, PickCardShare } from "@voteapp/api-client";
import { ErrorNotice, LoadingNotice } from "../components/Status";
import { FollowedCandidatesSection } from "../components/FollowedCandidatesSection";
import { ResearchAreasSection } from "../components/ResearchAreasSection";
import { ShareButton } from "../components/ShareButton";
import { VerifyPrompt } from "../components/VerifyPrompt";
import { useDocumentTitle } from "../lib/useDocumentTitle";
import { usLatestLocalDate } from "../lib/usLatestLocalDate";

// My Picks: the voter's planning hub. Three sections — the pick cards
// (fight-card view of each upcoming election day: what's on the ballot, who
// I picked, what's still undecided), the ranked issue editor (moved here
// from Settings), and the followed-candidates manager (moved from the
// retired /me/follows page).

// One race row on a date card. "Pick chips" per candidate so a multi-seat
// race reads name-by-name, with won/lost/withdrawn carried per candidacy.
function pickStatusChip(status: string) {
  if (status === "won") {
    return <span className="ml-1 rounded bg-green-700 px-1.5 py-0.5 text-xs font-semibold text-white">Won</span>;
  }
  if (status === "lost") {
    return <span className="ml-1 rounded bg-surface px-1.5 py-0.5 text-xs font-medium text-ink-soft">Lost</span>;
  }
  if (status === "withdrawn") {
    return <span className="ml-1 text-xs text-ink-soft">(withdrew)</span>;
  }
  return null;
}

function PickedLine({ choice }: { choice: ElectionChoice }) {
  if (choice.measure_position !== null) {
    return (
      <span className={choice.measure_position === "yes" ? "font-semibold text-green-900" : "font-semibold text-red-900"}>
        {choice.measure_position === "yes" ? "Yes" : "No"}
      </span>
    );
  }
  return (
    <span className="font-semibold text-green-900">
      {choice.picks.map((pick, index) => (
        <span key={pick.candidate_id}>
          {index > 0 ? ", " : null}
          {pick.display_name}
          {pickStatusChip(pick.candidacy_status)}
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
    return (
      <span className="flex flex-wrap items-center gap-2">
        {/* pageMeta on the public page supplies the og card; ShareButton
            handles native share / copy / X / Facebook / WhatsApp / email. */}
        <ShareButton
          path={`/picks/${mint.data.share.token}`}
          shareText={`My ${formatElectionDate(electionDate)} election picks`}
        />
        <span className="text-xs text-ink-soft">Anyone with the link can see this card.</span>
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
        {mint.isPending ? "…" : "Share this card"}
      </button>
      {mint.isError ? (
        <span role="alert" className="text-xs font-medium text-red-800">
          Couldn't create the share link — try again.
        </span>
      ) : null}
    </span>
  );
}

function PickDateCard({
  date,
  elections,
  choiceByElectionId,
}: {
  date: string;
  elections: ElectionSummary[];
  choiceByElectionId: Map<string, ElectionChoice> | undefined;
}) {
  const pickedCount = elections.filter((election) =>
    hasRenderablePick(choiceByElectionId?.get(election.id))
  ).length;
  return (
    <section className="rounded-xl border border-line bg-white p-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h3 className="text-lg font-semibold text-ink">My {formatElectionDate(date)} Picks</h3>
        {/* Mint-on-demand: no share row (and no live public URL) exists until
            the user asks for one. Hidden entirely while the card has zero
            picks — the backend refuses to mint for an empty card anyway. */}
        {pickedCount > 0 ? <ShareCardControl electionDate={date} /> : null}
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
                  <Link to={`/elections/${election.id}`} className="text-ink hover:text-rausch">
                    {election.official_ballot_title}
                  </Link>
                  <span className="text-ink-soft"> — </span>
                  <PickedLine choice={choice} />
                </>
              ) : (
                // Undecided: the whole line is the quiet call to action —
                // grey, clickable, straight to the race.
                <Link
                  to={`/elections/${election.id}`}
                  className="text-ink-soft underline decoration-dotted underline-offset-2 hover:text-ink"
                >
                  {election.official_ballot_title} — no pick yet
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
// while picks history should survive indefinitely.
function PastPicks({ choices, today }: { choices: ElectionChoice[]; today: string }) {
  const past = choices
    .filter((choice) => choice.election_date < today)
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
            <Link to={`/elections/${choice.election_id}`} className="text-ink hover:text-rausch">
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
  const { choices, choiceByElectionId } = useElectionChoices();
  // Same source as the saved ballot page: the user's districts decide which
  // races belong on their cards.
  const ballot = useQuery({
    queryKey: ["me", "ballot"],
    queryFn: () => apiRequest<BallotSummary>("/api/me/ballot"),
    enabled: verified,
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
  // must not interleave dates here.
  const byDate = new Map<string, ElectionSummary[]>();
  for (const election of ballot.data?.elections ?? []) {
    if (election.election_date < today) {
      continue;
    }
    const group = byDate.get(election.election_date) ?? [];
    group.push(election);
    byDate.set(election.election_date, group);
  }
  const dates = [...byDate.keys()].sort();

  return (
    <div className="mx-auto max-w-3xl space-y-10 px-4 py-8">
      <section>
        <h1 className="text-2xl font-bold">My Picks</h1>
        {ballot.isPending ? <LoadingNotice text="Loading your elections…" /> : null}
        {ballot.isError ? (
          <div className="mt-4">
            <ErrorNotice error={ballot.error} />
          </div>
        ) : null}
        {ballot.isSuccess && dates.length === 0 ? (
          <p className="mt-3 text-sm text-ink-soft">
            No upcoming elections on your ballot yet.{" "}
            <Link to="/me/ballot" className="underline hover:text-ink">
              Set your address
            </Link>{" "}
            to see your races.
          </p>
        ) : null}
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
        <PastPicks choices={choices ?? []} today={today} />
      </section>

      <ResearchAreasSection />

      <FollowedCandidatesSection />
    </div>
  );
}

export default PicksPage;
