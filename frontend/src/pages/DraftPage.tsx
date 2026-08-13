import { Link, Navigate } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { apiRequest, formatElectionDate, useMe } from "@voteapp/api-client";
import type { BallotSummary, ElectionSummary } from "@voteapp/api-client";
import { EmptyNotice, ErrorNotice, LoadingNotice } from "../components/Status";
import type { ElectionNavState } from "../lib/detailNavContext";
import { draftChoicesByElectionId, draftPickCount, useBallotDraft } from "../lib/ballotDraft";
import { PickDateCard } from "./PicksPage";
import { useDocumentTitle } from "../lib/useDocumentTitle";
import { usLatestLocalDate } from "../lib/usLatestLocalDate";

// The guest's My Picks: the header's "My Ballot Draft" link lands here, and
// the page renders the SAME date cards the signed-in /me/picks page shows —
// races from the guest's last-viewed ballot, picks from the localStorage
// draft — minus sharing (the share API is account-only). The one addition is
// the register CTA: the draft on display is itself the pitch for saving it.

const DRAFT_NAV_STATE: ElectionNavState = {
  backTo: { path: "/draft", label: "My Ballot Draft" },
};

export function DraftPage() {
  useDocumentTitle("My Ballot Draft");
  const { me } = useMe();
  const draft = useBallotDraft();
  const districtIds = draft.district_ids;
  // Same key as BallotPage's default-sort query so a guest arriving from
  // /ballot reuses the cached payload instead of refetching.
  const ballot = useQuery({
    queryKey: ["ballot", districtIds.join(","), "vote_power"],
    queryFn: () =>
      apiRequest<BallotSummary>(
        `/api/ballot?district_ids=${encodeURIComponent(districtIds.join(","))}&sort=vote_power`
      ),
    enabled: me === null && districtIds.length > 0,
  });

  // No-flash rule: nothing until the session state is known.
  if (me === undefined) {
    return <LoadingNotice text="Loading…" />;
  }
  // Signed-in visitors have the real thing — and this is where a guest lands
  // right after registering from the CTA below (?next=/draft), by which time
  // the flush hook has replayed the draft into the account.
  if (me !== null) {
    return <Navigate to="/me/picks" replace />;
  }

  const pickCount = draftPickCount(draft);
  const choices = draftChoicesByElectionId(draft);
  const today = usLatestLocalDate();
  // Same strict date grouping as /me/picks, upcoming only: a draft is a plan,
  // and past races can no longer be picked.
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
    <div className="mx-auto max-w-3xl px-4 py-8">
      <h1 className="text-2xl font-bold">My Ballot Draft</h1>

      {districtIds.length === 0 ? (
        pickCount === 0 ? (
          <>
            <EmptyNotice text="Your ballot draft is empty." />
            <p className="text-center">
              <Link to="/" className="text-ink underline hover:text-rausch">
                Start with your address
              </Link>{" "}
              to see your elections and build your ballot.
            </p>
          </>
        ) : (
          // Deep-link entry: picks exist but no ballot was ever loaded, so
          // there is no race list to card — list the picks themselves and
          // point at the address search, the only page that can build the
          // real ballot around them.
          <>
            <ul className="mt-4 space-y-2">
              {[...choices.values()]
                .sort((a, b) =>
                  a.election_date < b.election_date ? -1 : a.election_date > b.election_date ? 1 : 0
                )
                .map((choice) => (
                  <li key={choice.election_id} className="text-sm">
                    <span className="text-ink-soft">{formatElectionDate(choice.election_date)} · </span>
                    <Link
                      to={`/elections/${choice.election_id}`}
                      state={DRAFT_NAV_STATE}
                      className="text-ink hover:text-rausch"
                    >
                      {choice.official_ballot_title}
                    </Link>
                    <span className="text-ink-soft"> — </span>
                    <span
                      className={
                        choice.measure_position === "no"
                          ? "font-semibold text-red-900"
                          : "font-semibold text-green-900"
                      }
                    >
                      {choice.measure_position !== null
                        ? choice.measure_position === "yes"
                          ? "Yes"
                          : "No"
                        : choice.picks.map((pick) => pick.display_name).join(", ")}
                    </span>
                  </li>
                ))}
            </ul>
            <p className="mt-3 text-sm text-ink-soft">
              <Link to="/" className="underline hover:text-ink">
                Search your address
              </Link>{" "}
              to see every race on your ballot.
            </p>
          </>
        )
      ) : (
        <>
          {ballot.isPending ? <LoadingNotice text="Loading your elections…" /> : null}
          {ballot.isError ? (
            <div className="mt-4">
              <ErrorNotice error={ballot.error} />
            </div>
          ) : null}
          {ballot.isSuccess ? (
            dates.length === 0 ? (
              <EmptyNotice text="No upcoming elections found for your districts yet. Check back — new elections are added as they are announced." />
            ) : (
              <div className="mt-4 space-y-4">
                {dates.map((date) => (
                  <PickDateCard
                    key={date}
                    date={date}
                    elections={byDate.get(date) ?? []}
                    choiceByElectionId={choices}
                    share={false}
                    navState={DRAFT_NAV_STATE}
                  />
                ))}
              </div>
            )
          ) : null}
          <p className="mt-3 text-sm text-ink-soft">
            <Link
              to={`/ballot?d=${encodeURIComponent(districtIds.join(","))}`}
              className="underline hover:text-ink"
            >
              View your full ballot
            </Link>{" "}
            to read up on each race.
          </p>
        </>
      )}

      {pickCount > 0 ? (
        <p className="mt-6 flex flex-wrap items-center gap-x-3 gap-y-1 text-sm">
          <Link
            to={`/register?next=${encodeURIComponent("/draft")}`}
            className="rounded-lg bg-rausch px-3 py-1.5 font-semibold text-white transition hover:bg-rausch-dark"
          >
            Sign up free to save your picks
          </Link>
          <span className="text-xs text-ink-soft">
            Your draft lives only on this device until you sign up.
          </span>
        </p>
      ) : null}
    </div>
  );
}

export default DraftPage;
