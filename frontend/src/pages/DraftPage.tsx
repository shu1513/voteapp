import { useState } from "react";
import { Link, Navigate } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { apiRequest, formatElectionDate, useMe } from "@voteapp/api-client";
import type { BallotSummary, ElectionChoice, ElectionSummary } from "@voteapp/api-client";
import { BallotPreviewSheets, BallotViewToggle } from "../components/BallotPreview";
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

// Bare pick lines rendered straight from draft rows (date · race — choice),
// for the picks no ballot card can carry: the no-ballot-context fallback,
// and picks made off the stored ballot via a shared or searched link.
function DraftChoiceRows({ rows }: { rows: ElectionChoice[] }) {
  const sorted = [...rows].sort((a, b) =>
    a.election_date < b.election_date ? -1 : a.election_date > b.election_date ? 1 : 0
  );
  return (
    <ul className="mt-3 space-y-2">
      {sorted.map((choice) => (
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
              choice.measure_position === "no" ? "font-semibold text-red-900" : "font-semibold text-green-900"
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
  );
}

export function DraftPage() {
  useDocumentTitle("My Ballot Draft");
  const { me } = useMe();
  const draft = useBallotDraft();
  const districtIds = draft.district_ids;
  const [view, setView] = useState<"list" | "ballot">("list");
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
  // Ballot view payload, fetched lazily on first toggle (separate query on
  // purpose: widening the shared default query above would break its cache
  // reuse with /ballot). Same contract as the signed-in preview fetch.
  const ballotPreview = useQuery({
    queryKey: ["ballot", districtIds.join(","), "preview"],
    queryFn: () =>
      apiRequest<BallotSummary>(
        `/api/ballot?district_ids=${encodeURIComponent(districtIds.join(","))}&include=preview&sort=state_baseline&followed_first=false`
      ),
    enabled: me === null && districtIds.length > 0 && view === "ballot",
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
  // Draft rows the date cards won't display, keyed off the ids actually
  // rendered (not the raw payload): a pick on a just-finished race still in
  // the ballot response would otherwise vanish from both the cards and this
  // list.
  const cardedIds = new Set([...byDate.values()].flat().map((election) => election.id));
  const extraRows = [...choices.values()].filter((choice) => !cardedIds.has(choice.election_id));

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
            <DraftChoiceRows rows={[...choices.values()]} />
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
              <>
                <div className="mt-4">
                  <BallotViewToggle view={view} onChange={setView} />
                </div>
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
                        choiceByElectionId={choices}
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
                        choiceByElectionId={choices}
                        share={false}
                        navState={DRAFT_NAV_STATE}
                      />
                    ))}
                  </div>
                )}
              </>
            )
          ) : null}
          {/* Picks the cards above can't carry — made on races outside the
              stored ballot (a shared or searched link) or on races the
              upcoming cards no longer show. A saved pick must never be
              invisible on this page: the header badge and the signup CTA
              both count it, so hiding it here reads as lost. Only rendered
              once the ballot settles — before that, "outside the cards" is
              unknowable. */}
          {ballot.isSuccess && extraRows.length > 0 ? (
            <section className="mt-6">
              <h2 className="text-lg font-semibold text-ink">Other saved picks</h2>
              <p className="mt-0.5 text-xs text-ink-soft">
                Races you picked from a direct link — not part of the ballot above.
              </p>
              <DraftChoiceRows rows={extraRows} />
            </section>
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
