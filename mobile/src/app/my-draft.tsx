import type { BallotSummary, ElectionChoice, ElectionSummary, Me } from "@voteapp/api-client";
import {
  apiRequest,
  formatElectionDate,
  useElectionChoices,
  useMintPickCardShare,
} from "@voteapp/api-client";
import { useQuery } from "@tanstack/react-query";
import { Stack, useRouter } from "expo-router";
import { Pressable, ScrollView, Text, View } from "react-native";
import { AccountGate } from "../components/AccountGate";
import { Collapsible } from "../components/Collapsible";
import { RemoveStrandedPickButton } from "../components/ElectionChoiceControls";
import { ShareButton, SITE_ORIGIN } from "../components/ShareButton";
import { ErrorNotice, LoadingNotice } from "../components/Status";
import { VerifyPrompt } from "../components/VerifyPrompt";
import { openExternalUrl } from "../lib/openExternalUrl";
import { usLatestLocalDate } from "../lib/usLatestLocalDate";

// My Draft (port of the web PicksPage's list view): the pick cards — one per
// upcoming election day on the saved ballot: what's on it, who I picked,
// what's still undecided. Mobile drops the web page's ballot-sheet view
// (no facsimile on mobile yet) and its auto-pick controls (Phase 5), and has
// no guest branch — the screen is reached from the saved-ballot header,
// behind AccountGate.

// Inline outcome flags on a picked name. The web renders these as badge
// chips; React Native can't style nested Text spans with padding or rounded
// corners, so the same vocabulary rides as colored text instead. Each flag
// leads with a real space — without it the copy/accessible text runs the
// name into the label ("Jane SmithWon"). The certified writer projects
// advanced/runoff onto winners (everyone else becomes lost), so all five
// terminal statuses need a flag.
function pickStatusFlag(status: string) {
  if (status === "won" || status === "advanced" || status === "runoff") {
    return (
      <Text className="text-xs font-semibold text-green-700">
        {status === "won" ? " Won" : status === "advanced" ? " Advanced" : " In runoff"}
      </Text>
    );
  }
  if (status === "lost") {
    return <Text className="text-xs font-medium text-ink-soft"> Lost</Text>;
  }
  if (status === "withdrawn") {
    return <Text className="text-xs text-ink-soft"> (withdrew)</Text>;
  }
  return null;
}

// Outcome flag for a measure pick: the word states the FACT
// ("Passed"/"Failed"); the color says how it landed for the owner — green
// when the outcome matches their vote, muted otherwise. Anything but the
// writer's two canonical values (including a pre-field backend during
// deploy skew) renders nothing.
function measureOutcomeFlag(position: "yes" | "no", result: string | null | undefined) {
  if (result !== "passed" && result !== "failed") {
    return null;
  }
  const matchedPick = (result === "passed") === (position === "yes");
  const label = result === "passed" ? " Passed" : " Failed";
  return matchedPick ? (
    <Text className="text-xs font-semibold text-green-700">{label}</Text>
  ) : (
    <Text className="text-xs font-medium text-ink-soft">{label}</Text>
  );
}

// Result-derived flag for a pick the candidacy pipeline hasn't labeled yet:
// election-night calls arrive as result rows long before
// candidate_elections.status flips. Id-only matching and decisive outcomes
// only — and silence when the pick isn't among the winners (the card
// announces the payoff, it doesn't rub in the loss).
function pickResultFlag(
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
  return (
    <Text className="text-xs font-semibold text-green-700">
      {outcome === "won" ? " Won" : outcome === "advanced" ? " Advanced" : " In runoff"}
    </Text>
  );
}

// Marks a row the auto-pick engine wrote (origin = 'auto', set from the web
// — the account is shared); a manual re-pick clears it.
function autoFlag() {
  return <Text className="text-xs text-ink-soft"> · Auto</Text>;
}

// Text-only body of a picked row ("Jane Doe Won · Auto" / "Yes Passed").
// Text can only nest Text in React Native, so the stranded-pick Remove
// button renders separately below the row (StrandedRemoveButtons).
function PickedLine({ choice, election }: { choice: ElectionChoice; election?: ElectionSummary }) {
  // The canonical result reaches this line two ways: via the ballot summary
  // while the race is still carded, and via the choice itself afterwards
  // (attached on the choices list read).
  const resultOutcome = election?.current_result_outcome ?? choice.current_result_outcome;
  const resultWinners = election?.current_result_winners ?? choice.current_result_winners;
  if (choice.measure_position !== null) {
    return (
      <Text className={choice.measure_position === "yes" ? "font-semibold text-green-900" : "font-semibold text-red-900"}>
        {choice.measure_position === "yes" ? "Yes" : "No"}
        {measureOutcomeFlag(choice.measure_position, choice.measure_result ?? resultOutcome)}
        {choice.measure_origin === "auto" ? autoFlag() : null}
      </Text>
    );
  }
  return (
    <Text className="font-semibold text-green-900">
      {choice.picks.map((pick, index) => (
        <Text key={pick.candidate_id}>
          {index > 0 ? ", " : null}
          {pick.display_name}
          {/* candidacy_status (certified won/lost, withdrawn) outranks the
              result-derived flag — never both. */}
          {pickStatusFlag(pick.candidacy_status) ?? pickResultFlag(resultOutcome, resultWinners, pick.candidate_id)}
          {pick.origin === "auto" ? autoFlag() : null}
        </Text>
      ))}
    </Text>
  );
}

// A withdrawn pick on an upcoming race is otherwise unremovable: the
// election screen's roster no longer lists the candidacy, yet the pick
// still counts toward the seat cap. Date-gated because the backend rejects
// writes to past elections.
function StrandedRemoveButtons({ choice, today }: { choice: ElectionChoice; today: string }) {
  if (choice.measure_position !== null || choice.election_date < today) {
    return null;
  }
  const stranded = choice.picks.filter((pick) => pick.candidacy_status === "withdrawn");
  if (stranded.length === 0) {
    return null;
  }
  return (
    <View className="mt-1 gap-1">
      {stranded.map((pick) => (
        <RemoveStrandedPickButton
          key={pick.candidate_id}
          electionId={choice.election_id}
          candidateId={pick.candidate_id}
          candidateName={pick.display_name}
        />
      ))}
    </View>
  );
}

// One decided row outside the date cards (uncarded upcoming, past): the
// choices payload alone carries title + date, so no ballot data is needed.
function ChoiceRow({ choice, today }: { choice: ElectionChoice; today: string }) {
  const router = useRouter();
  return (
    <View>
      <Text className="text-sm">
        <Text className="text-ink-soft">{formatElectionDate(choice.election_date)} · </Text>
        <Text
          className="text-ink underline"
          accessibilityRole="link"
          onPress={() => router.push(`/elections/${choice.election_id}`)}
        >
          {choice.official_ballot_title}
        </Text>
        <Text className="text-ink-soft"> — </Text>
        <PickedLine choice={choice} />
      </Text>
      <StrandedRemoveButtons choice={choice} today={today} />
    </View>
  );
}

function ShareCardControl({ electionDate }: { electionDate: string }) {
  const mint = useMintPickCardShare();
  if (mint.isSuccess) {
    const path = `/picks/${mint.data.share.token}`;
    return (
      <View className="mt-2 gap-2">
        {/* The minted URL is the deliverable — visible the moment it exists.
            It points at the public WEBSITE (there is no native card viewer;
            recipients mostly don't have the app, and the web page carries
            the og card) — tapping it opens the browser so the sharer can
            see exactly what recipients will. */}
        <Text
          className="text-xs text-ink underline"
          accessibilityRole="link"
          onPress={() => openExternalUrl(`${SITE_ORIGIN}${path}`)}
        >
          {`${SITE_ORIGIN.replace(/^https?:\/\//, "")}${path}`}
        </Text>
        <View className="flex-row items-center gap-2">
          <ShareButton path={path} shareText={`My ${formatElectionDate(electionDate)} election picks`} />
          {/* Names the name: the public page shows the owner's first name,
              and the sharer must learn that HERE, before posting the link. */}
          <Text className="flex-1 text-xs text-ink-soft">
            Anyone with the link can see this card and your first name.
          </Text>
        </View>
      </View>
    );
  }
  return (
    <View className="mt-2 flex-row flex-wrap items-center gap-2">
      <Pressable
        disabled={mint.isPending}
        onPress={() => mint.mutate(electionDate)}
        accessibilityRole="button"
        accessibilityLabel={`Share my ${formatElectionDate(electionDate)} picks`}
        className={`self-start rounded-lg border border-line bg-white px-3 py-1.5 active:border-ink${mint.isPending ? " opacity-50" : ""}`}
      >
        <Text className="text-sm font-medium text-ink">{mint.isPending ? "…" : "Share"}</Text>
      </Pressable>
      {mint.isError ? (
        <Text className="text-xs font-medium text-red-800">Couldn&apos;t create the share link — try again.</Text>
      ) : null}
    </View>
  );
}

// The label-worthiness rule the cards count by: a choice whose only pick
// lost its candidate (deleted/merged) renders nothing and counts as
// undecided. Same rule as the web page and isDecidedChoice.
function hasRenderablePick(choice: ElectionChoice | undefined): choice is ElectionChoice {
  return choice !== undefined && (choice.picks.length > 0 || choice.measure_position !== null);
}

// Per-election-day card: x/y progress, share, one row per race.
function PickDateCard({
  date,
  elections,
  choiceByElectionId,
  today,
}: {
  date: string;
  elections: ElectionSummary[];
  choiceByElectionId: Map<string, ElectionChoice> | undefined;
  today: string;
}) {
  const router = useRouter();
  const pickedCount = elections.filter((election) => hasRenderablePick(choiceByElectionId?.get(election.id))).length;
  // Cards outlive their election day (the ballot keeps finished races for a
  // few days so results can land on them); once the date passes, "no pick
  // yet" would invite an action that's no longer possible.
  const isPast = date < today;
  return (
    <View className="rounded-xl border border-line bg-white p-4">
      <Text className="text-lg font-semibold text-ink">My {formatElectionDate(date)} Election Draft</Text>
      <Text className="mt-0.5 text-xs text-ink-soft">
        {pickedCount} of {elections.length} race{elections.length === 1 ? "" : "s"} decided
      </Text>
      {/* Mint-on-demand: no share row exists until the user asks for one.
          Hidden entirely while the card has zero picks — the backend
          refuses to mint for an empty card anyway. */}
      {pickedCount > 0 ? <ShareCardControl electionDate={date} /> : null}
      <View className="mt-3 gap-2">
        {elections.map((election) => {
          const choice = choiceByElectionId?.get(election.id);
          return (
            <View key={election.id}>
              {hasRenderablePick(choice) ? (
                <>
                  <Text className="text-sm">
                    <Text
                      className="text-ink underline"
                      accessibilityRole="link"
                      onPress={() => router.push(`/elections/${election.id}`)}
                    >
                      {election.official_ballot_title}
                    </Text>
                    <Text className="text-ink-soft"> — </Text>
                    <PickedLine choice={choice} election={election} />
                  </Text>
                  <StrandedRemoveButtons choice={choice} today={today} />
                </>
              ) : (
                // Undecided: the whole line is the quiet call to action —
                // grey, tappable, straight to the race.
                <Text
                  className="text-sm text-ink-soft underline"
                  accessibilityRole="link"
                  onPress={() => router.push(`/elections/${election.id}`)}
                >
                  {election.official_ballot_title} — {isPast ? "no pick" : "no pick yet"}
                </Text>
              )}
            </View>
          );
        })}
      </View>
    </View>
  );
}

// Upcoming picks the date cards do NOT show, from the choices payload alone.
// The choice API accepts a pick on ANY upcoming race with a valid candidacy
// — via candidate search, a shared link, or before an address change —
// while the cards render only the saved ballot, so without this section
// such a pick would silently vanish from the screen that claims to list
// "My Election Draft". Also the whole list for the unverified render, where
// no ballot loads and nothing is carded.
function UpcomingUncardedPicks({
  title,
  choices,
  today,
  cardedElectionIds,
}: {
  title: string;
  choices: ElectionChoice[];
  today: string;
  cardedElectionIds: Set<string>;
}) {
  const upcoming = choices
    .filter((choice) => choice.election_date >= today && !cardedElectionIds.has(choice.election_id))
    .filter((choice) => choice.picks.length > 0 || choice.measure_position !== null)
    // Soonest first — the reverse of PastPicks: what's next matters most.
    .sort((a, b) => (a.election_date < b.election_date ? -1 : a.election_date > b.election_date ? 1 : 0));
  if (upcoming.length === 0) {
    return null;
  }
  return (
    <View className="mt-6">
      <Text className="text-sm font-semibold uppercase tracking-wide text-ink-soft">{title}</Text>
      <View className="mt-3 gap-2">
        {upcoming.map((choice) => (
          <ChoiceRow key={choice.election_id} choice={choice} today={today} />
        ))}
      </View>
    </View>
  );
}

// Past picks come from the choices payload alone, not the ballot: the saved
// ballot only keeps recently finished elections, while picks history should
// survive indefinitely. Races still carded above (the ballot's
// just-finished window) are excluded — their picks are already on display,
// with results; they fall in here when the ballot drops them.
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
    <View className="mt-4">
      <Collapsible summary={`Past elections (${past.length})`}>
        <View className="mt-3 gap-2">
          {past.map((choice) => (
            <ChoiceRow key={choice.election_id} choice={choice} today={today} />
          ))}
        </View>
      </Collapsible>
    </View>
  );
}

function MyDraftBody({ me }: { me: Me }) {
  const router = useRouter();
  const verified = me.email_verified;
  const { choices, choiceByElectionId, isLoading: choicesLoading, isError: choicesError } = useElectionChoices();
  // Same key AND url as useMyPicksProgress (the saved-ballot header's
  // counter), so opening this screen from the header is ONE shared request
  // — in paper-ballot contest order, the user's saved list preferences
  // never applying here. Deliberately NOT the ["me", "ballot"] key: the
  // saved-ballot tab owns that one with the user's saved sort. No
  // include=preview — that include exists for the web's ballot-sheet view.
  const ballot = useQuery({
    queryKey: ["me", "ballot", "picks"],
    queryFn: () => apiRequest<BallotSummary>("/api/me/ballot?sort=state_baseline&followed_first=false"),
    enabled: verified,
    retry: false,
    staleTime: 60_000,
  });

  const today = usLatestLocalDate();

  if (!verified) {
    // The verify wall must not hide the picks themselves: the choice API
    // deliberately accepts any registered session — only the
    // address-derived ballot stays verified-gated. Nothing is carded (the
    // ballot query never ran), so the upcoming section lists every decided
    // choice. Same rule as the web page.
    const nothingCarded = new Set<string>();
    return (
      <ScrollView className="flex-1 bg-white" contentContainerClassName="pb-10">
        <VerifyPrompt email={me.email} />
        <View className="px-4">
          {choicesLoading && !choicesError ? <LoadingNotice text="Loading your picks…" /> : null}
          {choicesError ? (
            // Without this, a failed fetch is indistinguishable from an
            // empty pick history under the verify prompt.
            <Text className="mt-4 rounded-lg border border-rausch/40 bg-rausch/5 px-3 py-2 text-sm text-rausch-dark">
              Could not load your picks — try again shortly.
            </Text>
          ) : (
            <>
              <UpcomingUncardedPicks
                title="Your upcoming picks"
                choices={choices ?? []}
                today={today}
                cardedElectionIds={nothingCarded}
              />
              <PastPicks choices={choices ?? []} today={today} cardedElectionIds={nothingCarded} />
            </>
          )}
        </View>
      </ScrollView>
    );
  }

  // Strict date grouping: cards are "everything you face on this day", and
  // within a day the payload's ballot order stands as-is. No date filter of
  // our own: the ballot payload already keeps just-finished elections for a
  // few days, and the card should live exactly as long — results land right
  // on it before it retires to Past elections.
  const byDate = new Map<string, ElectionSummary[]>();
  for (const election of ballot.data?.elections ?? []) {
    const group = byDate.get(election.election_date) ?? [];
    group.push(election);
    byDate.set(election.election_date, group);
  }
  const dates = [...byDate.keys()].sort();
  const cardedElectionIds = new Set((ballot.data?.elections ?? []).map((election) => election.id));

  // One reveal: nothing below the heading until BOTH queries settle, and no
  // cards at all when the choices fetch failed — rendering them from an
  // unloaded map would claim "no pick yet" on races already decided, and a
  // visible error beats confidently wrong "0 of N decided" cards.
  const choicesReady = choiceByElectionId !== undefined;
  const picksSettled = ballot.isSuccess && choicesReady;
  // Ballot failed but choices loaded: the choice-only sections (upcoming,
  // past) need nothing from the ballot, so they must survive the failure —
  // an error line plus the whole saved history beats an error line alone.
  // Nothing is carded then, so every decided choice lists as uncarded.
  const choiceListsReady = choicesReady && (ballot.isSuccess || ballot.isError);

  return (
    <ScrollView className="flex-1 bg-white" contentContainerClassName="px-4 py-8">
      <Text className="text-2xl font-bold text-ink">My Election Draft{dates.length > 1 ? "s" : ""}</Text>
      {ballot.isPending || (choicesLoading && !choicesError) ? <LoadingNotice text="Loading your elections…" /> : null}
      {ballot.isError ? (
        <View className="mt-4">
          <ErrorNotice error={ballot.error} />
        </View>
      ) : null}
      {choicesError ? (
        // Not ErrorNotice: this screen has two failure slots (ballot,
        // picks) that must stay tellable apart. Same visual shell,
        // specific words.
        <Text className="mt-4 rounded-lg border border-rausch/40 bg-rausch/5 px-3 py-2 text-sm text-rausch-dark">
          Could not load your picks — try again shortly.
        </Text>
      ) : null}
      {picksSettled && dates.length === 0 ? (
        ballot.data.district_ids.length === 0 ? (
          // No saved address (empty district set): the ask is the whole
          // message. Same green as AddressNudge — one color for the "give
          // address → see your ballot" action, wherever it appears.
          <View className="mt-3 rounded-md border border-nudge-line bg-nudge px-3 py-2">
            <Text className="text-sm text-ink">
              <Text
                className="font-medium text-nudge-deep underline"
                accessibilityRole="link"
                onPress={() => router.push("/settings/address")}
              >
                Set your address
              </Text>{" "}
              to see your races.
            </Text>
          </View>
        ) : (
          // Address is saved and the lookup ran — there genuinely are no
          // upcoming elections. No CTA: there is nothing for them to do.
          <Text className="mt-3 text-sm text-ink-soft">No upcoming elections on your ballot yet.</Text>
        )
      ) : null}
      {picksSettled ? (
        <View className="mt-4 gap-4">
          {dates.map((date) => (
            <PickDateCard
              key={date}
              date={date}
              elections={byDate.get(date) ?? []}
              choiceByElectionId={choiceByElectionId}
              today={today}
            />
          ))}
        </View>
      ) : null}
      {choiceListsReady ? (
        <>
          <UpcomingUncardedPicks
            // With no cards above (ballot failed), "Other" would point at
            // nothing — these ARE the upcoming picks.
            title={ballot.isSuccess ? "Other upcoming picks" : "Your upcoming picks"}
            choices={choices ?? []}
            today={today}
            cardedElectionIds={cardedElectionIds}
          />
          <PastPicks choices={choices ?? []} today={today} cardedElectionIds={cardedElectionIds} />
        </>
      ) : null}
    </ScrollView>
  );
}

export default function MyDraftScreen() {
  return (
    <View className="flex-1 bg-white">
      <Stack.Screen options={{ title: "My Election Draft" }} />
      {/* allowUnverified: the verify wall must not hide the picks — the
          body renders VerifyPrompt plus the pick lists itself. */}
      <AccountGate signedOutText="Log in to plan your votes." allowUnverified>
        {(me) => <MyDraftBody me={me} />}
      </AccountGate>
    </View>
  );
}
