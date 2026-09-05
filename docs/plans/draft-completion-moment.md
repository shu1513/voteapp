# Draft completion moment ("You have completed your … draft")

Status: PR 1 (notice + plumbing) merged 2026-09-04 (#1108); PR 2
(draft-page milestone, `DraftMilestone.tsx`) implemented 2026-09-04.

## Idea

Today nothing marks the moment a user decides the last race on their
ballot. The header label quietly flips from "My Draft 12/13" to "My Draft ✓"
and that is all. We want a visible finish line: tell the user every race
has a pick, name the election day, and hand them the next step (review the
draft, sign up to save it).

## Decision

One app-level completion notice, driven by shared progress state, shown
once per election day, plus a milestone line on the draft pages. No
auto-redirect. No modal. No new machinery for seats, withdrawals, or
partial ballots.

Why not auto-redirect to the draft page:

- Multi-seat races (pick 2 of 3) would be interrupted mid-race.
- The undo affordance (re-click the pick control) is on the page the user
  is standing on. `PostPickActions.tsx` already bans navigation on save for
  this reason.
- Auto pick fills several races at once; the user wants to review the page
  they are on, not be moved off it.

Why not key the celebration to the pick click:

- Completion is not always a click on a pick card. Auto pick on the
  election page (`AutoPickControl`) and batch auto-fill on `/me/picks`
  (`AutoPickFillControl`) can finish the draft with no pick card on screen.
  A pick in another tab can finish it too.
- The sticky pick card on the candidate page is bottom-anchored and already
  holds a button plus two links. A banner in it is cramped on a phone.

So the trigger is the shared progress value going from a known incomplete
value to a known complete value, watched in one place (`App.tsx`), not the
click that caused it.

## Wording

The counting rule (`isDecidedChoice` in
`packages/api-client/src/useElectionChoices.ts`) treats a race as decided
when it has at least one pick or a Yes/No position. One pick decides a
multi-seat race even with seats left; a withdrawn pick still counts; ZIP
and city searches produce partial ballots that can reach 100%. So "your
ballot is complete" and "you're done" promise more than the data
establishes. We keep the counting rule and say only what it establishes:

- Headline: "You have completed your November 3, 2026 draft."
- Action: "Review my picks"

Owner decision 2026-09-04, after seeing it live: the earlier hedged copy
("Picks added for every race…", "13 of 13 races decided. Review your picks
and make any changes.") was replaced by the one sentence above. "Completed"
therefore leans on the counting rule described here; the date card's own
"N of M races decided" line, directly under the milestone, is the only
remaining hedge.

No mention of sharing in the notice: guests cannot share until they have
an account, and signed-in users find Share on the date card already.

## What exists (verified 2026-09-04)

- Progress is already global and shared:
  `packages/api-client/src/pickProgress.ts` exports `nearestDayPickProgress`
  and `myDraftLabel` (`{picked, total, complete}` over the nearest upcoming
  election day). Web hooks in `frontend/src/lib/usePickProgress.ts`:
  `useMyPicksProgress` (signed in, reads the cached
  `["me","ballot","preview"]` query, null until it resolves or while the
  email is unverified) and `useGuestDraftNav` (guest, reads the localStorage
  draft via `draftProgress`; exposes a label and `complete`, not the
  counts). `App.tsx` already calls both for the header label.
- `useMe()` returns `me` as `undefined` while loading and `null` for a
  guest.
- Guest draft store `frontend/src/lib/ballotDraft.ts`: localStorage key
  `voteapp_ballot_draft`, carries `target.election_date` and
  `target.election_ids` (the denominator) once the guest has viewed
  `/ballot`. `draftProgress` reads the stored target with no date check, and
  the target is refreshed only by `BallotPage`. `/draft` fetches its own
  ballot and filters past dates, so after the target day passes the header
  and the draft page can disagree.
- Draft pages: `/draft` (`DraftPage.tsx`, guest, "Sign up free to save your
  picks" CTA at the bottom, upcoming dates only) and `/me/picks`
  (`PicksPage.tsx`, keeps just-finished dates for a few days, so the first
  date is not always upcoming). Both have a List / Ballot preview toggle;
  `PickDateCard` (with its "N of M races decided" line and, signed in, the
  per-date Share control) renders only in List view.
- Printing exists on both draft pages through Ballot preview → "Print this
  preview" (`BallotPreview.tsx`, `ballot-print-area`). Nothing to add.
- Stacking: the candidate and election sticky pick cards, the chatbot
  launcher and panel, and the terms gate all use `z-30`; the sticky header
  is `z-20`. There is no free layer between the pick card and the chatbot.
- No shared toast component. Existing inline `role="status"` notices only.
- Mobile has its own `useMyPicksProgress` in `mobile/src/lib/` and reads
  `PickProgress` through `myDraftLabel`. Mobile UI is out of scope here.

## Design

### 1. Completion notice (new component, mounted in `App.tsx`)

`DraftCompleteNotice.tsx` in `frontend/src/components/`.

Progress source:

- Signed in: `useMyPicksProgress()`. Guest (`me === null` only, never while
  `me` is `undefined`): `useGuestPickProgress()`, the same `draftProgress`
  value the guest header link is built from. Both return the nullable
  progress with its `election_date`; see Plumbing.

Trigger:

- Baseline: the last non-null progress seen for the tracked ballot, which
  is identity (`me.email` or "guest") + `election_date` + that day's sorted
  race ids. Fire only when a later progress for the same ballot goes from
  `complete: false` to `complete: true`. Null never counts as false; a
  returning user whose first resolved value is complete gets nothing.
- Reset the baseline when identity, `election_date`, or the race list
  changes. Switching ballots or a denominator shrinking from 1/2 to 1/1
  (address change, retired race) must not manufacture a completion, and
  must not consume the day's once-only marker.
- Null progress (draft cleared, ballot with no upcoming races, identity
  unresolved) drops an open notice and resets the baseline: nothing
  confirms the message any more, and a pick made during an unknown gap is
  simply not observed.
- Once per election day per browser: on fire, add the date to
  `voteapp_draft_complete_seen` (a JSON array of dates) in localStorage
  (`frontend/src/lib/draftCompleteSeen.ts`). Skip firing when the date is
  already present. Reads and writes are wrapped in try/catch with an
  in-memory set as fallback; the check consults memory first, so a browser
  whose reads work but whose writes fail still suppresses repeats for the
  tab's life. This is browser-local suppression shared by every account on
  the browser, not per-user across devices. Good enough.
- Suppressed routes (`/draft`, `/me/picks`, `/`): keep observing progress
  so the baseline stays current, render nothing, and do not queue the
  notice for later. The draft-page milestone (section 2) marks the date as
  seen when it renders, so arriving there counts as acknowledged.

Lifetime:

- Stays until the user dismisses it, clicks "Review my picks", or progress
  for that date becomes incomplete again (unpick). It survives route
  changes on purpose: "Back to election" after the last pick should not
  make it vanish before the user reads it. It is hidden on the suppressed
  routes regardless.

Presentation:

- A compact notice rendered as an extra row inside the sticky header
  (`App.tsx`), not a fixed overlay. That keeps it clear of the bottom pick
  card and its undo control, needs no z-index, and pushes content down
  instead of covering it. Green-800 accent to match the header's complete
  state.
- `role="status"` (polite live region) on the message. No focus movement,
  no `role="dialog"`: this is a success notification with optional actions,
  and keyboard users keep their place, including when completion arrives
  from another tab. Ordinary link ("Review my picks", to `/draft` for
  guests, `/me/picks` signed in) and a dismiss button. Escape is not
  special.
- SSR renders nothing (progress is null there).

### 2. Milestone on the draft pages

`DraftMilestone.tsx` in `frontend/src/components/`, rendered by both pages
above the List / Ballot preview toggle (so it shows in both views), for
the nearest upcoming date only. It takes the day's elections and the
choice map, counts with `isDecidedChoice` (the same rule as the date
card's count line), and renders nothing unless every race is decided:

- Nearest upcoming = the first date on or after today in the page's date
  list. On `/me/picks` that is not always `dates[0]` because just-finished
  dates stay carded for a few days.
- Render when that date's decided count equals its race count: check icon,
  "You have completed your November 3, 2026 draft.", and the
  "13 of 13 races decided" line.
- Guest: a "Sign up free to save your picks" link (same wording and
  target, `/register?next=/draft`, as the existing bottom CTA, which
  stays).
- Signed in: no extra action. Share is already on the date card in List
  view.
- Rendering the milestone adds the date to `voteapp_draft_complete_seen`.

`PickDateCard` itself is unchanged; its count line stays.

### 3. Pick card: no change

The post-pick link already reads "My Draft ✓" through `myDraftLabel` once
the day is complete. Leave `PostPickActions.tsx` alone.

### 4. Guest target expiry (small correctness fix)

- `draftProgress` (or `useGuestDraftNav`) treats a stored target whose
  `election_date` is before `usLatestLocalDate()` as absent: progress null,
  header label falls back to the pick-count form.
- `DraftPage` calls `setDraftBallotContext` on a successful ballot load,
  the same way `BallotPage` does, so either guest page refreshes the
  denominator. No scheduler; without another load the next denominator
  stays unknown, which is fine.

## Plumbing

- `nearestDayPickProgress` returns `election_date` and `election_ids`
  alongside `picked/total/complete`. This widens the shared `PickProgress`
  type used by mobile; `myDraftLabel` ignores the new fields. Run the
  api-client tests and the mobile typecheck.
- `useMyPicksProgress` is unchanged in shape (it returns the widened
  object).
- New `useGuestPickProgress()` in `usePickProgress.ts` exposes the guest
  progress directly; `useGuestDraftNav` builds its label from it and keeps
  its shape.
- `draftProgress(draft, today)` takes the calendar date and returns null
  for an expired target; `nearestUpcomingTarget(elections, today)` moved
  from `BallotPage` into `ballotDraft.ts` so `DraftPage` can share it.

## PRs

1. Plumbing + notice: `election_date` on progress, `useGuestDraftNav`
   exposing progress, guest target expiry, `DraftPage` refreshing the
   target, `DraftCompleteNotice` mounted in the header.
2. Draft-page milestone on `/draft` and `/me/picks`.

Both are frontend only. No migrations, no API change, no flags.

## Tests

- Notice: fires on a known false → true transition; does not fire when the
  first resolved progress is complete (asynchronous initial load); does not
  fire on identity change or `election_date` change; not while `me` is
  `undefined`; once per date across unpick/repick; hidden on `/draft`,
  `/me/picks`, `/`; dismisses when progress becomes incomplete while open;
  link target per session kind; storage failure falls back to memory
  without throwing; `role="status"` present, no focus movement.
- Guest progress: expired target yields null; `/draft` load refreshes the
  target.
- Milestone: appears only when the nearest upcoming date is fully decided;
  a past card before the nearest upcoming card is ignored; visible in both
  List and Ballot preview views; guest gets the sign-up link; wording shows
  the count, never "complete".
- Browser check: narrow-screen header row does not overlap the sticky pick
  card; keyboard position preserved when the notice appears.

## Edge cases

- Guest who picked from a deep link without viewing `/ballot`: no target,
  progress null, nothing fires.
- Ballot with zero upcoming races: progress null, nothing fires.
- Unpick after completion: header and milestone revert; the notice closes;
  it does not re-fire for that date (seen marker).
- Guest completes, then registers: the draft flushes to the account
  asynchronously. The signed-in hook's first resolved value is then already
  complete (baseline complete, no fire), and the seen marker holds the date
  anyway.
- Signed-in picks have no cross-tab broadcast; another tab sees completion
  only after a refetch (for example window-focus refetch). Guest drafts do
  broadcast via the `storage` event. Either way the notice appears once.
- Nearest day passes: guest target expires to null; signed-in progress
  moves to the next date, which starts a fresh baseline.

## Mobile (Expo app)

Ported 2026-09-04, same rules with three deliberate differences:

- Account only. Mobile has no guest draft, so there is one progress source
  (`mobile/src/lib/useMyPicksProgress.ts`, the saved-ballot header's
  counter) and no sign-up link on the milestone.
- Notice placement: RN has no shared header row and the codebase forbids
  absolute overlays (the pick screens use "root View + footer sibling" so
  nothing can cover the pick footer). The notice
  (`mobile/src/components/DraftCompleteNotice.tsx`) therefore mounts in
  flow at the top of the two screens where picks are made — the election
  screen and the candidate screen — above their ScrollView. Per-screen
  instances: the baseline starts from the cached progress on mount, so
  arriving on a screen with an already-complete draft never fires. Screens
  beneath the focused one stay mounted in a native stack, so only the
  focused instance fires and announces (`useIsFocused`); the others keep
  their baseline current and stay silent. Losing focus, an unpick, a ballot
  change, or unknown progress clears the fired notice at render time (not
  just hides it), so an unpick → repick cannot resurrect a notice the seen
  marker already ruled out. No root-layout mount, no app-wide ballot query.
- Seen marker is AsyncStorage (`mobile/src/lib/draftCompleteSeen.ts`, same
  key and per-date array as the web), so the check is async; the effect
  guards the resolved promise against an unpick or unmount in between.

Same as the web: known incomplete → known complete for the same identity +
day + race list; null breaks the chain; wording never says "complete";
polite live region (Android) plus an explicit VoiceOver announcement;
"Review my picks" pushes `/my-draft`; the My Draft screen's milestone
(`mobile/src/components/DraftMilestone.tsx`, nearest upcoming date, above
the cards) marks the day as seen, which also covers batch auto-fill done
on that screen. Mobile has no test runner; verification is typecheck,
lint, and a simulator run.

## Out of scope

- Seat-completion or abstention rules, partial-ballot wording changes.
- Printing (already exists in Ballot preview), confetti, animation, email
  or push on completion.
