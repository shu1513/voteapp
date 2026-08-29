# Mobile feature parity — stance summary, picks, auto-pick, auth

Status: planned 2026-08-28, revised same day after review. Phase 1
implemented in PR #925 (this plan's own PR); Phase 2 implemented in
PR #928 (2026-08-28); phases 3–6 not started.

## Context

The web frontend has shipped a wave of decision-making features that the Expo
app never received. Audit (2026-08-28) compared `mobile/src` against
`frontend/src` and `packages/api-client`.

**Already at parity (nothing to do):**

- Partial-address ballot (ZIP / city / region), anonymous address resolve with
  the `pendingDistricts` post-signup handoff.
- Vote power badges, tier rename ("Normal"), 15px `text-sm` raise.
- Finance summary cards, records grouped by research area, follows,
  ballot filters / sort chips, push notifications, terms gates.

**Missing on mobile:** election choices ("my pick"), the My Draft / My Picks
page, pick-card sharing, auto-pick, issue direction + hard-veto editing,
Google Sign-In, guest ballot draft, chatbot, ballot facsimile, membership.
(The candidate stance summary was on this list; Phase 1 shipped it.)

**What is and is not shared today.** `@voteapp/api-client` exports the core
choice hooks and every choice/auto-pick type: `useElectionChoices`,
`useSetElectionChoice`, `useElectionChoiceSaving`, `useAutoPick`
(`packages/api-client/src/useElectionChoices.ts`, `types.ts:522-620`), plus
the stance aggregation helpers (`aggregateRecordAreaStances`,
`EVALUATIVE_AREA_SLUGS`, research-area priority sort). Still **web-local** and
needed by these phases:

- `useMyDistricts` (`frontend/src/lib/useMyDistricts.ts`) — deliberately in
  frontend: it composes the account districts fetch with the localStorage
  guest draft, which the shared package cannot depend on.
- `useMyPicksProgress` / `myDraftLabel` (`frontend/src/lib/usePickProgress.ts`).
- The share-mint mutation (`POST /api/me/pick-card-shares`, inline in
  `PicksPage.tsx:187`).
- Auto-pick fill + date-scoped clear mutations (inline in
  `AutoPickFillControl.tsx:106,124`).
- ~~`classifyStanceSummary`~~ — moved to `researchAreaScoring.ts` in Phase 1.

No backend work is needed for phases 1–5; the auth epic may need none either
(see phase 6).

## Scope decisions

- **Skip the guest ballot draft** (`ballotDraft.ts`, `DraftPage`). It exists
  on web for anonymous SEO traffic; porting means an AsyncStorage draft plus
  merge-flush wiring. App installers can register. Revisit only if guest
  usage data demands it.
- **Guests on mobile see no pick controls.** The election and candidate
  screens are public (no `AccountGate`), so this must be explicit: where a
  pick control would render on an upcoming race, a logged-out viewer gets one
  small "Log in to plan your ballot" line routing to `/auth/login` — no
  yellow, no per-candidate buttons. Logged-out past races render nothing.
- **Membership: web-only signup, Netflix pattern (decided 2026-08-28).**
  Paid membership is never sold inside the app — no in-app purchase, no
  in-app Stripe checkout, ever. This avoids the app stores' 30% cut and
  IAP-rule exposure entirely. The mobile build ships at most a small
  settings row, rendered only when `GET /api/me/membership` says
  `enabled: true` (`GET /api/me/membership`): for members it shows
  honorary-member status with
  "manage your membership on the website"; for non-members, a short
  "become an honorary member on our website" line (web PR #920's
  vocabulary; the web destination is `/support`, and the shared
  `MembershipMembership` type in `@voteapp/api-client` already carries the
  status the row needs). Whether that line may be a tappable link (and
  whether price may be named) depends on the App Store 3.1.1 /
  anti-steering rules and Google Play billing policy **current at build
  time** — default to the Netflix-minimal form (informational text, no
  link, no price) and add a link only if the then-current rules clearly
  allow it.
  **Corollary now that membership carries a benefit** (#920's honorary-
  member pitch includes private analysis reports): those reports must never
  be delivered or unlocked inside the app. The moment the app gates any
  content on membership, Apple's IAP rules apply to the membership itself
  and the web-only posture collapses. Terms 1.3 §14.5 (#922) already pins
  delivery to the account email — keep it that way; the mobile
  email-preferences toggle for the member newsletter (shipped in #922) is
  fine, since a preference switch delivers nothing in-app.
  Blocked behind the web rollout regardless (Stripe live config still
  pending); not scheduled as a phase here.
- **Defer chatbot and ballot facsimile** — cost-guarded / web itself is only
  phase 1. Neither blocks the decision journey.
- **Testing posture: manual-only UI, tested domain logic.** The mobile
  package has no test suite and this plan does not introduce an RN test
  harness. Instead, every piece of reusable logic these phases touch moves
  into `@voteapp/api-client`, where vitest covers it. Accepted risk: mobile
  screen wiring is verified by hand in Expo Go.

## Phase 1 — candidate stance summary (one PR, small, independent) — DONE, PR #925

1. Extract `classifyStanceSummary` from `CandidatePage.tsx:267` into
   `@voteapp/api-client` — it depends only on already-shared helpers — moving
   its tests along; web imports move, behavior unchanged.
2. Render the Supports / Opposes / Mixed box at the top of
   `mobile/src/app/candidates/[candidateId].tsx`. Keep the web rules exactly:
   mixed always shown, no majority collapse, evaluative areas excluded,
   viewer's saved areas ranked first.

Closes the "mobile parity = follow-up" note from web PR #709. First because it
is the smallest end-to-end win and exercises the extract-to-shared workflow.

## Phase 2 — pick core (one PR) — DONE (2026-08-28)

The "add to cart" primitive: pick a candidate, pick Yes/No on a measure, see
and undo the pick.

1. **Design tokens.** Add the reserved pick yellow to
   `mobile/tailwind.config.js`: `pick: "#ffd814"`, `pick-hover: "#f7ca00"`
   (mirror `frontend/src/index.css:56`). Yellow appears nowhere else.
2. **Shared district hook.** Add an account-only `useMyAccountDistricts` to
   `@voteapp/api-client` (the `["me", "districts"]` query, verified-gated,
   from `useMyDistricts.ts:29-34`). The web `useMyDistricts` becomes a thin
   wrapper composing it with the guest draft — do NOT move the whole hook;
   its guest half reads web localStorage by design. Mobile consumes the
   account hook directly (no guest half — guests get no controls, above).
3. **Cache invalidation.** `mobile/src/components/SavedAddressForm.tsx:25`
   invalidates only `["me", "ballot"]` today. Address save AND the
   `pendingDistricts` → `POST /api/me/districts/initialize` handoff must also
   invalidate `["me", "districts"]`, or the gate keeps stale districts after
   a move.
4. **`mobile/src/components/ElectionChoiceControls.tsx`** — RN port of the
   web pair (`CandidatePickButton`, `CandidatePickRow`,
   `MeasureChoiceButtons`), using the shared mutations unchanged. Multi-seat
   races respect `seats_to_fill` and the `picks` array.
5. **Gates, copied exactly from web** (`ElectionPage.tsx:260-278`,
   `CandidatePage.tsx:618-654`):
   - upcoming only (`election_date >= usLatestLocalDate()`);
   - office candidacies only while `status !== "withdrawn" && status !== "lost"`;
   - **stranded-pick removal**: also port `StrandedPicksNotice` /
     `RemoveStrandedPickButton` (web `ElectionChoiceControls.tsx`). A
     withdrawn candidacy is filtered out of the election payload but its
     stored pick still counts toward the seat cap, so without the notice a
     multi-seat race can wedge with every remaining button disabled and
     nothing visible to remove (`chosen: false` on the shared mutation).
     Mobile has no guest draft, so only the signed-in (withdrawn-status)
     branch applies — the roster-absence branch exists for web guests;
   - measures only when the measure details payload exists (a TBD measure
     renders no pick UI of any kind);
   - district gate 3-state rule (docs/plans/pick-district-gate.md): in my
     districts → controls; known-foreign → nothing; unknown → address nudge;
   - **safety valve**: a race with an existing decided choice ALWAYS keeps
     its controls, regardless of district match — an imperfect geocode must
     never lock someone out of changing a pick. Decided races also never get
     the nudge.
6. **Screen layout.** RN has no `position: sticky` and mobile has no existing
   bottom-bar pattern. Do NOT absolutely position an overlay. Restructure the
   two detail screens as root `View` → `ScrollView` + a sibling safe-area-
   aware footer `View`, and give the ScrollView bottom padding so the last
   content clears the footer. Footer contents:
   - candidate screen: the sticky pick card **only when exactly one
     candidacy is pickable** (`CandidatePage.tsx:654` — the card's button
     names no race, so with several races the page relies on the
     self-describing inline "My choice" rows instead);
   - election measure screen: the Yes/No card — the page's ONLY Yes/No
     control (`ElectionPage.tsx:807`), with the post-pick back link.
7. **Picked state on lists.** `ElectionCard` checkmark on decided races
   (`isDecidedChoice`), so the saved-ballot list shows progress at a glance.

Non-goals: no My Draft screen, no auto-pick, no sharing.

## Phase 3 — My Draft screen (one PR)

Web vocabulary, not "My Picks": the surface is "My Draft n/m" until every
race on the nearest election day is decided, then "My Picks ✓"
(`usePickProgress.ts:17-23`).

1. New route `mobile/src/app/my-draft.tsx` reached from the saved-ballot tab
   header (no fifth tab; tab real estate is scarce).
2. **Move the progress logic to `@voteapp/api-client`** (`myDraftLabel`,
   `isDecidedChoice`, and the date-grouping from `useMyPicksProgress`),
   parameterizing the ballot source. Mobile fetches plain
   `/api/me/ballot?sort=state_baseline&followed_first=false` under its own
   query key — **no `include=preview`**: that include exists for the web
   ballot-sheet view (facsimile); mobile has no such view yet.
3. Port from `PicksPage.tsx`: `PickDateCard` (per-election-day card, x/y
   progress), `UpcomingUncardedPicks`, `PastPicks` (results survive the
   ballot's window via the choices payload alone), result chips — all
   optional result fields (`measure_result`, `current_result_*`) render as
   "no result yet" when absent. Include the withdrawn-pick "Remove pick"
   button next to the "(withdrew)" flag on upcoming races (see
   `PickedLine` in `PicksPage.tsx` — date-gated because the backend rejects
   writes to past elections).
4. **Unverified users keep their picks.** The choices API is deliberately not
   verification-gated; only the address-derived ballot is. Mirror
   `PicksPage.tsx:505-524`: unverified renders `VerifyPrompt` (mobile already
   has the component) plus the uncarded upcoming + past pick lists — never a
   blank page.
5. **Share.** Mint via `POST /api/me/pick-card-shares` (move the mutation
   into the shared package), then share the absolute web URL
   `https://<web-origin>/picks/<token>` through the existing native
   `ShareButton`. No native card viewer — the web page + OG image serve link
   opens. **Required disclosure, copied from web** (`PicksPage.tsx:225`): the
   sharer must see "Anyone with the link can see this card and your first
   name." before posting the link.
6. Progress label on the saved-ballot header via the shared hook.

## Phase 4 — issue-editor parity (one PR)

Auto-pick's inputs are direction and hard veto, and mobile's research-areas
editor (`mobile/src/app/settings/research-areas.tsx`) is rank-only today —
mobile-first users would run auto-pick on defaults they never chose.

1. Add Support / Oppose direction toggles and the hard-veto ("line in the
   sand") control to the mobile editor, porting the semantics of
   `frontend/src/components/ResearchAreaPicker.tsx` (including the
   ethics-area special case: no direction control, veto reads
   direction-neutral).
2. No data-loss bug exists today — verified: the backend preference writer
   preserves stored `direction`/`hard_veto` for areas the caller omits and
   defaults new rows to support/false
   (`backend/src/pipeline/users/userResearchAreaPreferences.ts:332-354`).
   This phase is UX parity, not a fix, which is why it can wait until here.

## Phase 5 — auto-pick (one PR)

1. Port `AutoPickControl` (single race, election screen, placed right after
   the roster — `ElectionPage.tsx:796`) and `AutoPickFillControl` (per-date
   fill on the My Draft date cards).
2. **Direct commit, no dry run.** Neither web control uses `dry_run`; the
   run commits (`mode: "replace"` single-race, `mode: "fill_empty"` per
   date) and then explains itself. Do not invent a preview step.
3. Move the fill + clear mutations into the shared package. Clear is the
   date-scoped `DELETE /api/me/auto-picks?election_date=…` so one card's
   clear can't touch another date's auto picks.
4. "Why this pick" panel from `AutoPickCandidateReport` (score, vetoes,
   per-issue net) in the existing mobile `Collapsible`; per-race no-pick
   reasons ride inline on the race rows, as on web after #796.
5. **Tokens: orange, not teal.** Four tokens from `index.css:68`:
   `--color-autopick` #ffa41c, `-dark` #fa8900, `-ink` #0f1111, `-border`
   #c45500 (the border draws the boundary — the fill is only ~2:1 on white).
   Never rausch; never a `bg-auto` token name (collides with Tailwind's
   `background-size: auto`).

## Phase 6 — auth epic (multiple PRs, independent, last)

Not a simple "add Google" PR. Known constraints, to verify against current
Expo/Google/Apple docs before building (per `mobile/AGENTS.md`, read the
versioned docs first):

1. **Development build is likely required.** Expo's current guidance is that
   Google OAuth needs a development build with the provider-native library
   (`@react-native-google-signin`); the old `expo-auth-session` proxy flow is
   not a supported Expo Go path anymore. This project has so far stayed
   Expo Go-compatible on purpose (see the gesture-handler pin) — **moving to
   dev builds is a workflow decision for Shu, not a line item**. Get that
   decision first; it gates the whole phase.
2. **Google + Sign in with Apple together.** App Review guideline 4.8: an app
   offering third-party login must offer a privacy-equivalent option, which
   in practice means Sign in with Apple ships in the same release.
3. **Backend may need no change.** `POST /api/auth/google` verifies the ID
   token. Native Google flows can request the ID token with the *server's
   web client ID* as audience, in which case the existing check passes.
   Observe the real token's `aud` before touching any audience allowlist.
4. **`me.has_password` handling ships first** (small, useful before any
   OAuth): mobile's profile and security screens unconditionally require a
   password today (`settings/profile.tsx:96`, `settings/security.tsx:26`).
   Mirror the web split (`SettingsPage.tsx:455-463`): no password → "Add
   password" section; change-email and delete-account hidden until a
   password exists.
5. Store configuration (OAuth client IDs for iOS/Android, Apple capability,
   provider testing) is its own chunk of work; plan it as such.

## Cross-cutting rules

- **Share domain logic and hooks; keep platform behavior local.** Reused
  data logic, mutations, and pure helpers belong in `packages/api-client`.
  Screens, navigation, storage (localStorage vs AsyncStorage), and layout
  stay per-platform — that's why `useMyDistricts` splits rather than moves.
- **Optional-field tolerance.** All choice/result extras (`measure_origin`,
  `measure_result`, `current_result_*`, `origin`) are optional for deploy
  skew — render sensibly when absent.
- **Verification per PR:** `npm run typecheck` + `npm run lint` in `mobile/`;
  `npx vitest run` in `packages/api-client` (the package has **no npm test
  script**); `npm test` in `frontend/` whenever a web file moves;
  `npx expo install --check` when mobile deps change (per `mobile/README.md`,
  which excludes react/react-dom on purpose); then a manual Expo Go walk of
  the touched flow (pick → undo → measure Yes/No → gate states → progress →
  share URL opens the web card).
- Phases 1 and 6.4 are independent of everything else; 2 → 3 → 5 is a strict
  chain; 4 must land before 5.
- **Terms version on distributed builds.** Mobile bundles `TERMS_VERSION`
  at compile time. No distributed build exists today, so bumps have been
  free — but this plan's later phases (auth epic, store setup) end that.
  Any build that actually ships must carry the then-current version (≥1.3
  after #922), and every future bundle bump must use the
  `GRACE_TERMS_VERSIONS` dual-accept window (`backend/src/constants/legal.ts`)
  so already-installed builds keep working through the rollout.

## Out of scope (tracked, not planned here)

- Guest ballot draft on mobile (revisit on demand).
- Chatbot widget, ballot facsimile phases.
- Membership settings row (web-only signup decision above; blocked on the
  web rollout).
