# Ballot facsimile plan — My Picks as a paper-ballot preview (v2)

## Goal

Turn the My Picks date card into a rendering that looks and reads like the voter's
paper ballot: the races VoteApp associates with their saved districts, in ballot
order, with official titles, ballot-style visuals (contest boxes, instruction line,
fill-in ovals), and the user's picks pre-filled — so a voter can hold the phone (or a
printout) next to the paper ballot and copy marks across without hunting for races.

Ballot look varies by state and county (layout, oval side, columns, contest order),
so the render is **regionalized**: users in different counties should see facsimiles
that each match their own ballot as closely as our data allows — and every claim of
closeness is **earned per element** (order, wording, layout) and labeled with its
source and cycle. Where we don't know, we say so; we never imply fidelity we don't
have.

Explicitly NOT the goal: pixel-perfect vendor typography, or anything mistakable for
an official ballot (see Guardrails).

## Research summary (2026-08, verified online)

- **No dependable nationwide public source of physical ballot layouts.** Layout is
  produced per county by voting-system vendors (ES&S, Dominion, Hart). The
  machine-readable standard ([NIST SP 1500-20 Ballot Definition CDF](https://nvlpubs.nist.gov/nistpubs/SpecialPublications/NIST.SP.1500-20.pdf),
  2023) is an interchange spec, not a public dataset.
- **Contest order baselines come from state statute** (e.g. Maine 21-A §601,
  NC §163-165.6, MN 204D.13), but several states delegate detail to the secretary of
  state or election-board rules — so statute-derived order is a **baseline** to be
  validated against current official sample ballots, not gospel.
- **[Google Civic `voterInfoQuery`](https://developers.google.com/civic-information/docs/v2/elections/voterInfoQuery)**
  (free, live in 2026) returns per address: `contests[].ballotPlacement`,
  `candidates[].orderOnBallot`, `ballotTitle`, `numberVotingFor`, referendum text and
  response labels. Fed by the [Voting Information Project](https://www.votinginfoproject.org/)
  (40+ state partnerships — partnerships, not guaranteed local coverage; fields are
  optional, address-specific, and appear only close to the election).
- **BallotReady GraphQL** (paid) has `Position.rowOrder`, `tier`, and — notably —
  distinguishes `seats` from `selectionsAllowed`. We skip paid sources but adopt that
  distinction (see seats vs. max selections below).
- **Official sample ballots** are the ground truth for order + layout. Access varies
  by county: PDFs by precinct/style, voter-search portals (NC serves a per-voter
  exact facsimile), HTML, or mailed guides. Prior-cycle samples are usable as layout
  *inspiration* with an explicit cycle label — never as verification of the current
  ballot.
- **Candidate order within a race**: several states rotate names per precinct or
  assembly district (CA randomized alphabet), others use random draws or alphabetical
  order. Exact rotation needs precinct knowledge we deliberately don't hold →
  rotation states get a visible disclaimer, never a guess.
- **In-booth phone use is NOT universally allowed** (e.g. Texas Elec. Code §61.014
  bans wireless devices within 100 ft of voting stations; NC allows reference use but
  bans photography/communication). The product must offer print/download and say
  "polling-place device rules vary." Mail/at-home voters — a large share of ballots —
  are unaffected.

## Current data model (repo facts, verified)

- User geography = **district ids only** (`user_districts`); no address/precinct/ward
  stored, matched address transient by design
  (`backend/src/pipeline/users/userAddressDistrictUpdater.ts`).
- **Ward-seat superset**: a county/place district row carries every seat attached to
  it, so ward-level races reach every resident; `sub_district_seat` flags the smaller
  electorate but does not filter (`backend/src/pipeline/elections/subDistrictSeat.ts:1-11`,
  `packages/api-client/src/types.ts:117-124`). The preview may therefore include
  races that are not on the reader's paper ballot — the UI must say so.
- `GET /api/me/ballot` summary (`ballotLookup.ts:1425`): the SQL already selects
  `e.seats_to_fill` (`ballotLookup.ts:1468`) — it is simply not projected into the
  payload/type. Candidate roster is not in the summary; the per-election query
  (`ballotLookup.ts:1063`) returns name/party/incumbent/status **plus running-mate
  fields**, alphabetical, and **hides withdrawn candidacies** (product decision,
  `ballotLookup.ts:1092-1094`) — though a late withdrawal may still be printed on
  paper.
- Measures (`ballotLookup.ts:1102-1117`): `official_ballot_title`, `summary`,
  `what_yes_means`, `what_no_means`, `official_measure_url`. **No full printed
  question text, no official response labels.** Our summary text must always be
  labeled as VoteApp's explanation, never rendered as ballot wording.
- Choice model (`db/migrations/203_add_user_election_choices.sql`): unranked
  candidate picks (capped at `COALESCE(seats_to_fill, 1)`) + yes/no measure position.
  **Cannot represent** ranked-choice preferences, retention responses, or
  For/Against-style labels → those races fall back to list view inside the preview.
- No ballot-placement column anywhere; per-state config slots exist
  (`state_resources` table; FIPS-keyed registries in `backend/src/constants/`).
- Frontend tokens: Tailwind v4 CSS vars in `frontend/src/index.css`.

## Guardrails (non-negotiable)

1. **Never present the render as an official ballot.** Header copy:
   "Ballot preview — not an official ballot. Based on your saved districts; it may
   include nearby district races that aren't on your ballot, or miss local ones.
   Compare with your official sample ballot." No seals, no "OFFICIAL BALLOT" caption,
   no signature boxes.
2. **Fidelity is earned per element and labeled.** Race order, wording, layout each
   carry their own provenance: "order: your state's ballot rules (approximate)" /
   "order & layout: your county's 2026 sample ballot" / "layout: based on your
   county's 2024 ballot". Never upgrade fidelity from vendor name or county alone —
   only from an actual ballot-style source with a cycle date.
3. **No new PII.** Everything derives from district ids; no user addresses stored or
   sent to third parties.
4. **Don't promise in-booth phone use.** Copy: "Polling-place device rules vary —
   print this or check local rules." Print/download is a first-class action.
5. **Graceful fallback**: ranked-choice, retention, fusion/multi-line states,
   unresolved sub-district seats, and unsupported response types render as the
   plain list row inside the preview, not a wrong facsimile.
6. Free read-side feature → flags default ON per policy; nothing costs money.

## Phase 1 — Ballot-style preview + small API adds

The copy-across win with no new tables.

0. **Two surfaces, one card.** `PickDateCard` (exported from `PicksPage.tsx`) is now
   shared by signed-in `/me/picks` and the guest `/draft` page
   (`frontend/src/pages/DraftPage.tsx`), which renders the same cards from the
   localStorage ballot draft over the public `GET /api/ballot?district_ids=…`
   endpoint. The List / Ballot view toggle is PAGE-level state on both pages
   (shipped as `BallotViewToggle`; a per-card toggle would fragment the fetch and
   the reading flow), so guests get the ballot view too — the draft-on-display is
   already the signup pitch, and a ballot-shaped draft is a stronger one. Guest
   specifics:
   upcoming-only dates (already the page's rule), and the draft's "other saved
   picks" (races outside the stored ballot) stay plain list rows — they have no
   ballot context to render into.
1. **Backend: `include=preview` on BOTH ballot endpoints** — `GET /api/me/ballot`
   and public `GET /api/ballot` share `lookupBallotSummariesByDistrictIds`, so the
   opt-in lands once in the shared reader (default payloads unchanged):
   - Project `seats_to_fill` into the summary (already selected in SQL). Render
     "Vote for up to N" from it, with an honest asterisk: seats ≠ max selections in
     limited-voting jurisdictions. A nullable `elections.max_selections` override
     column ships in Phase 3 for curated exceptions; until curated, `seats_to_fill`
     is the instruction and single-seat races say "Vote for One" (NULL→1 is already
     the app-wide convention).
   - Attach candidate rows per election reusing the existing roster query shape:
     `candidate_election_id`, `display_name`, `party`, `is_incumbent`, `status`,
     running-mate fields. **Include withdrawn candidacies in this payload** (status
     carried) — the paper ballot may still print them; the preview renders them
     struck-through "(withdrew — votes may not count)", which matches paper better
     than hiding. Election-page behavior unchanged.
   - Attach measure render fields: measure title + what-yes/what-no, flagged as
     VoteApp's explanation (`summary_is_voteapp: true` — or simply fixed UI labeling).
2. **Backend: per-state contest-order registry** —
   `backend/src/constants/stateBallotOrderByFips.ts` (matches existing FIPS-registry
   pattern). Input: `office.scope` + `district_type` + `race_type` + per-state
   special cases. Output: integer rank, exposed as sort mode `state_baseline` in
   `ballotElectionOrdering.ts`. Start with states that have real Nov-2026 usage;
   generic federal→state→county→local→measures rank elsewhere. **Validation: compare
   ranks against current official sample ballots for covered states, not only unit
   fixtures** (statutes delegate detail to SoS/board rules in some states). UI label:
   "Approximate order based on your state's ballot rules."
3. **Frontend: `BallotPreview` component** (user-facing name "Ballot view";
   page-level List / Ballot view toggle on both `/me/picks` and `/draft`).
   Note the guest data path: `/draft` reuses BallotPage's cached
   `sort=vote_power` query; the ballot view fetches its `include=preview` +
   `state_baseline` payload lazily on first toggle rather than widening the shared
   default query. Component details:
   - Paper-ballot archetype: off-white sheet, ruled contest boxes, bold uppercase
     contest header (`official_ballot_title` + `sub_district_seat` badge),
     instruction line per above, candidate rows as **fill-oval + name + party
     (+ running mate)**, user's pick = filled oval + textual "Your pick" (never color
     alone), undecided races = empty ovals + "no pick yet" chip.
   - Measures: official title, "YES ○ / NO ○" with pick filled, VoteApp explanation
     visually set apart and labeled — never styled as ballot text.
   - **No synthetic write-in row** (write-in rules vary; add per-state later only if
     sourced).
   - Ward-seat races (`sub_district_seat` present) get an inline "may not be on your
     ballot — seat covers {Ward 3}" note.
   - Unsupported race shapes (see Guardrail 5) render as list rows.
   - Print CSS + print/download action; dark mode maps sheet to dark surface.
   - Link to the official sample-ballot page for the user's state where we have it
     (`state_resources` / how-to-vote surface) — cheap and high value.
   - Disclaimer footer per Guardrail 1.
4. **Tests**: rank-fn unit tests per state, component tests (picked / undecided /
   multi-seat / withdrawn / measure / ward-note / fallback shapes), typecheck + test
   in both packages.

## Phase 2 — Regional layout theming (per-state/per-county, source-verified)

Regional visual matching is the product goal; the discipline is that every profile
comes from an actual ballot-style source.

1. **Template archetypes** (closed set, CSS variants): `oval-left` (most common),
   `oval-right`, `box-left`. (`party-grid` deliberately dropped: fusion/multi-line
   ballots can't be modeled with one party string per candidate — NY-style ballots
   use the generic archetype + fidelity note.)
   Options: column count, shaded headers, party abbreviation vs full name,
   instruction phrasing.
2. **DB: `ballot_style_profiles`** (next free migration number at implementation
   time): `state_fips`, `county_geoid NULL` (NULL = state default), `template`,
   `options jsonb`, `source_url`, `source_cycle`, `verified_at`. Resolution: county →
   state → national default. A profile row REQUIRES a source; `source_cycle` drives
   the fidelity label ("layout based on your county's {2024} ballot").
3. **Curation via `voteapp-manual-research`**: new reference doc + payload contract;
   researched from official sample ballots (current cycle when posted; prior cycle
   allowed as layout inspiration with the cycle label). Seed states with Nov-2026
   user coverage first.
4. Profile selection via the user's district set (state + county district ids already
   in the payload).

## Phase 3 — Curated exact order + wording (per ballot-style scope)

Exactness earned field-by-field from current official ballot styles.

1. **DB**: `election_ballot_placements` (`election_id`, `placement`, `source`,
   `source_url`, `ballot_style_scope`, `verified_at`) overriding the state baseline;
   `candidate_elections.ballot_order int NULL`; `elections.max_selections int NULL`
   (the seats-vs-selections fix); measure additions: `official_question_text`,
   `response_labels` (e.g. For/Against), `source_cycle`.
   - **Scope rule**: placements/order are stored against the ballot-style scope they
     were verified for (county profile), not asserted globally — a statewide race's
     placement can differ across counties, and rotation states never get
     `ballot_order` from a rotating sample.
2. Populated in the same manual-research sitting as Phase 2 profiles (one county
   sample ballot yields layout + order + wording together).
3. Fidelity label upgrades to "matches your county's {2026} sample ballot" only when
   current-cycle-verified.

## Phase 4 — Civic API assist (optional, flag-gated, free, scoped)

Server-side pipeline, never from user requests and never with user addresses: query
`voterInfoQuery` with representative public addresses (e.g. county-seat civic
buildings), and write placements **only for contests actually matched in that
response**, tagged `source='google_civic'` + the queried style's scope — unmatched
local contests keep the state baseline (one address = one ballot style; we never
generalize a style's local contests county-wide). Never write `orderOnBallot` in
rotation states. Runs in the pre-election window when VIP data exists; manual
curation outranks it. Flag `BALLOT_ORDER_CIVIC_SYNC`, ships dark.

## Out of scope / later

- Per-precinct candidate rotation (needs precinct data we deliberately don't hold).
- Fusion/multi-line rendering (needs per-candidacy ballot-line model).
- Ranked-choice mark rendering (needs ranked picks in the choice model first).
- Vendor typography, seals, stub artwork.
- Share-card ballot view (`PublicPickCardPage`) — revisit after Phase 1. (The public
  `/api/ballot` endpoint itself IS in scope — the guest /draft page depends on it.)
- Mobile (Expo) parity — port after web settles.
- Usability test of the preview against real ballots (EAC ballot-design guidance)
  before any "matches your ballot" marketing claim.

## Risks

- **Superset/missing races** (ward seats; uncatalogued local districts): Guardrail 1
  copy + per-race ward notes; the official sample-ballot link is the user's
  completeness check.
- **Instruction wrong in limited-voting jurisdictions**: `max_selections` override
  (Phase 3); until then the instruction comes from `seats_to_fill`, which is correct
  for the overwhelming majority and matches the app's existing pick cap.
- **Statute rank wrong in delegated-rule states**: validate against current sample
  ballots; county overrides are the correction path.
- **Election-integrity optics**: Guardrail 1 wording reviewed before launch.
