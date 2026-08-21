# Current-cycle decisiveness for vote power (2026-08)

## Goal

Vote power today has two axes: representation (district size) and decisiveness
(how close the race is likely to be). Decisiveness is derived only from
historic margins (`historical_contest_margins`, weighted last-3 blend). A race
can be a toss-up right now even though it was historically safe — and vice
versa. This plan adds a **current-cycle competitiveness rating per election**,
researched manually, which the decisiveness axis **prefers when fresh and
confident** and falls back to historic data otherwise.

## Scope

Scope = **currently discovered VoteApp elections**, not national coverage.
Local DB counts (verified 2026-08-20): US Senate 25 rows (25 distinct states;
national cycle is 35 seats), US House 436 rows (**435 voting districts + the
DC delegate**), Governor 23 rows (national is 36), Mayor 98 rows (~30
major-city targets). Consequences:

- **Freeze an exact election-ID manifest before each data run** (a `due`
  output snapshot committed to the run's scratch state file). No "all rows
  matching a name pattern" at write time.
- **DC delegate is handled explicitly**: Inside Elections' export contains the
  435 voting districts only. The DC row is excluded from the outlet-consensus
  due list (documented), not silently skipped.
- President is out of scope until outlets rate 2028 (~mid-2027).
- Mayors are **phase-gated behind federal/governor** (see Mayoral section) —
  same feature, later data milestone.

## Research summary (verified 2026-08-20)

- **Outlets actively rating 2026**: Cook, Inside Elections, Sabato (the
  independent human "Big Three"), plus model-based DDHQ, Split Ticket, RCP,
  Fox, VoteHub, Race to the WH, FiftyPlusOne, Economist, Silver Bulletin.
  Wikipedia tabulates them per race with as-of dates.
- **Inside Elections data access**: the ratings pages link generated cache
  JSON exports, e.g.
  `https://insideelections.com/wp-content/themes/inside-elections/cache/ratings_latest_house_year=2026_district=all_clean.json`
  (also `..._senate_...`, `..._governor_...`). Verified live in a real
  browser: top-level `last_updated` and `generated_at`, per-row `date` (last
  rating change), `rating`, `rating_numeric` (0–10), `previous_rating`,
  incumbent, `open`, `special`. **Plain curl/Node fetch gets Cloudflare 403
  even with a browser User-Agent** — research access is browser-tier
  (Claude browser / manual download), never a backend fetch job. An older
  `/wp-json/ratings/v1/...` path also 403s; treat the cache JSON linked from
  the live ratings pages as the current export and re-discover the link from
  the page each run.
- **Sabato**: ratings pages are free; the Center for Politics about page has
  carried a reuse-with-attribution statement, while page footers say
  "All rights reserved." Archive the exact grant URL + a dated copy before
  the first data run; if the grant text cannot be re-verified, treat Sabato
  the same as IE below.
- **Cook**: terms explicitly forbid reproduce/download/**store** and bar both
  robot and manual copying; they sell an API/license for exactly this use.
  **Never fetch or ingest from cookpolitical.com.** Excluded from v1 stored
  data entirely.
- **Legal posture, stated as risk acceptance, not clearance**: we store
  individual factual ratings ("outlet X rated race Y label Z on date D") with
  attribution, per-outlet URLs, and dates; we never reproduce any outlet's
  full table arrangement, prose, or marks. Case law (facts/merger doctrine,
  Barclays v. Theflyonthewall) and the absence of any found enforcement
  history are context that lowers risk — they are not permission, and they do
  not erase contract/trademark risk. Decision: proceed for local/dev;
  **before prod promotion, decide whether to email IE/Sabato for written
  confirmation** (cheap, converts ambiguity into a relationship).
- **Wikipedia** aggregation tables (CC BY-SA): cross-check and discovery
  only — take fact-of-rating values, never their table arrangement.
  **Ballotpedia** (GFDL): facts only, never prose.
- **Mayors**: no outlet rates them (confirmed negative). Handled by a
  deterministic evidence rule, below.

## Design decisions

1. **Fallback rule.** Decisiveness uses the current rating when a row exists
   with `evidence_status='rated'`, `confidence IN ('high','medium')`, the
   election is upcoming (`election_date >= CURRENT_DATE`), and `as_of >=
   CURRENT_DATE - 60`. Otherwise historic margins, exactly as today. Rows
   with `confidence='low'` are stored (provenance) but never override
   history.
2. **Same label enum.** `toss_up | very_competitive | competitive |
   somewhat_competitive | safe` — shared via a neutral export
   `COMPETITIVENESS_LABELS` / `CompetitivenessLabel` in
   `competitivenessLabels.ts`, keeping `HistoricalContestCompetitivenessLabel`
   as a compatibility alias. Drop-in for `decisivenessLevelFromContest`
   (`votePower.ts:127`) — zero scoring-math changes; the uncontested
   short-circuit (`candidateCount === 1` → "none") still runs first, which is
   correct.
3. **Consensus is code-derived, never agent arithmetic.** The manual payload
   carries **raw per-outlet observations**; a pure function
   (`deriveConsensusLabel` in `backend/src/pipeline/competitiveness/`)
   derives label + confidence. Reproducible, unit-tested, no drift.
   - Payload per outlet: `outlet` (`inside_elections | sabato`), `raw_rating`
     (verbatim string), `as_of` (feed date, never in the future), `url`.
     `favored` (`D | R | I | none`) and `intensity` (`toss_up=0 | tilt=2 |
     lean=3 | likely=4 | solid=5`, the distance ladder) are **parsed from
     `raw_rating` in code** (`parseOutletRawRating`) against **each outlet's
     own vocabulary** (IE: Tilt/Lean/Likely/Solid; Sabato: Leans/Likely/Safe;
     both: Toss-up); a payload carrying either field is rejected, and an
     unrecognized or other-outlet rating string is rejected rather than
     guessed at — so a payload can never contradict its own evidence.
   - Function: mean of intensity → bins `<1.0` toss_up, `<2.5`
     very_competitive, `<3.5` competitive, `<4.5` somewhat_competitive,
     `≥4.5` safe.
   - **Guardrails**: (a) outlets favoring *opposite* parties → cap label at
     `very_competitive`, confidence `medium` (fixes the "Safe D + Safe R →
     safe" hole in a naive distance mean); (b) `safe` requires every outlet
     Solid/Safe **and the same favored side**; (c) 2 outlets → confidence
     `high`, 1 outlet → `medium`; (d) 0 outlets → invalid payload.
   - No Wikipedia-absence shortcut for House: the IE export already carries
     all 435 voting districts, so every House row gets direct IE evidence
     (+ Sabato where Sabato rates it; Sabato's House list is competitive
     seats only, so most safe districts are single-outlet `medium` — that is
     honest and acceptable).
4. **Freshness semantics.** `as_of` = the feed-level `last_updated` of the IE
   snapshot used (the outlet actively maintains the whole set as of that
   date), NOT the per-row `date`. Per-row `date` is stored in evidence as
   `changed_at`. Otherwise long-unchanged Solid seats would look stale while
   actively confirmed. For Sabato (no feed timestamp), `as_of` = the page's
   stated update date. `none_found` rows retry from `researched_at` (30
   days), since they have no `as_of`.
5. **Mayoral rule — deterministic, data-gated, later milestone.** A mayor
   race gets a label **only from numeric current-cycle evidence**:
   - a completed first round of the same cycle → the margin (or 50%-clearance
     shortfall) mapped through the existing margin bins (≤2 / ≤5 / ≤10 / ≤15
     / >15), graded against the **decisive round** (recorded per city:
     LA can end at the June primary; San Diego always has a Nov runoff;
     Charlotte's runoff is 30%+1 on request; a safe-D city's primary is not
     decisive when a loser holds an independent line); or
   - ≥2 independent public polls within 60 days, each verified on the
     pollster's own site (a fabricated LA poll circulated this cycle) →
     average margin through the same bins.
   - Structural signals (incumbency, open seat, field size, challenger
     funding, endorsements) **adjust confidence only, never the label**.
     No numeric evidence → `none_found`. This removes researcher judgment
     from the label; two researchers produce the same grade.
6. **Sourcing: tiered seed list + class rules, no whitelist.** Federal/Gov
   tier 1 = IE export + Sabato pages, Wikipedia tables for cross-check;
   tier 2 = news reports of rating changes. Mayor tier 1 = Ballotpedia race
   page + city finance portal + prior decisive-round result; tier 2 = named
   local outlets + vetted pollster list (skill doc); tier 3 = open browser
   search under class rules (named outlet with editorial standards,
   pollster's own site, official filings; existing blocklist classes stay
   banned; anonymous blogs/content farms/market screenshots never sole
   evidence). Validator enforces blocklist + URL shape on **every nested
   evidence URL**, and for `outlet_consensus` checks outlet ↔ hostname
   (inside_elections → insideelections.com, sabato → centerforpolitics.org;
   wikipedia.org allowed as a cross-check URL slot).
7. **No-evidence attempts recorded in the same table** (`label NULL`,
   `evidence_status='none_found'`) so the due list skips a race for 30 days.
   No separate ledger.
8. **One row per election, no history.** Upsert; the writer **refuses an
   upsert whose `as_of` is older than the stored row's** (and a `none_found`
   overwrite of a stored rating) unless `--force`. The refusal lives in the
   upsert's `DO UPDATE ... WHERE` guard, so it is atomic and holds under
   concurrent writers. Evidence jsonb keeps raw strings so labels can be
   re-derived.
9. **No automation in v1.** No scheduler, no IE polling job (403s
   server-side anyway). Future work only, and it would need stable
   permissioned access first.

## Schema (Phase 1 — migration `248_add_current_race_ratings.sql`)

```sql
CREATE TABLE IF NOT EXISTS public.current_race_ratings (
  election_id uuid PRIMARY KEY REFERENCES public.elections(id) ON DELETE CASCADE,
  schema_version text NOT NULL,
  competitiveness_label text,        -- NULL iff none_found
  method text NOT NULL,              -- 'outlet_consensus' | 'mayoral_rubric'
  confidence text,                   -- NULL iff none_found
  evidence_status text NOT NULL,     -- 'rated' | 'none_found'
  as_of date,                        -- NULL iff none_found
  decisive_round text,               -- mayors only
  evidence jsonb NOT NULL,
  source_url text NOT NULL,
  researched_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT current_race_ratings_schema_check
    CHECK (schema_version = 'current_race_rating.v1'),
  CONSTRAINT current_race_ratings_status_check
    CHECK (evidence_status IN ('rated','none_found')),
  CONSTRAINT current_race_ratings_label_check
    CHECK (competitiveness_label IS NULL OR competitiveness_label IN
      ('toss_up','very_competitive','competitive','somewhat_competitive','safe')),
  CONSTRAINT current_race_ratings_rated_fields_check
    CHECK (
      (evidence_status = 'rated'
        AND competitiveness_label IS NOT NULL
        AND confidence IS NOT NULL
        AND as_of IS NOT NULL)
      OR
      (evidence_status = 'none_found'
        AND competitiveness_label IS NULL
        AND confidence IS NULL
        AND as_of IS NULL)
    ),
  CONSTRAINT current_race_ratings_method_check
    CHECK (method IN ('outlet_consensus','mayoral_rubric')),
  CONSTRAINT current_race_ratings_confidence_check
    CHECK (confidence IS NULL OR confidence IN ('high','medium','low')),
  CONSTRAINT current_race_ratings_source_url_check
    CHECK (btrim(source_url) <> ''),
  CONSTRAINT current_race_ratings_evidence_check
    CHECK (jsonb_typeof(evidence) = 'object')
);
```

No `as_of` index (≈500 rows; PK lookups dominate). `set_updated_at` trigger
like migration 107. Identifiers ≤63 chars. Election-UUID keying matches the
manual-payload pattern (`--election-id` + in-payload `election_id`,
cross-checked against a loaded context, exactly like
`manualElectionResults.ts`).

## Read-path wiring (Phase 3)

- `backend/src/pipeline/address/currentRaceRatingLookup.ts` (new): one
  batched query by election-id list applying the full override rule
  (rated + high|medium + upcoming + as_of window) in SQL. Sibling of
  `loadHistoricalCompetitivenessByElection` (`ballotLookup.ts:839`).
- `ballotLookup.ts` list (`:1741`) and detail (`:2043`):
  `competitivenessLabel: currentRating?.competitiveness_label ??
  historicalCompetitiveness?.competitiveness_label`. When current is used,
  the explanation context carries the current-rating info and **not** the
  historic `marginPercent`/`marginContests`/`staleAfterRedistricting`.
- `votePower.ts` (explanation only; scoring untouched):
  - `VotePowerExplanationContext` gains `currentRating?: { asOf: string;
    method: 'outlet_consensus'|'mayoral_rubric'; confidence:
    'high'|'medium'; outlets: { outlet: string; rawRating: string;
    intensity: number }[] } | null`.
  - `decisivenessPart` branches: `stat` like `rated toss-up as of Aug 6,
    2026`; `detail` names the sources; `formula` shows the real derivation,
    e.g. `IE "Tilt Democrat" (d=2) + Sabato "Leans Democratic" (d=3) → mean
    2.5 → "competitive" → grade average` (mirrors the pure function; no
    `MARGIN_GRADE_SCALE`, no redistricting suffix).
  - `HOW_CALCULATED` becomes source-dependent ("based on current race
    ratings" vs "based on past results").
  - Current-rating `confidence` feeds the explanation caveat when `medium`
    ("single-source rating"), without touching `confidenceFor` axis logic.
- **Current source must be visible (not deferred).** Verified: both web
  (`ElectionPage.tsx:602` area) and mobile
  (`mobile/src/app/elections/[electionId].tsx:194`) render the
  `historical_competitiveness` chip next to the vote-power label, so a
  current toss-up over a historically-safe race would show a visible
  contradiction. Fix in PR 3: detail/summary payloads gain a small
  `current_competitiveness` object (`display_label`, `display_description`,
  `as_of`, `method`); web + mobile render it **instead of** the historic chip
  whenever the current rating drove decisiveness (historic chip stays for
  fallback races). api-client `types.ts` gains the optional object
  (optional-field convention, old backends keep working).
- Side effect, intended: vote-power score drives ordering
  (`ballotElectionOrdering.ts`, api-client `railSort`) — a toss-up upgrade
  reorders ballots with no further code. Covered by an ordering test.

## Import path (Phase 2)

Clone of the election-results pattern:

- Contract `backend/src/contracts/currentRaceRatingPayloadContract.ts`:
  `{ "ratings": [ { "election_id", "method", "evidence_status",
  "observations": [ { "outlet", "raw_rating", "as_of", "url" } ],
                    // outlet_consensus, evidence_status=rated;
                    // favored+intensity are parsed from raw_rating and
                    // stored in evidence, never accepted as inputs
  "numeric_evidence": { ... },        // mayoral_rubric, evidence_status=rated
  "decisive_round"?, "source_url" } ] }` — the contract calls
  `deriveConsensusLabel` (or the mayoral bin function) itself; the label and
  confidence are **outputs**, never payload inputs. Rejects unknown/duplicate
  election ids, blocklisted or malformed URLs anywhere in evidence, outlet ↔
  hostname mismatches, DC-delegate election id in outlet_consensus mode.
- Context loader + validator under `backend/src/pipeline/competitiveness/`;
  URL liveness checking reuses the election-results approach
  (`electionResultSourceValidation.ts`), where a 403 from a known-good host
  is acceptable-with-note (matches existing validator behavior — IE 403s
  non-browser clients).
- Writer `currentRaceRatingWriter.ts`: upsert `ON CONFLICT (election_id) DO
  UPDATE`, refusing older `as_of` without `--force`.
- Script `backend/src/scripts/manualCurrentRaceRatings.ts`
  (`due | context | write`, npm scripts `manual:current-ratings:*`):
  - `due` — from a frozen manifest file (`--manifest path`) or the scope
    query keyed on `offices.canonical_name` ('United States Senator' /
    'Governor'; House via `district_type='us_house'`), since ballot titles
    vary by state ("US Senate", "Governor / Lt. Governor") and the discovery
    contest family is not always set (MI's Senate row). Fallbacks for
    unresolved offices: the contest family for Senate, a statewide title
    list for Governor. `general`+`special` stages. A `--mayors` scope ships
    with the v1.1 mayoral milestone and will require an explicit city-list
    manifest, never all 98 rows. Excludes rows with fresh ratings (60 d from
    `as_of`) or recent `none_found` (30 d from `researched_at`); the DC
    delegate row is listed as excluded with a reason.
  - `context` — election + office + district + roster + historic label,
    capped at 10.
  - `write` — validate + derive + upsert in a transaction; `--dry-run`,
    `--force`; `requireLocalDatabaseTarget`; `assertKnownCliFlags`.

## Skill reference doc (Phase 2)

`references/current-race-ratings.md` + entries in `commands.md` /
`pipeline-map.md`:

- Browser-tier retrieval recipe: open the IE ratings page, follow its export
  link (re-discover each run — the cache filename is generated), save JSON;
  Sabato page URLs; Wikipedia cross-check URLs. Note: backend/curl cannot
  fetch IE (Cloudflare) — browser only.
- Payload-building worksheet: outlet string → `favored` + `intensity` table
  (incl. 7-point outlets having no tilt), two worked examples, DC-delegate
  exclusion, `none_found` instruction.
- Mayoral numeric-evidence rule + per-city decisive-round table +
  poll-verification rule (pollster's own site).
- Attribution requirements (outlet, as-of, URL — all three, every
  observation). Hard rules: never fetch cookpolitical.com; never copy
  Ballotpedia/Wikipedia prose; no AI provider calls.

## Phases

**PR 1 — storage + derivation core.** Migration 248; `CompetitivenessLabel`
neutral export + alias; `deriveConsensusLabel` + mayoral bin function (pure);
payload contract; writer (older-`as_of` refusal); freshness lookup. Tests:
opposite-favored cap, Safe-guardrail (same side required), 1-vs-2-outlet
confidence, label/status CHECK combinations, snapshot-vs-changed-at dates,
`none_found` retry window, older-upsert refusal, 60-day boundary (fresh at
60, stale at 61), past-election exclusion. No behavior change.

**PR 2 — import path.** Context loader, source validation (nested URLs,
outlet↔hostname), CLI + npm scripts, skill reference doc + index entries.
Test: `due` counts against the frozen manifest; `--dry-run` on a hand-built
two-election payload; one real Senate write verified.

**PR 3 — read path + visibility.** ballotLookup preference + explanation
context; votePower source-aware copy incl. derivation formula and
medium-confidence caveat; `current_competitiveness` payload object;
api-client type; web + mobile chip swap (current replaces historic when it
drove the rating). Tests: votePower explanation per source/method;
list/detail parity; uncontested precedence over a current rating; low
confidence falls back; ordering shift; DC row untouched. Browser-verify the
detail panel on a seeded race.

**PR 4 — data run (no code).** Pilot 2 races end-to-end → Senate 25 + Gov 23
→ House 435 voting (IE export + Sabato where rated; audit every non-safe
label + a 20-row random safe sample against the outlet pages) → DC decision
recorded (`none_found` or exclusion note). Manifest + checkpoint state in a
scratch file per token-discipline rules.

**Later milestone (v1.1): mayors.** Deterministic numeric rule above, city
manifest ~30, only after the federal/gov run is audited. **Re-run the whole
data set the last week of October 2026** (ratings move fast late-cycle).

## Future (explicitly not now)

- Automated IE sync (needs stable permissioned access; server-side fetch
  currently 403s).
- Cook as an input (licensing email) · presidential (mid-2027) · rating
  history table · prod promotion (standard checklist + written-permission
  decision + new table in the sync set).

## Gotchas

- Migration numbering: 248 next; never renumber.
- IE `rating_numeric` is 0–10 with 4/6 unused — do not "fix" the gaps; our
  payload stores `intensity` (distance ladder), not IE's raw numeric.
- Sabato House coverage is competitive-seats-only → most safe House rows are
  single-outlet `medium` confidence; that is expected, not a data bug.
- Do not derive a mayor's election year from Ballotpedia's "term ends"
  column.
- The IE export filename is a generated cache path — always re-discover it
  from the live ratings page rather than hardcoding.
