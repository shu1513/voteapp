# Rhode Island campaign finance — implementation plan

Date: 2026-08-12, revised same day after a second-opinion review (valid items
folded in below; verified against the live portal and the repo before
acceptance — the review's pagination and migration-number findings were
confirmed real, its PR-reorder proposal was rejected, see "PR sequence").
Governs the Rhode Island state-finance build. Read alongside
`plan.md` ("Pause point — add new states") and
`docs/finance-module-capability-matrix.md` — their rules apply to every PR
here. Feasibility research: external Rhode Island report dated 2026-08-03,
plus live portal verification on 2026-08-12 (results below). Do not re-hit the
portal during code-only PRs (portal work happens only in the acquisition
spike, when authorized).

## Verdict

Rhode Island is born on the shared factories with **zero migration debt** and
**zero new factory capability**. Canonical identity (`committee_id` = ERTS
organization key, a stable numeric Board key — e.g. McKee is `2235`;
`committee_name`), all 5 standard tables, standard summary columns. Direct
breakdown categories are **`contribution_size` only** (decision 1 — Rhode
Island discloses employer, not occupation, and the standard layer has no
employer path). Outside spending ships via **curated supplements** (decision
5 — CF-8s are scanned PDFs, volume is tiny, no OCR exists in the repo).
Reference siblings: **maryland/georgia** for the wrappers (diff against them
before writing each file), **north carolina** for the acquisition shape
(stateful ASP.NET portal, many small per-committee fetches), **houston** for
the WebForms session mechanics, **san jose** for the paper-filing supplement
contract.

Scope v1: current-cycle (2026) statewide + General Assembly candidates,
official totals + cash on hand + contribution-size buckets + single-target
CF-8 independent expenditures + outside-group donor rows from the same
transcribed CF-8s. Municipal candidates, historical cycles, and any direct
employer/industry display are out of v1 (decisions 1, 9).

## Live portal verification (2026-08-12)

Verified directly against `ricampaignfinance.com` (ERTS, ASP.NET WebForms,
`v 20201012.1`):

- **Reconciliation fixture**: Daniel J. McKee (org key 2235), Q2 2026
  (04/01–06/30): contribution search totals Individual $241,264.29 +
  PAC $12,450.00 + Interest $5,116.77 + In-Kind Individual $3,508.00 +
  Other $113.95; expenditure search total $945,434.57. Cash receipts
  ($258,945.01, excluding in-kind) and expenditures reconcile cent-exact to
  the CF-2 summary: $1,355,115.78 beginning + $258,945.01 = $1,614,060.79
  total available; $1,614,060.79 − $945,434.57 = $668,626.22 ending.
- **Contribution result columns**: Donor, Employer, Type, Amount, Receipt
  Date, Deposit Date, detail link (transaction ID). Expenditure columns:
  Payee Name, Disbursement Type, Cash Amount, Expenditure Date, Payment Date,
  detail link. Both offer "Export Detail to comma delimited file".
- **No occupation field anywhere** — search filter and results expose
  Employer only, matching R.I. Gen. Laws § 17-25-11 (place of employment).
- **Search constraint**: contribution/expenditure search rejects date-only
  queries — "You must specify at least an Organization, Donor Name or
  Employer Name as a Search criteria." There is no statewide date-window
  dump; acquisition must be per-organization.
- **Contribution type vocabulary** (full list from the search form):
  Individual, Aggregate-{Individual,PAC,Party}, PAC, Party, Loan Proceeds
  {,-PAC,-Party}, In-Kind-{Individual,Party,PAC,Aggregate}, Interest
  Received, Refund/Rebate, **State Check Off**, Party Building-{Individual,
  PAC,Party}, Other Receipt, **Matching Public Funds**. The last two were
  absent from the feasibility report; Rhode Island has public matching funds
  for general-office candidates (decision 2).
- **Other Filings grid is paginated** (`RIPublic/Homepage.aspx`,
  `dgdCF8FilingList`): 10 rows per page, pager shows pages 1–10 plus a
  "..." continuation — the grid spans multiple years, not one screen (an
  earlier read of this plan claimed "10 filings, no pagination"; that was a
  bad test and is corrected here). Pages 1–2 verified live 2026-08-12; the
  2025-01-01..2026-12-31 cycle contains **4 INDEPENDENT EXPENDITURE
  filings**: Stop The Wait RI (Jul 3 2025), Collective Action for Education
  (Jul 3 2025 and Feb 9 2026), UNITE HERE LOCAL 26 RI IEPAC (Jul 30 2026) —
  plus COVERED TRANSFER, BALLOT QUESTION ADVOCACY, and **REFERENDUM** rows.
  The 2026 IEs appear to target Providence municipal races (Smiley/Morales),
  so after the election-year + office gates the v1 eligible outside row
  count may currently be **zero** — verify against the VoteApp roster at
  transcription time, and report inventory stats (pages traversed, rows
  scanned, cycle rows, eligible rows, excluded-by-gate rows) in every
  refresh. All filings are scanned PDFs under `/ReportsScanned/`, filing
  type embedded in the file name, GUID-stable URLs. Cycle volume (~4 IEs)
  is what kills the OCR pipeline (decision 5).

## Shared-piece config (settled)

- **Writer** — `createStandardStateFinanceSnapshotWriter` wrapper, maryland
  pattern:
  - `label: "Rhode Island"`, `minElectionYear: 2026` (georgia precedent —
    current cycle only; historical expansion is a later, separately tested
    decision given the per-organization crawl cost)
  - `summaryUpdatePolicy`: replace all summary fields, preserve-when-null the
    two outside totals (direct and outside are built from different artifact
    sets; a direct-only refresh must not wipe outside totals)
  - `outsideGroupValidation: "pairing"` — mandatory, cascade-FK trap
  - `supersededLinkSource: "erts_portal"` (also the link-source CHECK value
    next to `'manual'`)
  - `normalizeCommitteeId`: none — ERTS org keys are numeric; strict numeric
    validation + trim in the wrapper mapping
  - Pool-guard wrapper stub (maryland `requireMarylandPool` pattern)
  - **`allowNegativeCashOnHand: true` from day one**, with the matching
    relaxed `amounts_check` in the migration (flows stay `>= 0`; only
    `cash_on_hand` is signed). Georgia and Massachusetts both shipped the
    canonical nonnegative CHECK and then needed relaxation migrations
    (231/232) when real filings carried negative balances; RI CF-2s carry
    liabilities (McKee: $21,922.88), so a negative ending balance is
    plausible and an official value must never be written as NULL.
- **Due-list** — `createStandardStateFinanceDueListQuery`, canonical config,
  no `linkColumns`, no `mapRow`.
- **Loader** — shared-loader wrapper (~40 lines, maryland shape):
  `state: "RI"`, `source: "RHODE_ISLAND_ERTS"`,
  `directBreakdownCategoryTypes: ["contribution_size"]` (vermont precedent),
  `evidenceLabelTypes: ["donor"]`, `isEligibleElection`, and both coverage
  notes (decisions 1 and 5). Characterization test via
  `tests/helpers/stateFinanceLoaderCharacterization.ts` written up front.
- **Auto-link** — copy maryland/san jose loop; shared name-safety vetoes
  (`personNameMiddleEvidence`, `personFirstNameNicknames`) apply.

## Schema (allocate the next free migration number immediately before the PR)

Do not pre-claim a number in this doc: 234 is already the chatbot schema and
235 is claimed by the open San Diego PR — check `db/migrations/` and open PRs
at PR time.

`db/migrations/<next>_add_rhode_island_campaign_finance_tables.sql` — clone
migration 212 (north carolina), `ri_` prefix, short constraint prefix
`ri_cff_` on the outside-group-breakdowns table (maryland `md_cff_` trick).
Identifiers ≤63 chars. Link-source CHECK lists `'manual'` + `'erts_portal'`.
Direct-breakdown `category_type` CHECK stays canonical
(`('occupation','contribution_size')`) even though v1 writes only
`contribution_size` — no schema drift for a data-absence reason. Summary
`amounts_check` allows signed `cash_on_hand` (georgia 231 wording), all other
money columns `IS NULL OR >= 0`.

## Settled design decisions

1. **No occupations — and no employer relabeling.** § 17-25-11 and Form CF-3
   require the donor's employer, never occupation. The standard layer's
   direct path is `occupation` + `contribution_size` end-to-end (writer enum,
   table CHECK, loader whitelist, both UI cards; `top_employers` is always
   empty). Rhode Island therefore ships **contribution-size buckets only** on
   the direct side, with `direct_coverage_note`: "Rhode Island discloses a
   direct contributor's employer, not occupation, so donor-occupation
   breakdowns are not available for this state; size buckets reflect
   itemized contributions only." (The second clause matters: `Aggregate - *`
   rows are lawful for donors at or under $200/year and are in the direct
   total but not the buckets — decision 13 — so buckets never reconcile to
   the total and the note must say why.) Employer strings are
   **never** written as `occupation` rows. A direct employer/industry chart
   would require a cross-state standard-layer extension (new CHECK, loader
   routing, UI section) — out of scope here; if ever wanted, it is its own
   plan, not a Rhode Island rider.
2. **Cycle totals are per-period CF-2 sums, not "the latest CF-2".** CF-2
   summaries are reporting-period documents — the latest one describes its
   period, not the cycle. Publication rule:
   - select the authoritative amendment per reporting period (decision 4
     selector), assert periods are non-overlapping — an overlap quarantines
     the org, never double-counts;
   - `total_receipts` = sum of authoritative CF-2 cash receipts across the
     cycle's periods; `total_disbursements` = same for disbursements;
   - `cash_on_hand` = the latest authoritative CF-2 ending balance, with
     the period-end as its as-of evidence;
   - reconcile exports to CF-2 per period first, then across the cycle.
   Cycle window: **2025-01-01..2026-12-31** (RI's election cycle runs
   odd-year January through even-year December — § 17-25-3 / 2026 Campaign
   Finance Manual; pin the exact statutory wording at the spike).
   `direct_contribution_total` is donor money only — loans, interest,
   Matching Public Funds, and State Check Off are excluded from it (fleet
   rule: raised = donor money). Matching Public Funds stays inside
   `total_receipts` as the CF-2 reports it; no bespoke public-funds column
   (a factory capability with one user, which the plan.md rules reject).
3. **Acquisition is per-organization.** Verified live: no date-only search.
   Pipeline: filings search → org roster with Board keys → per-org
   contribution + expenditure exports (date-bounded) + filing list + CF-2
   values. North-carolina cache shape (many small keyed artifacts, query-hash
   keys), NCSBE transport discipline (one request in flight, fixed ~2 s
   spacing, bounded retries on network/429/5xx only, descriptive UA),
   houston-style generic hidden-field capture for the WebForms
   postbacks/session cookie. Fail closed on anything that does not parse as
   the expected result page (a login page or error page must never install).
   Integrity gates: (a) verify export row counts against the UI result
   count/pagination so a silently capped export never installs; (b) snapshot
   the org's filing list before and after its exports and discard the
   bundle if the authoritative filing set changed mid-fetch (re-fetch next
   run — cheap consistency check, no transactional install machinery);
   (c) the Other Filings grid (`dgdCF8FilingList`) is traversed by WebForms
   pager postbacks, validating dates descend page-over-page, until an
   entire page predates the cycle start.
4. **Amendment semantics are unproven and release-gating.** ERTS marks
   filings amended and versions them, but whether the public
   transaction export returns original rows, amended rows, or current-ledger
   state is undocumented. The spike must test ≥5 visibly amended report
   families (retrieve every version, diff transaction IDs and totals against
   a date-bounded export, re-test after a new amendment lands). Until proven,
   report-version documents are the accounting authority and the export is an
   index. Selection follows the north-carolina report-selector pattern: group
   originals with amendments, pick the in-force filing, quarantine every
   ambiguous lineage instead of guessing. No `is_amendment` DB columns —
   selection happens in TypeScript before the writer, as everywhere else.
5. **Outside spending ships as curated supplements, not OCR.** CF-8s are
   scanned PDFs; the repo has no OCR capability (pdfjs-dist is text-layer
   only) and the verified cycle volume is 4 independent expenditures.
   Building an OCR pipeline for that volume is the definition of
   overengineering. Instead: the sync ingests a hand-curated supplement
   module (san jose `sanJosePaperFilingSupplements.ts` contract — each entry
   is a human-verified transcription of one CF-8 expenditure line, added
   only after reading the scan). Supplement entry shape: `filingGuid` (from
   the `/ReportsScanned/` URL), filed date, spender identity, per-candidate
   target entries sharing the `filingGuid` (san jose multi-candidate rule),
   stance, filer-stated per-target amount or none, and an
   amendment/supersedes note when a filing replaces an earlier GUID —
   with a uniqueness check so the same filing line cannot be entered twice.
   The acquisition script snapshots the paginated Other Filings index each
   run and diffs against the previous snapshot for **new, changed, and
   removed** GUIDs — any diff surfaces in sync diagnostics until a human
   resolves it. Set `outsideCoverageNote`: "Outside-spending filings in
   Rhode Island are scanned documents; totals include manually verified
   filings with a clear per-candidate amount — filings naming several
   candidates without a stated split are excluded — and the state requires
   spenders to disclose only donors above $1,000 per cycle, with statutory
   exceptions." (The exclusion clause keeps the note consistent with
   decision 7's quarantine — the verified $53,090 multi-target filing is
   excluded even though a human transcribed it. The "statutory exceptions"
   phrase covers § 17-25.3-1's written-restriction / separate-account
   carve-outs without enumerating them in UI text.) Revisit only if volume
   grows past what a human can transcribe per cycle.
6. **Filing-type gates.** Only INDEPENDENT EXPENDITURE filings can produce
   support/oppose rows. ELECTIONEERING COMMUNICATION (`Not Applicable`
   stance), COVERED TRANSFER, BALLOT QUESTION ADVOCACY, and REFERENDUM
   (observed live on the grid) never enter candidate outside totals. Covered transfers are also not double-counted
   against the spender's disclosed donor rows (the live UNITE HERE pair shows
   the same $100,000 as a covered transfer in, a disclosed donation, and the
   own-funds control — one receipt, not three).
7. **Multi-target allocation: filer-stated apportionment only.** RI law is
   stronger than Georgia's here: 410-RICR-10-00-13 § 13.5(B)(2)(d)
   *requires* a communication naming multiple candidates to be apportioned
   proportionally — so a multi-target filing with one amount and no stated
   allocation (live example: $53,090, Morales Support + Smiley Oppose) is a
   filing defect, not a normal case. Policy: use an explicit filer-provided
   per-target allocation when the filing states one; a single-target line
   allocates its full amount; a multi-target line with no stated allocation
   quarantines the **full amount** with diagnostic reason
   `missing_required_apportionment` (excluded dollars reported in sync
   diagnostics; the static coverage note tells users such filings are
   excluded — the loader's note is static text and cannot carry amounts).
   Never divide, never duplicate, never assign to one target. Add to the
   Board request: how filers correct or supplement a missing apportionment.
   Mechanics follow the georgia D6 quarantine precedent; the legal basis is
   RI's own regulation. No communication-level staging tables — supplement
   entries plus diagnostics carry this at current volume.
8. **Support/oppose fail-closed.** Stance comes from the form's explicit
   checkbox as read by the human transcriber; anything ambiguous (unclear
   checkbox, candidate not uniquely resolvable on the applicable ballot) is
   not entered. Target names resolve against the VoteApp roster through the
   normal token-based matching with the shared middle-name/nickname vetoes;
   a name matching zero or two candidates stays out.
9. **Eligible offices v1** (DB-ground the exact canonical names at PR 1
   against Rhode Island 2026 election rows): Governor, Lieutenant Governor
   (elected separately in RI, unlike Ohio), Secretary of State, Attorney
   General, General Treasurer (RI's official title — **VoteApp canonical
   office name is `State Treasurer`**, per `seedOffices.ts`; verified in the
   local DB 2026-08-12: the RI 2026 statewide row is `State Treasurer`, so
   the eligible list must use the canonical name or the race is silently
   omitted), state_upper + state_lower legislators. DB check 2026-08-12
   also found **no RI state_upper/state_lower 2026 election rows locally**
   — the General Assembly roster is a real prerequisite, not a formality
   (PR 1 preflight).
   Municipal offices excluded from v1: smaller committees may lawfully file
   on paper (electronic filing is mandatory only for general-office
   candidates and committees over the activity thresholds), so electronic
   coverage there is unproven — a completeness claim we cannot make yet.
10. **Itemization rules.** Sub-$200 itemized rows are retained as-is (legal
    when a donor's calendar-year aggregate crosses the threshold, or
    voluntary). `Aggregate - *` rows never become donors and never enter
    size buckets; they exist so recomputed receipts can reconcile to CF-2.
    Contribution types outside the pinned vocabulary land in a diagnostic
    counter, never in a guessed bucket.
11. **Outside-donor rows are organizations only (fleet rule).** The
    outside-breakdown schema allows only donor + industry categories, and
    the fleet's semantics (georgia aggregator, verbatim) are: "ORGANIZATION
    contributors are the only donors surfaced — an individual's money has
    no outside category to land in." So for transcribed CF-8 donor
    sections: an entity donor becomes a `donor` row and its name feeds the
    classifier → `industry` rows; an **individual** donor is never
    published as a personal-name donor row and their money stays out of the
    breakdowns in v1 (georgia parity — deriving industries from individual
    donors' disclosed employers is a possible follow-up, not a v1 rider).
    RI discloses spender donors only at $1,000+ cycle aggregate
    (§ 17-25.3-1), stated in the coverage note (decision 5). CF-8 donor
    rows are cycle-incremental across a spender's filings: ingest each
    filing's new rows once, dedupe repeats, use the cycle aggregate as a
    control value only — never as a transaction, and never added to the
    own-funds control (decision 6's UNITE HERE example).
12. **CF-5 exemption is a deferral, not a zero.** A CF-5 filer defers
    periodic reporting; it does not establish zero receipts or complete
    totals. Rule: a CF-5 alone → valid link, **no finance summary
    published yet**; totals publish only when an authoritative CF-2/annual
    report exists; empty breakdowns are valid only when report or export
    evidence confirms no itemized donor rows. Never publish zeros from the
    exemption itself.
13. **Receipt-type mapping is pinned** (verify the vocabulary against real
    export bytes at the spike; unknown types → diagnostic counter, never a
    guessed bucket):

    | ERTS contribution type | direct total | size bucket |
    |---|---|---|
    | Individual / PAC / Party (itemized) | yes | yes |
    | Aggregate - Individual / PAC / Party | yes | no |
    | In-Kind - Individual / PAC / Party (itemized) | yes | yes |
    | In Kind - Aggregate | yes | no |
    | Loan Proceeds (all), Interest Received, Refund/Rebate, State Check Off, Matching Public Funds, Other Receipt | no | no |
    | Party Building (any) on a candidate org | quarantine + diagnostic | no |

    Size buckets and contributor counting clone georgia's
    `contributionSizeBucket` exactly ($1–99, $100–249, $250–499, $500–999,
    $1,000–4,999, $5,000+) with `contributorCount` = unique normalized
    contributors, not rows (georgia `contributorKeys.size` pattern).
    **Known shared-loader defect (release-gating for buckets, fleet-wide):**
    `standardStateFinanceBallotLookupLoader.ts` ranks direct breakdowns
    `rn <= 5` per category type, so a candidate with all six buckets
    populated silently loses the smallest-amount bucket — this already
    bites every 6-bucket state (georgia, ohio, north carolina, maryland,
    …). The fix (exempt `contribution_size` from the cap, or raise it to 6
    for that category) is a shared-loader change and lands in its own PR
    per the plan.md working rules, before or alongside RI PR 2 — it is not
    an RI-module workaround.
14. **Manual candidate-finance links carry the standard fleet semantics —
    no RI carve-out.** The factory link upsert preserves both
    `link_status` and `link_source` when the existing row's `link_source`
    is `'manual'` (built into `standardStateFinanceSnapshotWriter.ts`
    since PR #667, always-on), and `erts_portal` supersession deactivates
    only `erts_portal` rows — so a sync can never relabel or deactivate an
    operator-curated link. Preferred correction path remains the
    resolver/alias layer; a manual link row is the escape hatch when a
    committee genuinely cannot be resolved. (An earlier revision declared
    manual links unsupported because the factory then overwrote
    `link_source` on conflict — true when written, fixed fleet-wide by
    #667; kept here so the stale claim is not reintroduced.)

## Required artifacts per sync

- Filings-search snapshot per tracked organization (org key, office, status,
  filing list with amended flags and report/document links)
- CF-2 summary values per authoritative filing (from the filing profile
  and/or generated report PDF URL as evidence)
- Per-org contribution export + expenditure export, date-bounded to the cycle
- Other Filings index snapshot (the CF-8 diff source for decision 5)
- Curated CF-8 supplement entries (in-repo module, not fetched)

## Module layout

`backend/src/pipeline/rhodeIslandFinance/`: ERTS client (WebForms session +
postbacks + exports), artifact cache (validate → SHA-256 → atomic install +
manifest), export row parsers (pinned vocabulary), report selector
(decision 4), committee resolver, auto-link, direct-contribution aggregator
(size buckets + reconciliation), CF-8 supplements + outside-spending
aggregator, outside-group donor aggregator, writer wrapper, ballot-lookup
loader wrapper, eligible offices, sync, batchSync, `index.ts`. Scheduler at
`backend/src/scheduler/rhodeIslandCandidateFinanceSyncScheduler.ts` (georgia
template; pick a daily-cron minute unused by any existing state scheduler).

## Flags and registration (wired in PRs 1–2, plan.md rules)

- `RHODE_ISLAND_CAMPAIGN_FINANCE_ENABLED` (read; `backend/.env.example` AND
  `render.yaml` in the schema PR — the Ohio lesson),
  `RHODE_ISLAND_CAMPAIGN_FINANCE_SYNC_ENABLED` (conjunctive, `force` bypass),
  `RHODE_ISLAND_ERTS_RAW_DATA_REFRESH_ENABLED` (portal fetches).
- `"RHODE_ISLAND_ERTS"` in the `ballotLookupFinanceShared.ts` source union +
  `FINANCE_SUMMARY_SOURCES` (compile-time guard) +
  `FINANCE_SOURCE_LABELS` ("Rhode Island Board of Elections") + one
  `STATE_FINANCE_LOOKUP_ADAPTERS` entry.
- npm scripts `rhode-island-candidates:finance:{probe,sync-due,raw:refresh,
  scheduler:*}`, `financeCliFlagGuard` on every CLI.

## PR sequence (one PR at a time; `npm run typecheck` + `npm test` green in `backend/`; cite matrix row)

Code-only PRs 1–2 are data-independent and proceed under the no-portal rule.
The acquisition spike gates all parser/aggregator work — it settles the
amendment question (decision 4) and captures real export fixtures. The
review's proposal to run the spike before the schema was considered and
rejected: the canonical schema is deliberately data-independent (drift is
rejected regardless of what the spike finds), and the one schema-sensitive
discovery it named — negative cash — is already adopted up front via signed
`cash_on_hand`. Identity/overlap/truncation findings land in parsers and
selectors, not the migration.

1. Migration + writer wrapper + writer test + eligible offices (+ test,
   canonical names DB-grounded against RI 2026 election rows — this is also
   the roster preflight: confirm RI statewide/GA elections and candidate
   rosters exist before anything depends on them) + feature flags
   end-to-end.
2. Loader wrapper + characterization pin + flag-gate test + source
   union/labels + `ballotLookup.ts` registry entry.
3. **Acquisition spike** (user-authorized portal access; no migration, no DB
   writes): prove the WebForms transport (session cookie, hidden-field
   postbacks, export retrieval), capture contribution/expenditure CSV
   schemas as fixtures, verify export row counts against UI counts (silent
   caps), confirm the CF-2 summary's period structure and pin the cycle
   window wording (decision 2), run the decision-4 amendment test on ≥5
   amended report families, traverse the paginated Other Filings grid to
   the cycle boundary (decision 3c) and snapshot it, reproduce the McKee Q2
   reconciliation from cached bytes. Also the point to send the Board the
   recurring-extract / amendment-semantics request
   (`campaign.finance@elections.ri.gov`) — user sends; the build does not
   block on a reply.
4. Artifact cache + export parsers (+ committed acquisition script; sync
   reads cache only). Committed fixtures are redacted/minimal — donor
   street addresses never enter the repo; raw exports stay gitignored in
   `scratch/`.
5. Committee resolver (+ test) + auto-link (+ test) + roster completeness
   report (eligible candidates, zero-match, one-match, multi-match, linked
   percentage).
6. Direct aggregator + report selector (decision 4 outcome baked in) +
   CF-2 reconciliation tests from spike fixtures.
7. Sync + batchSync + npm scripts + scheduler.
8. CF-8 supplements module + outside-spending aggregator (decisions 5–8) +
   outside-donor aggregator + classifier wiring + coverage notes; seed with
   the current cycle's transcribed filings; published groups enter the
   manual committee-label queue.
9. **Live run** through the committed acquisition script: fresh portal pull,
   full sync, money-reconciliation + match-rate report.

## Principal risks

1. **Amendment semantics** (decision 4) — the one open feasibility question;
   the spike answers it before any aggregation code exists.
2. **Portal fragility** — stateful WebForms app; mitigated by fail-closed
   validation, pacing, cache-only sync, atomic installs (a bad download can
   never destroy a good artifact).
3. **Supplement staleness** — a new CF-8 lands and nobody transcribes it;
   mitigated by the index-diff in every refresh run surfacing untranscribed
   filings in sync diagnostics, plus the standing coverage note.
4. **Employer mislabeling** — guarded structurally: nothing in the module
   ever constructs an `occupation` breakdown row.

## Status

- [x] PR 1 schema + writer + eligible offices + flags (migration 236;
  preflight 2026-08-12: five statewide 2026 races + rosters verified in the
  local DB — `State Treasurer` canonical confirmed — but no
  state_upper/state_lower RI 2026 election rows exist yet; the General
  Assembly roster remains a prerequisite for sync coverage, not for this PR)
- [x] PR 2 loader + wiring + source registration (shared-loader wrapper with
  contribution_size-only narrowing + both coverage notes; characterization
  pin incl. flag gate and migration-236 column check; `RHODE_ISLAND_ERTS`
  in the source union + `FINANCE_SUMMARY_SOURCES` + `FINANCE_SOURCE_LABELS`;
  RI adapter registered in `ballotLookup.ts`)
- [ ] PR 3 acquisition spike (needs user authorization)
- [ ] PR 4 artifact cache + parsers + acquisition script
- [ ] PR 5 resolver + auto-link
- [ ] PR 6 direct aggregator + report selector
- [ ] PR 7 sync + batchSync + scheduler
- [ ] PR 8 CF-8 supplements + outside aggregators
- [ ] PR 9 live run + reconciliation report
